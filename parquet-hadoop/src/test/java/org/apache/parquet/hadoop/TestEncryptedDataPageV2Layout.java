/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetrieverMock;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.InternalColumnEncryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that the repetition and definition levels of encrypted v2 data pages are encrypted together with the
 * values, as a single page module (as written by Arrow C++ and arrow-rs), and that the legacy layout written by older
 * parquet-java versions, with plaintext levels, is still readable.
 */
public class TestEncryptedDataPageV2Layout {

  private static final byte[] FOOTER_KEY = "0123456789012345".getBytes();
  private static final byte[] COLUMN_KEY = "1234567890123450".getBytes();
  private static final String FOOTER_KEY_ID = "kf";
  private static final String COLUMN_KEY_ID = "kc";
  private static final String COLUMN = "a";

  private static final MessageType SCHEMA =
      Types.buildMessage().repeated(INT32).named(COLUMN).named("msg");

  /** Rows: [1, 2], [], [3] */
  private static final int[][] ROWS = {{1, 2}, {}, {3}};

  private static final int[] REPETITION_LEVELS = {0, 1, 0, 0};
  private static final int[] DEFINITION_LEVELS = {1, 1, 0, 1};
  private static final int[] VALUES = {1, 2, 3};

  @TempDir
  private java.nio.file.Path tempDir;

  private enum Layout {
    STANDARD,
    LEGACY
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testWriterEncryptsLevelsWithValues(boolean offHeapDecryption) throws IOException {
    Path path = new Path(tempDir.resolve("standard-writer.parquet").toUri());
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withDictionaryEncoding(false)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withPageWriteChecksumEnabled(true)
        .withEncryption(encryptionProperties())
        .build()) {
      for (int[] row : ROWS) {
        Group group = factory.newGroup();
        for (int value : row) {
          group.append(COLUMN, value);
        }
        writer.write(group);
      }
    }

    assertThat(readPageLayout(path)).isEqualTo(Layout.STANDARD);
    assertRows(path, offHeapDecryption);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFileWriterEncryptsLevelsWithValues(boolean offHeapDecryption) throws IOException {
    Path path = new Path(tempDir.resolve("standard-file-writer.parquet").toUri());
    writePage(path, Layout.STANDARD);

    assertThat(readPageLayout(path)).isEqualTo(Layout.STANDARD);
    assertRows(path, offHeapDecryption);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLegacyLayoutWithPlaintextLevelsIsReadable(boolean offHeapDecryption) throws IOException {
    Path path = new Path(tempDir.resolve("legacy.parquet").toUri());
    writePage(path, Layout.LEGACY);

    assertThat(readPageLayout(path)).isEqualTo(Layout.LEGACY);
    assertRows(path, offHeapDecryption);
  }

  @Test
  public void testWriterVersionsWithPlaintextEncryptedV2Levels() {
    assertThat(ParquetFileReader.isWrittenWithPlaintextEncryptedV2Levels(
            "parquet-mr version 1.12.0 (build db75a6815f2ba1d1ee89d1a90aeb296f1f3a8f20)"))
        .isTrue();
    assertThat(ParquetFileReader.isWrittenWithPlaintextEncryptedV2Levels(
            "parquet-mr version 1.16.0 (build 0123456789)"))
        .isTrue();
    assertThat(ParquetFileReader.isWrittenWithPlaintextEncryptedV2Levels(
            "parquet-mr version 1.19.0-SNAPSHOT (build 0123456789)"))
        .isFalse();
    assertThat(ParquetFileReader.isWrittenWithPlaintextEncryptedV2Levels(
            "parquet-mr version 1.19.0 (build 0123456789)"))
        .isFalse();
    assertThat(ParquetFileReader.isWrittenWithPlaintextEncryptedV2Levels("parquet-cpp-arrow version 17.0.0"))
        .isFalse();
    assertThat(ParquetFileReader.isWrittenWithPlaintextEncryptedV2Levels(null))
        .isFalse();
  }

  /**
   * Writes a single v2 data page with {@link ParquetFileWriter}: either with the levels encrypted together with the
   * values, or in the legacy layout (plaintext levels followed by encrypted values) written by older parquet-java
   * versions.
   */
  private void writePage(Path path, Layout layout) throws IOException {
    Configuration conf = new Configuration();
    ParquetProperties props = ParquetProperties.builder()
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .build();
    ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, conf),
        SCHEMA,
        Mode.CREATE,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
        encryptionProperties(),
        props);
    ColumnDescriptor descriptor = SCHEMA.getColumns().get(0);

    BytesInput repetitionLevels = encodeLevels(REPETITION_LEVELS);
    BytesInput definitionLevels = encodeLevels(DEFINITION_LEVELS);
    ByteBuffer values = ByteBuffer.allocate(VALUES.length * 4).order(ByteOrder.LITTLE_ENDIAN);
    Statistics<?> statistics = Statistics.createStats(descriptor.getPrimitiveType());
    for (int value : VALUES) {
      values.putInt(value);
      statistics.updateStats(value);
    }
    statistics.incrementNumNulls(1);
    BytesInput plainValues = BytesInput.from(values.array());

    writer.start();
    writer.startBlock(ROWS.length);
    InternalFileEncryptor fileEncryptor = writer.getEncryptor();
    InternalColumnEncryptionSetup columnSetup = fileEncryptor.getColumnSetup(ColumnPath.get(COLUMN), true, 0);
    BlockCipher.Encryptor dataEncryptor = columnSetup.getDataEncryptor();
    byte[] pageAAD = AesCipher.createModuleAAD(fileEncryptor.getFileAAD(), ModuleType.DataPage, 0, 0, 0);
    byte[] pageHeaderAAD =
        AesCipher.createModuleAAD(fileEncryptor.getFileAAD(), ModuleType.DataPageHeader, 0, 0, 0);

    writer.startColumn(descriptor, REPETITION_LEVELS.length, CompressionCodecName.UNCOMPRESSED);
    if (layout == Layout.LEGACY) {
      // Encrypt the values only, and write the levels in plaintext
      BytesInput encryptedValues = BytesInput.from(dataEncryptor.encrypt(plainValues.toByteArray(), pageAAD));
      writer.writeDataPageV2(
          ROWS.length,
          1,
          REPETITION_LEVELS.length,
          repetitionLevels,
          definitionLevels,
          Encoding.PLAIN,
          encryptedValues,
          false,
          (int) plainValues.size(),
          statistics,
          columnSetup.getMetaDataEncryptor(),
          pageHeaderAAD,
          null);
    } else {
      writer.writeDataPageV2(
          ROWS.length,
          1,
          REPETITION_LEVELS.length,
          repetitionLevels,
          definitionLevels,
          Encoding.PLAIN,
          plainValues,
          false,
          (int) plainValues.size(),
          statistics,
          columnSetup.getMetaDataEncryptor(),
          pageHeaderAAD,
          null,
          dataEncryptor,
          pageAAD);
    }
    writer.endColumn();
    writer.endBlock();
    writer.end(Collections.emptyMap());
  }

  private static BytesInput encodeLevels(int[] levels) throws IOException {
    try (RunLengthBitPackingHybridEncoder encoder =
        new RunLengthBitPackingHybridEncoder(1, 64, 1024, new HeapByteBufferAllocator())) {
      for (int level : levels) {
        encoder.writeInt(level);
      }
      return BytesInput.copy(encoder.toBytes());
    }
  }

  /**
   * Reads the raw (encrypted) body of the first data page, and checks where its page module starts.
   */
  private Layout readPageLayout(Path path) throws IOException {
    Configuration conf = new Configuration();
    ParquetReadOptions options = ParquetReadOptions.builder()
        .withDecryption(decryptionProperties())
        .build();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf), options)) {
      ColumnChunkMetaData chunk =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);
      InternalFileDecryptor fileDecryptor =
          reader.getFooter().getFileMetaData().getFileDecryptor();
      byte[] pageHeaderAAD =
          AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleType.DataPageHeader, 0, 0, 0);
      reader.f.seek(chunk.getStartingPos());
      PageHeader pageHeader = Util.readPageHeader(
          reader.f, fileDecryptor.getColumnSetup(chunk.getPath()).getMetaDataDecryptor(), pageHeaderAAD);
      assertThat(pageHeader.getType()).isEqualTo(PageType.DATA_PAGE_V2);

      int levelsLength = pageHeader.getData_page_header_v2().getRepetition_levels_byte_length()
          + pageHeader.getData_page_header_v2().getDefinition_levels_byte_length();
      assertThat(levelsLength).isPositive();
      byte[] pageBody = new byte[pageHeader.getCompressed_page_size()];
      reader.f.readFully(pageBody);

      if (BytesUtils.readIntLittleEndian(pageBody, 0) + 4 == pageBody.length) {
        return Layout.STANDARD;
      }
      assertThat(levelsLength + BytesUtils.readIntLittleEndian(pageBody, levelsLength) + 4)
          .isEqualTo(pageBody.length);
      return Layout.LEGACY;
    }
  }

  private void assertRows(Path path, boolean offHeapDecryption) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetInputFormat.OFF_HEAP_DECRYPT_BUFFER_ENABLED, offHeapDecryption);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);
    List<List<Integer>> actual = new ArrayList<>();
    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(
            offHeapDecryption ? new DirectByteBufferAllocator() : new HeapByteBufferAllocator());
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
            .withConf(conf)
            .withAllocator(allocator)
            .withDecryption(decryptionProperties())
            .build()) {
      for (Group group = reader.read(); group != null; group = reader.read()) {
        List<Integer> row = new ArrayList<>();
        for (int i = 0; i < group.getFieldRepetitionCount(COLUMN); i++) {
          row.add(group.getInteger(COLUMN, i));
        }
        actual.add(row);
      }
    }
    List<List<Integer>> expected = new ArrayList<>();
    for (int[] row : ROWS) {
      List<Integer> expectedRow = new ArrayList<>();
      for (int value : row) {
        expectedRow.add(value);
      }
      expected.add(expectedRow);
    }
    assertThat(actual).isEqualTo(expected);
  }

  private static FileEncryptionProperties encryptionProperties() {
    ColumnEncryptionProperties columnProperties = ColumnEncryptionProperties.builder(COLUMN)
        .withKey(COLUMN_KEY)
        .withKeyID(COLUMN_KEY_ID)
        .build();
    return FileEncryptionProperties.builder(FOOTER_KEY)
        .withFooterKeyID(FOOTER_KEY_ID)
        .withEncryptedColumns(Collections.singletonMap(columnProperties.getPath(), columnProperties))
        .build();
  }

  private static FileDecryptionProperties decryptionProperties() {
    return FileDecryptionProperties.builder()
        .withKeyRetriever(new DecryptionKeyRetrieverMock()
            .putKey(FOOTER_KEY_ID, FOOTER_KEY)
            .putKey(COLUMN_KEY_ID, COLUMN_KEY))
        .build();
  }
}
