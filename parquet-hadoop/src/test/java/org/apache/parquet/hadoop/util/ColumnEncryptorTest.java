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
package org.apache.parquet.hadoop.util;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ColumnEncryptorTest {

  private Configuration conf = new Configuration();
  private Map<String, String> extraMeta = ImmutableMap.of("key1", "value1", "key2", "value2");
  private ColumnEncryptor columnEncryptor = null;
  private final int numRecord = 100000;
  private String inputFile = null;
  private String outputFile = null;
  private TestFileHelper.TestDocs testDocs = null;

  private void testSetup(String compression) throws IOException {
    columnEncryptor = new ColumnEncryptor(conf);
    testDocs = new TestFileHelper.TestDocs(numRecord);
    inputFile = TestFileHelper.createParquetFile(conf, extraMeta, numRecord, "input", compression,
      ParquetProperties.WriterVersion.PARQUET_1_0, ParquetProperties.DEFAULT_PAGE_SIZE, testDocs);
    outputFile = TestFileHelper.createTempFile("test");
  }

  @Test
  public void testFlatColumn() throws IOException {
    testSetup("GZIP");
    String[] encryptColumns = {"DocId"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false));

    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testNestedColumn() throws IOException {
    testSetup("GZIP");
    String[] encryptColumns = {"Links.Forward"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false));
    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testNoEncryption() throws IOException {
    testSetup("GZIP");
    String[] encryptColumns = {};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false));
    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testEncryptAllColumns() throws IOException {
    testSetup("GZIP");
    String[] encryptColumns = {"DocId", "Name", "Gender", "Links.Forward", "Links.Backward"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false));
    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testEncryptSomeColumns() throws IOException {
    testSetup("GZIP");
    String[] encryptColumns = {"DocId", "Name", "Links.Forward"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false));

    ParquetMetadata metaData = getParquetMetadata(EncDecProperties.getFileDecryptionProperties());
    assertTrue(metaData.getBlocks().size() > 0);
    List<ColumnChunkMetaData> columns = metaData.getBlocks().get(0).getColumns();
    Set<String> set = new HashSet<>(Arrays.asList(encryptColumns));
    for (ColumnChunkMetaData column : columns) {
      if (set.contains(column.getPath().toDotString())) {
        assertTrue(column.isEncrypted());
      } else {
        assertFalse(column.isEncrypted());
      }
    }
  }

  @Test
  public void testFooterEncryption() throws IOException {
    testSetup("GZIP");
    String[] encryptColumns = {"DocId"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, true));

    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testAesGcm() throws IOException {
    testSetup("GZIP");
    String[] encryptColumns = {"DocId"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_V1, true));

    verifyResultDecryptionWithValidKey();
  }

  @Test
  public void testColumnIndex() throws IOException {
    testSetup("GZIP");
    String[] encryptColumns = {"Name"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_V1, false));

    verifyResultDecryptionWithValidKey();
    verifyOffsetIndexes();
  }

  @Test
  public void testDifferentCompression() throws IOException {
    String[] compressions = {"GZIP", "ZSTD", "SNAPPY", "UNCOMPRESSED"};
    for (String compression : compressions)  {
      testSetup(compression);
    }
    String[] encryptColumns = {"Links.Forward"};
    columnEncryptor.encryptColumns(inputFile, outputFile, Arrays.asList(encryptColumns),
      EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false));
    verifyResultDecryptionWithValidKey();

  }

  private void verifyResultDecryptionWithValidKey() throws IOException  {
    ParquetReader<Group> reader = createReader(outputFile);
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertTrue(group.getLong("DocId", 0) == testDocs.docId[i]);
      assertArrayEquals(group.getBinary("Name", 0).getBytes(), testDocs.name[i].getBytes());
      assertArrayEquals(group.getBinary("Gender", 0).getBytes(), testDocs.gender[i].getBytes());
      Group subGroup = group.getGroup("Links", 0);
      assertArrayEquals(subGroup.getBinary("Forward", 0).getBytes(), testDocs.linkForward[i].getBytes());
      assertArrayEquals(subGroup.getBinary("Backward", 0).getBytes(), testDocs.linkBackward[i].getBytes());
    }
    reader.close();
  }

  private void verifyOffsetIndexes() throws IOException {
    ParquetReadOptions readOptions = HadoopReadOptions.builder(conf)
      .withDecryption(EncDecProperties.getFileDecryptionProperties())
      .build();

    try (TransParquetFileReader inReader = createFileReader(inputFile);
         TransParquetFileReader outReader = createFileReader(outputFile)) {
      ParquetMetadata inMetaData = getMetadata(readOptions, inputFile, inReader);
      ParquetMetadata outMetaData = getMetadata(readOptions, outputFile, outReader);
      compareOffsetIndexes(inReader, outReader, inMetaData, outMetaData);
    }
  }

  private ParquetMetadata getMetadata(ParquetReadOptions readOptions, String file, TransParquetFileReader reader) throws IOException {
    return ParquetFileReader.readFooter(HadoopInputFile.fromPath(new Path(file), conf),
                                        readOptions,
                                        reader.getStream());
  }

  private void compareOffsetIndexes(TransParquetFileReader inReader, TransParquetFileReader outReader,
                                    ParquetMetadata inMetaData, ParquetMetadata outMetaData) throws IOException {

    PageReadStore inStore = inReader.readNextRowGroup();
    PageReadStore outStore = outReader.readNextRowGroup();
    int blockIndex = 0;
    while (inStore != null && outStore != null) {
      List<ColumnChunkMetaData> inColumns = inMetaData.getBlocks().get(blockIndex).getColumns();
      List<ColumnChunkMetaData> outColumns = outMetaData.getBlocks().get(blockIndex).getColumns();
      assertEquals(inColumns.size(), outColumns.size());
      validateColumns(inReader, outReader, inColumns, outColumns);
      inStore = inReader.readNextRowGroup();
      outStore = outReader.readNextRowGroup();
      blockIndex++;
      if (inStore != null || outStore != null) {
        throw new IOException("Number of row groups are not equal");
      }
    }
  }

  private void validateColumns(TransParquetFileReader inReader, TransParquetFileReader outReader,
                               List<ColumnChunkMetaData> inColumns, List<ColumnChunkMetaData> outColumns) throws IOException {
    for (int i = 0; i < inColumns.size(); i ++) {
      ColumnChunkMetaData inChunk = inColumns.get(i);
      ColumnChunkMetaData outChunk = outColumns.get(i);
      OffsetIndex inOffsetIndex = inReader.readOffsetIndex(inChunk);
      OffsetIndex outOffsetIndex = outReader.readOffsetIndex(outChunk);
      assertEquals(inOffsetIndex.getPageCount(), outOffsetIndex.getPageCount());
      if (outChunk.isEncrypted()) {
        continue;
      }
      validatePages(inReader, outReader, inOffsetIndex, outOffsetIndex);
    }
  }

  private void validatePages(TransParquetFileReader inReader, TransParquetFileReader outReader,
                         OffsetIndex inOffsetIndex, OffsetIndex outOffsetIndex) throws IOException {
      for (int pageId = 0; pageId < inOffsetIndex.getPageCount(); pageId ++) {
        long inPageOffset = inOffsetIndex.getOffset(pageId);
        inReader.setStreamPosition(inPageOffset);
        long outPageOffset = outOffsetIndex.getOffset(pageId);
        outReader.setStreamPosition(outPageOffset);
        PageHeader inPageHeader = inReader.readPageHeader();
        PageHeader outPageHeader = outReader.readPageHeader();
        assertEquals(inPageHeader, outPageHeader);
        DataPageHeader inHeaderV1 = inPageHeader.data_page_header;
        DataPageHeader outHeaderV1 = outPageHeader.data_page_header;
        assertEquals(inHeaderV1, outHeaderV1);
        BytesInput inPageLoad = readBlockAllocate(inReader, inPageHeader.compressed_page_size);
        BytesInput outPageLoad = readBlockAllocate(outReader, outPageHeader.compressed_page_size);
        assertEquals(inPageLoad.toByteBuffer(), outPageLoad.toByteBuffer());
      }
  }

  private BytesInput readBlockAllocate(TransParquetFileReader reader, int length) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  private TransParquetFileReader createFileReader(String path) throws IOException {
    return new TransParquetFileReader(HadoopInputFile.fromPath(new Path(path), conf),
      HadoopReadOptions.builder(conf)
        .withDecryption(EncDecProperties.getFileDecryptionProperties())
        .build());
  }

  private ParquetReader<Group> createReader(String path) throws IOException {
    return ParquetReader.builder(new GroupReadSupport(),
      new Path(path)).withConf(conf).withDecryption(EncDecProperties.getFileDecryptionProperties()).build();
  }

  private ParquetMetadata getParquetMetadata(FileDecryptionProperties decryptionProperties) throws IOException {
    ParquetMetadata metaData;
    ParquetReadOptions readOptions = ParquetReadOptions.builder()
      .withDecryption(decryptionProperties)
      .build();
    InputFile file = HadoopInputFile.fromPath(new Path(outputFile), conf);
    try (SeekableInputStream in = file.newStream()) {
      metaData  = ParquetFileReader.readFooter(file, readOptions, in);
    }
    return metaData;
  }
}
