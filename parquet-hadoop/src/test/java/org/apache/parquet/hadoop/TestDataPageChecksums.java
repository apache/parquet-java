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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.Page;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.codec.SnappyCompressor;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that page level checksums are correctly written and that checksum verification works as
 * expected
 */
public class TestDataPageChecksums {
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  private static final Statistics<?> EMPTY_STATS_INT32 =
      Statistics.getBuilderForReading(Types.required(INT32).named("a")).build();

  private CRC32 crc = new CRC32();

  // Sample data, two columns 'a' and 'b' (both int32),

  private static final int PAGE_SIZE = 1024 * 1024; // 1MB

  private static final MessageType schemaSimple =
      MessageTypeParser.parseMessageType("message m {" + "  required int32 a;" + "  required int32 b;" + "}");
  private static final ColumnDescriptor colADesc = schemaSimple.getColumns().get(0);
  private static final ColumnDescriptor colBDesc = schemaSimple.getColumns().get(1);
  private static final byte[] colAPage1Bytes = new byte[PAGE_SIZE];
  private static final byte[] colAPage2Bytes = new byte[PAGE_SIZE];
  private static final byte[] colBPage1Bytes = new byte[PAGE_SIZE];
  private static final byte[] colBPage2Bytes = new byte[PAGE_SIZE];
  private static final int numRecordsLargeFile = (2 * PAGE_SIZE) / Integer.BYTES;

  /**
   * Write out sample Parquet file using ColumnChunkPageWriteStore directly, return path to file
   */
  private Path writeSimpleParquetFile(
      Configuration conf, CompressionCodecName compression, ParquetProperties.WriterVersion version)
      throws IOException {
    File file = tempFolder.newFile();
    file.delete();
    Path path = new Path(file.toURI());

    for (int i = 0; i < PAGE_SIZE; i++) {
      colAPage1Bytes[i] = (byte) i;
      colAPage2Bytes[i] = (byte) -i;
      colBPage1Bytes[i] = (byte) (i + 100);
      colBPage2Bytes[i] = (byte) (i - 100);
    }

    ParquetFileWriter writer = new ParquetFileWriter(
        conf,
        schemaSimple,
        path,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
        Integer.MAX_VALUE,
        allocator);

    writer.start();
    writer.startBlock(numRecordsLargeFile);

    CodecFactory codecFactory = new CodecFactory(conf, PAGE_SIZE);
    BytesInputCompressor compressor = codecFactory.getCompressor(compression);

    ColumnChunkPageWriteStore writeStore = ColumnChunkPageWriteStore.build(
            compressor, schemaSimple, new HeapByteBufferAllocator())
        .withColumnIndexTruncateLength(Integer.MAX_VALUE)
        .withPageWriteChecksumEnabled(ParquetOutputFormat.getPageWriteChecksumEnabled(conf))
        .build();

    if (version == ParquetProperties.WriterVersion.PARQUET_1_0) {
      PageWriter pageWriter = writeStore.getPageWriter(colADesc);
      pageWriter.writePage(
          BytesInput.from(colAPage1Bytes),
          numRecordsLargeFile / 2,
          numRecordsLargeFile / 2,
          EMPTY_STATS_INT32,
          Encoding.RLE,
          Encoding.RLE,
          Encoding.PLAIN);
      pageWriter.writePage(
          BytesInput.from(colAPage2Bytes),
          numRecordsLargeFile / 2,
          numRecordsLargeFile / 2,
          EMPTY_STATS_INT32,
          Encoding.RLE,
          Encoding.RLE,
          Encoding.PLAIN);

      pageWriter = writeStore.getPageWriter(colBDesc);
      pageWriter.writePage(
          BytesInput.from(colBPage1Bytes),
          numRecordsLargeFile / 2,
          numRecordsLargeFile / 2,
          EMPTY_STATS_INT32,
          Encoding.RLE,
          Encoding.RLE,
          Encoding.PLAIN);
      pageWriter.writePage(
          BytesInput.from(colBPage2Bytes),
          numRecordsLargeFile / 2,
          numRecordsLargeFile / 2,
          EMPTY_STATS_INT32,
          Encoding.RLE,
          Encoding.RLE,
          Encoding.PLAIN);
    } else if (version == ParquetProperties.WriterVersion.PARQUET_2_0) {
      PageWriter pageWriter = writeStore.getPageWriter(colADesc);
      pageWriter.writePageV2(
          numRecordsLargeFile / 2,
          0,
          numRecordsLargeFile / 2,
          BytesInput.from(colAPage1Bytes, 0, PAGE_SIZE / 4),
          BytesInput.from(colAPage1Bytes, PAGE_SIZE / 4, PAGE_SIZE / 4),
          Encoding.PLAIN,
          BytesInput.from(colAPage1Bytes, PAGE_SIZE / 2, PAGE_SIZE / 2),
          EMPTY_STATS_INT32);
      pageWriter.writePageV2(
          numRecordsLargeFile / 2,
          0,
          numRecordsLargeFile / 2,
          BytesInput.from(colAPage2Bytes, 0, PAGE_SIZE / 4),
          BytesInput.from(colAPage2Bytes, PAGE_SIZE / 4, PAGE_SIZE / 4),
          Encoding.PLAIN,
          BytesInput.from(colAPage2Bytes, PAGE_SIZE / 2, PAGE_SIZE / 2),
          EMPTY_STATS_INT32);

      pageWriter = writeStore.getPageWriter(colBDesc);
      pageWriter.writePageV2(
          numRecordsLargeFile / 2,
          0,
          numRecordsLargeFile / 2,
          BytesInput.from(colBPage1Bytes, 0, PAGE_SIZE / 4),
          BytesInput.from(colBPage1Bytes, PAGE_SIZE / 4, PAGE_SIZE / 4),
          Encoding.PLAIN,
          BytesInput.from(colBPage1Bytes, PAGE_SIZE / 2, PAGE_SIZE / 2),
          EMPTY_STATS_INT32);
      pageWriter.writePageV2(
          numRecordsLargeFile / 2,
          0,
          numRecordsLargeFile / 2,
          BytesInput.from(colBPage2Bytes, 0, PAGE_SIZE / 4),
          BytesInput.from(colBPage2Bytes, PAGE_SIZE / 4, PAGE_SIZE / 4),
          Encoding.PLAIN,
          BytesInput.from(colBPage2Bytes, PAGE_SIZE / 2, PAGE_SIZE / 2),
          EMPTY_STATS_INT32);
    } else {
      throw new IllegalArgumentException("Unknown writer version: " + version);
    }

    writeStore.flushToFileWriter(writer);

    writer.endBlock();
    writer.end(new HashMap<>());

    codecFactory.release();

    return path;
  }

  // Sample data, nested schema with nulls

  private static final MessageType schemaNestedWithNulls =
      MessageTypeParser.parseMessageType("message m {" + "  optional group c {"
          + "    required int64 id;"
          + "    required group d {"
          + "      repeated int32 val;"
          + "    }"
          + "  }"
          + "}");
  private static final ColumnDescriptor colCIdDesc =
      schemaNestedWithNulls.getColumns().get(0);
  private static final ColumnDescriptor colDValDesc =
      schemaNestedWithNulls.getColumns().get(1);

  private static final double nullRatio = 0.3;
  private static final int numRecordsNestedWithNullsFile = 2000;

  private Path writeNestedWithNullsSampleParquetFile(
      Configuration conf,
      boolean dictionaryEncoding,
      CompressionCodecName compression,
      ParquetProperties.WriterVersion version)
      throws IOException {
    File file = tempFolder.newFile();
    file.delete();
    Path path = new Path(file.toURI());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withAllocator(allocator)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withCompressionCodec(compression)
        .withDictionaryEncoding(dictionaryEncoding)
        .withType(schemaNestedWithNulls)
        .withPageWriteChecksumEnabled(ParquetOutputFormat.getPageWriteChecksumEnabled(conf))
        .withWriterVersion(version)
        .build()) {
      GroupFactory groupFactory = new SimpleGroupFactory(schemaNestedWithNulls);
      Random rand = new Random(42);

      for (int i = 0; i < numRecordsNestedWithNullsFile; i++) {
        Group group = groupFactory.newGroup();
        if (rand.nextDouble() > nullRatio) {
          // With equal probability, write out either 1 or 3 values in group e. To ensure our values
          // are dictionary encoded when required, perform modulo.
          if (rand.nextDouble() > 0.5) {
            group.addGroup("c").append("id", (long) i).addGroup("d").append("val", rand.nextInt() % 10);
          } else {
            group.addGroup("c")
                .append("id", (long) i)
                .addGroup("d")
                .append("val", rand.nextInt() % 10)
                .append("val", rand.nextInt() % 10)
                .append("val", rand.nextInt() % 10);
          }
        }
        writer.write(group);
      }
    }

    return path;
  }

  /**
   * Enable writing out page level crc checksum, disable verification in read path but check that
   * the crc checksums are correct. Tests whether we successfully write out correct crc checksums
   * without potentially failing on the read path verification .
   */
  private void testWriteOnVerifyOff(ParquetProperties.WriterVersion version) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED, version);

    try (ParquetFileReader reader = getParquetFileReader(path, conf, Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      DataPage colAPage1 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage1, colAPage1Bytes);
      assertCorrectContent(getPageBytes(colAPage1), colAPage1Bytes);

      DataPage colAPage2 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage2, colAPage2Bytes);
      assertCorrectContent(getPageBytes(colAPage2), colAPage2Bytes);

      DataPage colBPage1 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage1, colBPage1Bytes);
      assertCorrectContent(getPageBytes(colBPage1), colBPage1Bytes);

      DataPage colBPage2 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage2, colBPage2Bytes);
      assertCorrectContent(getPageBytes(colBPage2), colBPage2Bytes);
    }
  }

  @Test
  public void testWriteOnVerifyOffV1() throws IOException {
    testWriteOnVerifyOff(ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  @Test
  public void testWriteOnVerifyOffV2() throws IOException {
    testWriteOnVerifyOff(ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  /**
   * Test that we do not write out checksums if the feature is turned off
   */
  private void testWriteOffVerifyOff(ParquetProperties.WriterVersion version) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED, version);

    try (ParquetFileReader reader = getParquetFileReader(path, conf, Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      assertCrcNotSet(readNextPage(colADesc, pageReadStore));
      assertCrcNotSet(readNextPage(colADesc, pageReadStore));
      assertCrcNotSet(readNextPage(colBDesc, pageReadStore));
      assertCrcNotSet(readNextPage(colBDesc, pageReadStore));
    }
  }

  @Test
  public void testWriteOffVerifyOffV1() throws IOException {
    testWriteOffVerifyOff(ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  @Test
  public void testWriteOffVerifyOffV2() throws IOException {
    testWriteOffVerifyOff(ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  /**
   * Do not write out page level crc checksums, but enable verification on the read path. Tests
   * that the read still succeeds and does not throw an exception.
   */
  private void testWriteOffVerifyOn(ParquetProperties.WriterVersion version) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED, version);

    try (ParquetFileReader reader = getParquetFileReader(path, conf, Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      assertCorrectContent(getPageBytes(readNextPage(colADesc, pageReadStore)), colAPage1Bytes);
      assertCorrectContent(getPageBytes(readNextPage(colADesc, pageReadStore)), colAPage2Bytes);
      assertCorrectContent(getPageBytes(readNextPage(colBDesc, pageReadStore)), colBPage1Bytes);
      assertCorrectContent(getPageBytes(readNextPage(colBDesc, pageReadStore)), colBPage2Bytes);
    }
  }

  @Test
  public void testWriteOffVerifyOnV1() throws IOException {
    testWriteOffVerifyOn(ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  @Test
  public void testWriteOffVerifyOnV2() throws IOException {
    testWriteOffVerifyOn(ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  /**
   * Write out checksums and verify them on the read path. Tests that crc is set and that we can
   * read back what we wrote if checksums are enabled on both the write and read path.
   */
  private void testWriteOnVerifyOn(ParquetProperties.WriterVersion version) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED, version);

    try (ParquetFileReader reader = getParquetFileReader(path, conf, Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      DataPage colAPage1 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage1, colAPage1Bytes);
      assertCorrectContent(getPageBytes(colAPage1), colAPage1Bytes);

      DataPage colAPage2 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage2, colAPage2Bytes);
      assertCorrectContent(getPageBytes(colAPage2), colAPage2Bytes);

      DataPage colBPage1 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage1, colBPage1Bytes);
      assertCorrectContent(getPageBytes(colBPage1), colBPage1Bytes);

      DataPage colBPage2 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage2, colBPage2Bytes);
      assertCorrectContent(getPageBytes(colBPage2), colBPage2Bytes);
    }
  }

  @Test
  public void testWriteOnVerifyOnV1() throws IOException {
    testWriteOnVerifyOn(ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  @Test
  public void testWriteOnVerifyOnV2() throws IOException {
    testWriteOnVerifyOn(ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  /**
   * Test whether corruption in the page content is detected by checksum verification
   */
  private void testCorruptedPage(ParquetProperties.WriterVersion version) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED, version);

    InputFile inputFile = HadoopInputFile.fromPath(path, conf);
    try (SeekableInputStream inputStream = inputFile.newStream()) {
      int fileLen = (int) inputFile.getLength();
      byte[] fileBytes = new byte[fileLen];
      inputStream.readFully(fileBytes);
      inputStream.close();

      // There are 4 pages in total (2 per column), we corrupt the first page of the first column
      // and the second page of the second column. We do this by altering a byte roughly in the
      // middle of each page to be corrupted
      fileBytes[fileLen / 8]++;
      fileBytes[fileLen / 8 + ((fileLen / 4) * 3)]++;

      OutputFile outputFile = HadoopOutputFile.fromPath(path, conf);
      try (PositionOutputStream outputStream = outputFile.createOrOverwrite(1024 * 1024)) {
        outputStream.write(fileBytes);
        outputStream.close();

        // First we disable checksum verification, the corruption will go undetected as it is in the
        // data section of the page
        conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);
        try (ParquetFileReader reader = getParquetFileReader(path, conf, Arrays.asList(colADesc, colBDesc))) {
          PageReadStore pageReadStore = reader.readNextRowGroup();

          DataPage colAPage1 = readNextPage(colADesc, pageReadStore);
          assertFalse(
              "Data in page was not corrupted", Arrays.equals(getPageBytes(colAPage1), colAPage1Bytes));
          readNextPage(colADesc, pageReadStore);
          readNextPage(colBDesc, pageReadStore);
          DataPage colBPage2 = readNextPage(colBDesc, pageReadStore);
          assertFalse(
              "Data in page was not corrupted", Arrays.equals(getPageBytes(colBPage2), colBPage2Bytes));
        }

        // Now we enable checksum verification, the corruption should be detected
        conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);
        try (ParquetFileReader reader = getParquetFileReader(path, conf, Arrays.asList(colADesc, colBDesc))) {
          // We expect an exception on the first encountered corrupt page (in readAllPages)
          assertVerificationFailed(reader);
        }
      }
    }
  }

  @Test
  public void testCorruptedPageV1() throws IOException {
    testCorruptedPage(ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  @Test
  public void testCorruptedPageV2() throws IOException {
    testCorruptedPage(ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  /**
   * Tests that the checksum is calculated using the compressed version of the data and that
   * checksum verification succeeds
   */
  private void testCompression(ParquetProperties.WriterVersion version) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.SNAPPY, version);

    try (ParquetFileReader reader = getParquetFileReader(path, conf, Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      DataPage colAPage1 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage1, snappy(colAPage1Bytes, getDataOffset(colAPage1)));
      assertCorrectContent(getPageBytes(colAPage1), colAPage1Bytes);

      DataPage colAPage2 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage2, snappy(colAPage2Bytes, getDataOffset(colAPage2)));
      assertCorrectContent(getPageBytes(colAPage2), colAPage2Bytes);

      DataPage colBPage1 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage1, snappy(colBPage1Bytes, getDataOffset(colBPage1)));
      assertCorrectContent(getPageBytes(colBPage1), colBPage1Bytes);

      DataPage colBPage2 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage2, snappy(colBPage2Bytes, getDataOffset(colBPage2)));
      assertCorrectContent(getPageBytes(colBPage2), colBPage2Bytes);
    }
  }

  @Test
  public void testCompressionV1() throws IOException {
    testCompression(ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  @Test
  public void testCompressionV2() throws IOException {
    testCompression(ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  /**
   * Tests that we adhere to the checksum calculation specification, namely that the crc is
   * calculated using the compressed concatenation of the repetition levels, definition levels and
   * the actual data. This is done by generating sample data with a nested schema containing nulls
   * (generating non-trivial repetition and definition levels).
   */
  private void testNestedWithNulls(ParquetProperties.WriterVersion version) throws IOException {
    Configuration conf = new Configuration();

    // Write out sample file via the non-checksum code path, extract the raw bytes to calculate the
    // reference crc with
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);
    Path refPath = writeNestedWithNullsSampleParquetFile(conf, false, CompressionCodecName.SNAPPY, version);

    try (ParquetFileReader refReader =
        getParquetFileReader(refPath, conf, Arrays.asList(colCIdDesc, colDValDesc))) {
      PageReadStore refPageReadStore = refReader.readNextRowGroup();
      byte[] colCIdPageBytes = getPageBytes(readNextPage(colCIdDesc, refPageReadStore));
      byte[] colDValPageBytes = getPageBytes(readNextPage(colDValDesc, refPageReadStore));

      // Write out sample file with checksums
      conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
      conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);
      Path path = writeNestedWithNullsSampleParquetFile(conf, false, CompressionCodecName.SNAPPY, version);

      try (ParquetFileReader reader = getParquetFileReader(path, conf, Arrays.asList(colCIdDesc, colDValDesc))) {
        PageReadStore pageReadStore = reader.readNextRowGroup();

        DataPage colCIdPage = readNextPage(colCIdDesc, pageReadStore);
        assertCrcSetAndCorrect(colCIdPage, snappy(colCIdPageBytes, getDataOffset(colCIdPage)));
        assertCorrectContent(getPageBytes(colCIdPage), colCIdPageBytes);

        DataPage colDValPage = readNextPage(colDValDesc, pageReadStore);
        assertCrcSetAndCorrect(colDValPage, snappy(colDValPageBytes, getDataOffset(colDValPage)));
        assertCorrectContent(getPageBytes(colDValPage), colDValPageBytes);
      }
    }
  }

  @Test
  public void testNestedWithNullsV1() throws IOException {
    testNestedWithNulls(ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  @Test
  public void testNestedWithNullsV2() throws IOException {
    testNestedWithNulls(ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  private void testDictionaryEncoding(ParquetProperties.WriterVersion version) throws IOException {
    Configuration conf = new Configuration();

    // Write out dictionary encoded sample file via the non-checksum code path, extract the raw
    // bytes to calculate the  reference crc with
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);
    Path refPath = writeNestedWithNullsSampleParquetFile(conf, true, CompressionCodecName.SNAPPY, version);

    try (ParquetFileReader refReader =
        getParquetFileReader(refPath, conf, Collections.singletonList(colDValDesc))) {
      PageReadStore refPageReadStore = refReader.readNextRowGroup();
      // Read (decompressed) dictionary page
      byte[] dictPageBytes =
          readDictPage(colDValDesc, refPageReadStore).getBytes().toByteArray();
      byte[] colDValPageBytes = getPageBytes(readNextPage(colDValDesc, refPageReadStore));

      // Write out sample file with checksums
      conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
      conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);
      Path path = writeNestedWithNullsSampleParquetFile(conf, true, CompressionCodecName.SNAPPY, version);

      try (ParquetFileReader reader = getParquetFileReader(path, conf, Collections.singletonList(colDValDesc))) {
        PageReadStore pageReadStore = reader.readNextRowGroup();

        DictionaryPage dictPage = readDictPage(colDValDesc, pageReadStore);
        assertCrcSetAndCorrect(dictPage, snappy(dictPageBytes));
        assertCorrectContent(dictPage.getBytes().toByteArray(), dictPageBytes);

        DataPage colDValPage = readNextPage(colDValDesc, pageReadStore);
        assertCrcSetAndCorrect(colDValPage, snappy(colDValPageBytes, getDataOffset(colDValPage)));
        assertCorrectContent(getPageBytes(colDValPage), colDValPageBytes);
      }
    }
  }

  @Test
  public void testDictionaryEncodingV1() throws IOException {
    testDictionaryEncoding(ParquetProperties.WriterVersion.PARQUET_1_0);
  }

  @Test
  public void testDictionaryEncodingV2() throws IOException {
    testDictionaryEncoding(ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  /**
   * Compress using snappy
   */
  private byte[] snappy(byte[] bytes, int offset) throws IOException {
    int length = bytes.length - offset;
    SnappyCompressor compressor = new SnappyCompressor();
    compressor.reset();
    compressor.setInput(bytes, offset, length);
    compressor.finish();
    byte[] buffer = new byte[length * 2];
    int compressedSize = compressor.compress(buffer, 0, buffer.length);
    return BytesInput.concat(
            BytesInput.from(Arrays.copyOfRange(bytes, 0, offset)),
            BytesInput.from(Arrays.copyOfRange(buffer, 0, compressedSize)))
        .toByteArray();
  }

  private byte[] snappy(byte[] bytes) throws IOException {
    return snappy(bytes, 0);
  }

  private int getDataOffset(Page page) {
    if (page instanceof DataPageV2) {
      return (int) (((DataPageV2) page).getRepetitionLevels().size()
          + ((DataPageV2) page).getDefinitionLevels().size());
    } else {
      return 0;
    }
  }

  /**
   * Construct ParquetFileReader for input file and columns
   */
  private ParquetFileReader getParquetFileReader(Path path, Configuration conf, List<ColumnDescriptor> columns)
      throws IOException {
    HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
    SeekableInputStream inputStream = inputFile.newStream();
    ParquetReadOptions readOptions = HadoopReadOptions.builder(conf).build();
    ParquetMetadata footer = ParquetFileReader.readFooter(inputFile, readOptions, inputStream);
    ParquetFileReader reader = ParquetFileReader.open(inputFile, footer, readOptions, inputStream);
    reader.setRequestedSchema(columns);
    return reader;
  }

  /**
   * Read the dictionary page for the column
   */
  private DictionaryPage readDictPage(ColumnDescriptor colDesc, PageReadStore pageReadStore) {
    return pageReadStore.getPageReader(colDesc).readDictionaryPage();
  }

  /**
   * Read the next page for a column
   */
  private DataPage readNextPage(ColumnDescriptor colDesc, PageReadStore pageReadStore) {
    return pageReadStore.getPageReader(colDesc).readPage();
  }

  /**
   * Compare the extracted (decompressed) bytes to the reference bytes
   */
  private void assertCorrectContent(byte[] pageBytes, byte[] referenceBytes) {
    assertArrayEquals("Read page content was different from expected page content", referenceBytes, pageBytes);
  }

  private byte[] getPageBytes(DataPage page) throws IOException {
    if (page instanceof DataPageV1) {
      DataPageV1 pageV1 = (DataPageV1) page;
      return pageV1.getBytes().toByteArray();
    } else if (page instanceof DataPageV2) {
      DataPageV2 pageV2 = (DataPageV2) page;
      return BytesInput.concat(pageV2.getRepetitionLevels(), pageV2.getDefinitionLevels(), pageV2.getData())
          .toByteArray();
    } else {
      throw new IllegalArgumentException(
          "Unknown page type " + page.getClass().getName());
    }
  }

  /**
   * Verify that the crc is set in a page, calculate the reference crc using the reference bytes and
   * check that the crc's are identical.
   */
  private void assertCrcSetAndCorrect(Page page, byte[] referenceBytes) {
    assertTrue("Checksum was not set in page", page.getCrc().isPresent());
    int crcFromPage = page.getCrc().getAsInt();
    crc.reset();
    crc.update(referenceBytes);
    assertEquals(
        "Checksum found in page did not match calculated reference checksum",
        crc.getValue(),
        (long) crcFromPage & 0xffffffffL);
  }

  /**
   * Verify that the crc is not set
   */
  private void assertCrcNotSet(Page page) {
    assertFalse("Checksum was set in page", page.getCrc().isPresent());
  }

  /**
   * Read the next page for a column, fail if this did not throw an checksum verification exception,
   * if the read succeeds (no exception was thrown ), verify that the checksum was not set.
   */
  private void assertVerificationFailed(ParquetFileReader reader) {
    try {
      reader.readNextRowGroup();
      fail("Expected checksum verification exception to be thrown");
    } catch (Exception e) {
      assertTrue("Thrown exception is of incorrect type", e instanceof ParquetDecodingException);
      assertTrue(
          "Did not catch checksum verification ParquetDecodingException",
          e.getMessage().contains("CRC checksum verification failed"));
    }
  }
}
