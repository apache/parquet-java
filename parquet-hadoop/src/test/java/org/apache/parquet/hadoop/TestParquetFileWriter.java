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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.Version;
import org.apache.parquet.VersionParser;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.apache.parquet.format.BloomFilterStrategy;
import org.junit.Assume;
import org.junit.Rule;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.StatisticsOpts;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOptBuilder;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOpts;
import org.junit.Test;
import org.apache.parquet.Log;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.Assert.*;
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.*;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;

import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class TestParquetFileWriter {

  private static final Log LOG = Log.getLog(TestParquetFileWriter.class);

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("" +
      "message m {" +
      "  required group a {" +
      "    required binary b;" +
      "  }" +
      "  required group c {" +
      "    required int64 d;" +
      "  }" +
      "}");
  private static final String[] PATH1 = {"a", "b"};
  private static final ColumnDescriptor C1 = SCHEMA.getColumnDescription(PATH1);
  private static final String[] PATH2 = {"c", "d"};
  private static final ColumnDescriptor C2 = SCHEMA.getColumnDescription(PATH2);

  private static final byte[] BYTES1 = { 0, 1, 2, 3 };
  private static final byte[] BYTES2 = { 1, 2, 3, 4 };
  private static final byte[] BYTES3 = { 2, 3, 4, 5 };
  private static final byte[] BYTES4 = { 3, 4, 5, 6 };
  private static final CompressionCodecName CODEC = CompressionCodecName.UNCOMPRESSED;

  private static final BinaryStatistics STATS1 = new BinaryStatistics(null);
  private static final BinaryStatistics STATS2 = new BinaryStatistics(null);

  private String writeSchema;

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testWriteMode() throws Exception {
    File testFile = temp.newFile();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { required group a {required binary b;} required group "
        + "c { required int64 d; }}");
    Configuration conf = new Configuration();

    ParquetFileWriter writer = null;
    boolean exceptionThrown = false;
    Path path = new Path(testFile.toURI());
    try {
      writer = new ParquetFileWriter(conf, schema, path,
          ParquetFileWriter.Mode.CREATE);
    } catch(IOException ioe1) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    exceptionThrown = false;
    try {
      writer = new ParquetFileWriter(conf, schema, path,
          OVERWRITE);
    } catch(IOException ioe2) {
      exceptionThrown = true;
    }
    assertTrue(!exceptionThrown);
    testFile.delete();
  }

  @Test
  public void testWriteRead() throws Exception {
    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

   ParquetFileWriter w = new ParquetFileWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    long c1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<String, String>());

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
    assertEquals("footer: "+ readFooter, 2, readFooter.getBlocks().size());
    assertEquals(c1Ends - c1Starts, readFooter.getBlocks().get(0).getColumns().get(0).getTotalSize());
    assertEquals(c2Ends - c2Starts, readFooter.getBlocks().get(0).getColumns().get(1).getTotalSize());
    assertEquals(c2Ends - c1Starts, readFooter.getBlocks().get(0).getTotalByteSize());
    HashSet<Encoding> expectedEncoding=new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    expectedEncoding.add(BIT_PACKED);
    assertEquals(expectedEncoding,readFooter.getBlocks().get(0).getColumns().get(0).getEncodings());

    { // read first block of col #1
      ParquetFileReader r = new ParquetFileReader(configuration, readFooter.getFileMetaData(), path,
          Arrays.asList(readFooter.getBlocks().get(0)), Arrays.asList(SCHEMA.getColumnDescription(PATH1)));
      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
      assertNull(r.readNextRowGroup());
    }

    { // read all blocks of col #1 and #2

      ParquetFileReader r = new ParquetFileReader(configuration, readFooter.getFileMetaData(), path,
          readFooter.getBlocks(), Arrays.asList(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)));

      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH2, 2, BytesInput.from(BYTES2));
      validateContains(SCHEMA, pages, PATH2, 3, BytesInput.from(BYTES2));
      validateContains(SCHEMA, pages, PATH2, 1, BytesInput.from(BYTES2));

      pages = r.readNextRowGroup();
      assertEquals(4, pages.getRowCount());

      validateContains(SCHEMA, pages, PATH1, 7, BytesInput.from(BYTES3));
      validateContains(SCHEMA, pages, PATH2, 8, BytesInput.from(BYTES4));

      assertNull(r.readNextRowGroup());
    }
    PrintFooter.main(new String[] {path.toString()});
  }

  @Test
  public void testAlignmentWithPadding() throws Exception {
    File testFile = temp.newFile();

    Path path = new Path(testFile.toURI());
    Configuration conf = new Configuration();

    // uses the test constructor
    ParquetFileWriter w = new ParquetFileWriter(conf, SCHEMA, path, 120, 60);

    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    long c1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();

    long firstRowGroupEnds = w.getPos(); // should be 109

    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();

    long secondRowGroupEnds = w.getPos();

    w.end(new HashMap<String, String>());

    FileSystem fs = path.getFileSystem(conf);
    long fileLen = fs.getFileStatus(path).getLen();

    FSDataInputStream data = fs.open(path);
    data.seek(fileLen - 8); // 4-byte offset + "PAR1"
    long footerLen = BytesUtils.readIntLittleEndian(data);
    long startFooter = fileLen - footerLen - 8;

    assertEquals("Footer should start after second row group without padding",
        secondRowGroupEnds, startFooter);

    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path);
    assertEquals("footer: "+ readFooter, 2, readFooter.getBlocks().size());
    assertEquals(c1Ends - c1Starts, readFooter.getBlocks().get(0).getColumns().get(0).getTotalSize());
    assertEquals(c2Ends - c2Starts, readFooter.getBlocks().get(0).getColumns().get(1).getTotalSize());
    assertEquals(c2Ends - c1Starts, readFooter.getBlocks().get(0).getTotalByteSize());
    HashSet<Encoding> expectedEncoding=new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    expectedEncoding.add(BIT_PACKED);
    assertEquals(expectedEncoding,readFooter.getBlocks().get(0).getColumns().get(0).getEncodings());

    // verify block starting positions with padding
    assertEquals("First row group should start after magic",
        4, readFooter.getBlocks().get(0).getStartingPos());
    assertTrue("First row group should end before the block size (120)",
        firstRowGroupEnds < 120);
    assertEquals("Second row group should start at the block size",
        120, readFooter.getBlocks().get(1).getStartingPos());

    { // read first block of col #1
      ParquetFileReader r = new ParquetFileReader(conf, readFooter.getFileMetaData(), path,
          Arrays.asList(readFooter.getBlocks().get(0)), Arrays.asList(SCHEMA.getColumnDescription(PATH1)));
      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
      assertNull(r.readNextRowGroup());
    }

    { // read all blocks of col #1 and #2

      ParquetFileReader r = new ParquetFileReader(conf, readFooter.getFileMetaData(), path,
          readFooter.getBlocks(), Arrays.asList(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)));

      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH2, 2, BytesInput.from(BYTES2));
      validateContains(SCHEMA, pages, PATH2, 3, BytesInput.from(BYTES2));
      validateContains(SCHEMA, pages, PATH2, 1, BytesInput.from(BYTES2));

      pages = r.readNextRowGroup();
      assertEquals(4, pages.getRowCount());

      validateContains(SCHEMA, pages, PATH1, 7, BytesInput.from(BYTES3));
      validateContains(SCHEMA, pages, PATH2, 8, BytesInput.from(BYTES4));

      assertNull(r.readNextRowGroup());
    }
    PrintFooter.main(new String[] {path.toString()});
  }

  @Test
  public void testAlignmentWithNoPaddingNeeded() throws Exception {
    File testFile = temp.newFile();

    Path path = new Path(testFile.toURI());
    Configuration conf = new Configuration();

    // uses the test constructor
    ParquetFileWriter w = new ParquetFileWriter(conf, SCHEMA, path, 100, 50);

    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    long c1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();

    long firstRowGroupEnds = w.getPos(); // should be 109

    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), STATS1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), STATS2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();

    long secondRowGroupEnds = w.getPos();

    w.end(new HashMap<String, String>());

    FileSystem fs = path.getFileSystem(conf);
    long fileLen = fs.getFileStatus(path).getLen();

    FSDataInputStream data = fs.open(path);
    data.seek(fileLen - 8); // 4-byte offset + "PAR1"
    long footerLen = BytesUtils.readIntLittleEndian(data);
    long startFooter = fileLen - footerLen - 8;

    assertEquals("Footer should start after second row group without padding",
        secondRowGroupEnds, startFooter);

    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path);
    assertEquals("footer: "+ readFooter, 2, readFooter.getBlocks().size());
    assertEquals(c1Ends - c1Starts, readFooter.getBlocks().get(0).getColumns().get(0).getTotalSize());
    assertEquals(c2Ends - c2Starts, readFooter.getBlocks().get(0).getColumns().get(1).getTotalSize());
    assertEquals(c2Ends - c1Starts, readFooter.getBlocks().get(0).getTotalByteSize());
    HashSet<Encoding> expectedEncoding=new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    expectedEncoding.add(BIT_PACKED);
    assertEquals(expectedEncoding,readFooter.getBlocks().get(0).getColumns().get(0).getEncodings());

    // verify block starting positions with padding
    assertEquals("First row group should start after magic",
        4, readFooter.getBlocks().get(0).getStartingPos());
    assertTrue("First row group should end before the block size (120)",
        firstRowGroupEnds > 100);
    assertEquals("Second row group should start after no padding",
        109, readFooter.getBlocks().get(1).getStartingPos());

    { // read first block of col #1
      ParquetFileReader r = new ParquetFileReader(conf, readFooter.getFileMetaData(), path,
          Arrays.asList(readFooter.getBlocks().get(0)), Arrays.asList(SCHEMA.getColumnDescription(PATH1)));
      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
      assertNull(r.readNextRowGroup());
    }

    { // read all blocks of col #1 and #2

      ParquetFileReader r = new ParquetFileReader(conf, readFooter.getFileMetaData(), path,
          readFooter.getBlocks(), Arrays.asList(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)));

      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
      validateContains(SCHEMA, pages, PATH2, 2, BytesInput.from(BYTES2));
      validateContains(SCHEMA, pages, PATH2, 3, BytesInput.from(BYTES2));
      validateContains(SCHEMA, pages, PATH2, 1, BytesInput.from(BYTES2));

      pages = r.readNextRowGroup();
      assertEquals(4, pages.getRowCount());

      validateContains(SCHEMA, pages, PATH1, 7, BytesInput.from(BYTES3));
      validateContains(SCHEMA, pages, PATH2, 8, BytesInput.from(BYTES4));

      assertNull(r.readNextRowGroup());
    }
    PrintFooter.main(new String[] {path.toString()});
  }

  @Test
  public void testConvertToThriftStatistics() throws Exception {
    long[] longArray = new long[] {39L, 99L, 12L, 1000L, 65L, 542L, 2533461316L, -253346131996L, Long.MAX_VALUE, Long.MIN_VALUE};
    LongStatistics parquetMRstats = new LongStatistics(null);
    final String createdBy =
        "parquet-mr version 1.8.0 (build d4d5a07ec9bd262ca1e93c309f1d7d4a74ebda4c)";

    for (long l: longArray) {
      parquetMRstats.updateStats(l);
    }
    Statistics thriftStats =
        org.apache.parquet.format.converter.ParquetMetadataConverter.toParquetStatistics(parquetMRstats);
    LongStatistics convertedBackStats =
        (LongStatistics) org.apache.parquet.format.converter.ParquetMetadataConverter
            .fromParquetStatistics(createdBy, thriftStats, PrimitiveTypeName.INT64);

    assertEquals(parquetMRstats.getMax(), convertedBackStats.getMax());
    assertEquals(parquetMRstats.getMin(), convertedBackStats.getMin());
    assertEquals(parquetMRstats.getNumNulls(), convertedBackStats.getNumNulls());
  }

  @Test
  public void testBloomFilterConverter() {
    int v1 = 39;
    int v2 = 99;
    int v3 = 1000;
    final String createdBy =
        "parquet-mr version 1.8.0 (build d4d5a07ec9bd262ca1e93c309f1d7d4a74ebda4c)";
    MessageType messageType =
        MessageTypeParser.parseMessageType("Message messageType{optional int32 a;}");
    BloomFilterOpts bloomFilterOpts =
        new BloomFilterOptBuilder().enableCols("a").expectedEntries("100")
            .falsePositiveProbabilities("0.05").build(messageType);
    StatisticsOpts statisticsOpts = new StatisticsOpts(bloomFilterOpts);
    IntStatistics parquetMRstats = new IntStatistics(
        statisticsOpts.getStatistics(messageType.getColumnDescription(new String[] { "a" })));
    parquetMRstats.updateStats(v1);

    Statistics thriftStats = org.apache.parquet.format.converter.ParquetMetadataConverter
        .toParquetStatistics(parquetMRstats);
    IntStatistics convertedBackStats =
        (IntStatistics) org.apache.parquet.format.converter.ParquetMetadataConverter
            .fromParquetStatisticsWithBF(createdBy, thriftStats, PrimitiveTypeName.INT32);
    assertTrue(parquetMRstats.test(v1));
    assertTrue(convertedBackStats.test(v1));
    assertFalse(parquetMRstats.test(v2));
    assertFalse(convertedBackStats.test(v2));
    assertFalse(parquetMRstats.test(v3));
    assertFalse(convertedBackStats.test(v3));

    parquetMRstats.updateStats(v2);
    thriftStats = org.apache.parquet.format.converter.ParquetMetadataConverter
        .toParquetStatistics(parquetMRstats);
    convertedBackStats =
        (IntStatistics) org.apache.parquet.format.converter.ParquetMetadataConverter
            .fromParquetStatisticsWithBF(createdBy, thriftStats, PrimitiveTypeName.INT32);
    assertTrue(parquetMRstats.test(v1));
    assertTrue(convertedBackStats.test(v1));
    assertTrue(parquetMRstats.test(v2));
    assertTrue(convertedBackStats.test(v2));
    assertFalse(parquetMRstats.test(v3));
    assertFalse(convertedBackStats.test(v3));

    parquetMRstats.updateStats(v3);
    thriftStats = org.apache.parquet.format.converter.ParquetMetadataConverter
        .toParquetStatistics(parquetMRstats);
    convertedBackStats =
        (IntStatistics) org.apache.parquet.format.converter.ParquetMetadataConverter
            .fromParquetStatisticsWithBF(createdBy, thriftStats, PrimitiveTypeName.INT32);
    assertTrue(parquetMRstats.test(v1));
    assertTrue(convertedBackStats.test(v1));
    assertTrue(parquetMRstats.test(v2));
    assertTrue(convertedBackStats.test(v2));
    assertTrue(parquetMRstats.test(v3));
    assertTrue(convertedBackStats.test(v3));
  }

  @Test
  public void testWriteReadStatistics() throws Exception {
    // this test assumes statistics will be read
    Assume.assumeTrue(!shouldIgnoreStatistics(Version.FULL_VERSION, BINARY));

    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    MessageType schema = MessageTypeParser.parseMessageType("message m { required group a {required binary b;} required group c { required int64 d; }}");
    String[] path1 = {"a", "b"};
    ColumnDescriptor c1 = schema.getColumnDescription(path1);
    String[] path2 = {"c", "d"};
    ColumnDescriptor c2 = schema.getColumnDescription(path2);

    byte[] bytes1 = { 0, 1, 2, 3};
    byte[] bytes2 = { 1, 2, 3, 4};
    byte[] bytes3 = { 2, 3, 4, 5};
    byte[] bytes4 = { 3, 4, 5, 6};
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

    BinaryStatistics statsB1C1P1 = new BinaryStatistics(null);
    BinaryStatistics statsB1C1P2 = new BinaryStatistics(null);
    LongStatistics statsB1C2P1 = new LongStatistics(null);
    LongStatistics statsB1C2P2 = new LongStatistics(null);
    BinaryStatistics statsB2C1P1 = new BinaryStatistics(null);
    LongStatistics statsB2C2P1 = new LongStatistics(null);
    statsB1C1P1.setMinMax(Binary.fromString("s"), Binary.fromString("z"));
    statsB1C1P2.setMinMax(Binary.fromString("a"), Binary.fromString("b"));
    statsB1C2P1.setMinMax(2l, 10l);
    statsB1C2P2.setMinMax(-6l, 4l);
    statsB2C1P1.setMinMax(Binary.fromString("d"), Binary.fromString("e"));
    statsB2C2P1.setMinMax(11l, 122l);

    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(3);
    w.startColumn(c1, 5, codec);
    w.writeDataPage(2, 4, BytesInput.from(bytes1), statsB1C1P1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(bytes1), statsB1C1P2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(c2, 6, codec);
    w.writeDataPage(3, 4, BytesInput.from(bytes2), statsB1C2P1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(bytes2), statsB1C2P2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();

    w.startBlock(4);
    w.startColumn(c1, 7, codec);
    w.writeDataPage(7, 4, BytesInput.from(bytes3), statsB2C1P1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(c2, 8, codec);
    w.writeDataPage(8, 4, BytesInput.from(bytes4), statsB2C2P1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<String, String>());

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
    for (BlockMetaData block : readFooter.getBlocks()) {
      for (ColumnChunkMetaData col : block.getColumns()) {
        col.getPath();
      }
    }
    // correct statistics
    BinaryStatistics bs1 = new BinaryStatistics(null);
    bs1.setMinMax(Binary.fromString("a"), Binary.fromString("z"));
    LongStatistics ls1 = new LongStatistics(null);
    ls1.setMinMax(-6l, 10l);

    BinaryStatistics bs2 = new BinaryStatistics(null);
    bs2.setMinMax(Binary.fromString("d"), Binary.fromString("e"));
    LongStatistics ls2 = new LongStatistics(null);
    ls2.setMinMax(11l, 122l);

    { // assert stats are correct for the first block
      BinaryStatistics bsout = (BinaryStatistics)readFooter.getBlocks().get(0).getColumns().get(0).getStatistics();
      String str = new String(bsout.getMaxBytes());
      String str2 = new String(bsout.getMinBytes());

      assertTrue(
          ((BinaryStatistics) readFooter.getBlocks().get(0).getColumns().get(0).getStatistics())
              .equals(bs1));
      assertTrue(
          ((LongStatistics) readFooter.getBlocks().get(0).getColumns().get(1).getStatistics())
              .equals(ls1));
    }
    { // assert stats are correct for the second block
      assertTrue(((BinaryStatistics)readFooter.getBlocks().get(1).getColumns().get(0).getStatistics()).equals(bs2));
      assertTrue(((LongStatistics)readFooter.getBlocks().get(1).getColumns().get(1).getStatistics()).equals(ls2));
    }
  }

  @Test
  public void testMetaDataFile() throws Exception {

    File testDir = temp.newFolder();

    Path testDirPath = new Path(testDir.toURI());
    Configuration configuration = new Configuration();

    final FileSystem fs = testDirPath.getFileSystem(configuration);
    enforceEmptyDir(configuration, testDirPath);

    MessageType schema = MessageTypeParser.parseMessageType("message m { required group a {required binary b;} required group c { required int64 d; }}");
    createFile(configuration, new Path(testDirPath, "part0"), schema);
    createFile(configuration, new Path(testDirPath, "part1"), schema);
    createFile(configuration, new Path(testDirPath, "part2"), schema);

    FileStatus outputStatus = fs.getFileStatus(testDirPath);
    List<Footer> footers = ParquetFileReader.readFooters(configuration, outputStatus, false);
    validateFooters(footers);
    ParquetFileWriter.writeMetadataFile(configuration, testDirPath, footers, JobSummaryLevel.ALL);

    footers = ParquetFileReader.readFooters(configuration, outputStatus, false);
    validateFooters(footers);
    footers = ParquetFileReader.readFooters(configuration, fs.getFileStatus(new Path(testDirPath, "part0")), false);
    assertEquals(1, footers.size());

    final FileStatus metadataFile = fs.getFileStatus(new Path(testDirPath, ParquetFileWriter.PARQUET_METADATA_FILE));
    final FileStatus metadataFileLight = fs.getFileStatus(new Path(testDirPath, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE));
    final List<Footer> metadata = ParquetFileReader.readSummaryFile(configuration, metadataFile);

    validateFooters(metadata);

    footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, Arrays.asList(fs.listStatus(testDirPath, HiddenFileFilter.INSTANCE)), false);
    validateFooters(footers);

    fs.delete(metadataFile.getPath(), false);
    fs.delete(metadataFileLight.getPath(), false);

    footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, Arrays.asList(fs.listStatus(testDirPath)), false);
    validateFooters(footers);

  }

  @Test
  public void testWriteReadStatisticsAllNulls() throws Exception {
    // this test assumes statistics will be read
    Assume.assumeTrue(!shouldIgnoreStatistics(Version.FULL_VERSION, BINARY));

    File testFile = temp.newFile();
    testFile.delete();

    writeSchema = "message example {\n" +
            "required binary content;\n" +
            "}";

    Path path = new Path(testFile.toURI());

    MessageType schema = MessageTypeParser.parseMessageType(writeSchema);
    Configuration configuration = new Configuration();
    GroupWriteSupport.setSchema(schema, configuration);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, new GroupWriteSupport());

    Group r1 = new SimpleGroup(schema);
    writer.write(r1);
    writer.close();

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);

    // assert the statistics object is not empty
    assertTrue((readFooter.getBlocks().get(0).getColumns().get(0).getStatistics().isEmpty()) == false);
    // assert the number of nulls are correct for the first block
    assertEquals(1, (readFooter.getBlocks().get(0).getColumns().get(0).getStatistics().getNumNulls()));
  }

  private void validateFooters(final List<Footer> metadata) {
    LOG.debug(metadata);
    assertEquals(String.valueOf(metadata), 3, metadata.size());
    for (Footer footer : metadata) {
      final File file = new File(footer.getFile().toUri());
      assertTrue(file.getName(), file.getName().startsWith("part"));
      assertTrue(file.getPath(), file.exists());
      final ParquetMetadata parquetMetadata = footer.getParquetMetadata();
      assertEquals(2, parquetMetadata.getBlocks().size());
      final Map<String, String> keyValueMetaData = parquetMetadata.getFileMetaData().getKeyValueMetaData();
      assertEquals("bar", keyValueMetaData.get("foo"));
      assertEquals(footer.getFile().getName(), keyValueMetaData.get(footer.getFile().getName()));
    }
  }


  private void createFile(Configuration configuration, Path path, MessageType schema) throws IOException {
    String[] path1 = {"a", "b"};
    ColumnDescriptor c1 = schema.getColumnDescription(path1);
    String[] path2 = {"c", "d"};
    ColumnDescriptor c2 = schema.getColumnDescription(path2);

    byte[] bytes1 = { 0, 1, 2, 3};
    byte[] bytes2 = { 1, 2, 3, 4};
    byte[] bytes3 = { 2, 3, 4, 5};
    byte[] bytes4 = { 3, 4, 5, 6};
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

    BinaryStatistics stats1 = new BinaryStatistics(null);
    BinaryStatistics stats2 = new BinaryStatistics(null);

    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(3);
    w.startColumn(c1, 5, codec);
    w.writeDataPage(2, 4, BytesInput.from(bytes1), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(bytes1), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(c2, 6, codec);
    w.writeDataPage(2, 4, BytesInput.from(bytes2), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(bytes2), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(bytes2), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(c1, 7, codec);
    w.writeDataPage(7, 4, BytesInput.from(bytes3), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(c2, 8, codec);
    w.writeDataPage(8, 4, BytesInput.from(bytes4), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    final HashMap<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put("foo", "bar");
    extraMetaData.put(path.getName(), path.getName());
    w.end(extraMetaData);
  }

  private void validateContains(MessageType schema, PageReadStore pages, String[] path, int values, BytesInput bytes) throws IOException {
    PageReader pageReader = pages.getPageReader(schema.getColumnDescription(path));
    DataPage page = pageReader.readPage();
    assertEquals(values, page.getValueCount());
    assertArrayEquals(bytes.toByteArray(), ((DataPageV1)page).getBytes().toByteArray());
  }

  @Test
  public void testMergeMetadata() {
    FileMetaData md1 = new FileMetaData(
        new MessageType("root1",
            new PrimitiveType(REPEATED, BINARY, "a"),
            new PrimitiveType(OPTIONAL, BINARY, "b")),
        new HashMap<String, String>(), "test");
    FileMetaData md2 = new FileMetaData(
        new MessageType("root2",
            new PrimitiveType(REQUIRED, BINARY, "c")),
        new HashMap<String, String>(), "test2");
    GlobalMetaData merged = ParquetFileWriter.mergeInto(md2, ParquetFileWriter.mergeInto(md1, null));
    assertEquals(
        merged.getSchema(),
        new MessageType("root1",
            new PrimitiveType(REPEATED, BINARY, "a"),
            new PrimitiveType(OPTIONAL, BINARY, "b"),
            new PrimitiveType(REQUIRED, BINARY, "c"))
        );

  }

  @Test
  public void testMergeFooters() {
    List<BlockMetaData> oneBlocks = new ArrayList<BlockMetaData>();
    oneBlocks.add(new BlockMetaData());
    oneBlocks.add(new BlockMetaData());
    List<BlockMetaData> twoBlocks = new ArrayList<BlockMetaData>();
    twoBlocks.add(new BlockMetaData());
    List<BlockMetaData> expected = new ArrayList<BlockMetaData>();
    expected.addAll(oneBlocks);
    expected.addAll(twoBlocks);

    Footer one = new Footer(new Path("file:/tmp/output/one.parquet"),
        new ParquetMetadata(new FileMetaData(
            new MessageType("root1",
                new PrimitiveType(REPEATED, BINARY, "a"),
                new PrimitiveType(OPTIONAL, BINARY, "b")),
            new HashMap<String, String>(), "test"),
        oneBlocks));

    Footer two = new Footer(new Path("/tmp/output/two.parquet"),
        new ParquetMetadata(new FileMetaData(
            new MessageType("root2",
                new PrimitiveType(REQUIRED, BINARY, "c")),
            new HashMap<String, String>(), "test2"),
            twoBlocks));

    List<Footer> footers = new ArrayList<Footer>();
    footers.add(one);
    footers.add(two);

    ParquetMetadata merged = ParquetFileWriter.mergeFooters(
        new Path("/tmp"), footers);

    assertEquals(
        new MessageType("root1",
            new PrimitiveType(REPEATED, BINARY, "a"),
            new PrimitiveType(OPTIONAL, BINARY, "b"),
            new PrimitiveType(REQUIRED, BINARY, "c")),
        merged.getFileMetaData().getSchema());

    assertEquals("Should have all blocks", expected, merged.getBlocks());
  }

  /**
   * {@link ParquetFileWriter#mergeFooters(Path, List)} expects a fully-qualified
   * path for the root and crashes if a relative one is provided.
   */
  @Test
  public void testWriteMetadataFileWithRelativeOutputPath() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path relativeRoot = new Path("target/_test_relative");
    Path qualifiedRoot = fs.makeQualified(relativeRoot);

    ParquetMetadata mock = Mockito.mock(ParquetMetadata.class);
    FileMetaData fileMetaData = new FileMetaData(
            new MessageType("root1",
                new PrimitiveType(REPEATED, BINARY, "a")),
            new HashMap<String, String>(), "test");
    Mockito.when(mock.getFileMetaData()).thenReturn(fileMetaData);

    List<Footer> footers = new ArrayList<Footer>();
    Footer footer = new Footer(new Path(qualifiedRoot, "one"), mock);
    footers.add(footer);

    // This should not throw an exception
    ParquetFileWriter.writeMetadataFile(conf, relativeRoot, footers, JobSummaryLevel.ALL);
  }

}
