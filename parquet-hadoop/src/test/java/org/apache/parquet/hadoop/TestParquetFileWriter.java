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
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
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
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.Assert.*;
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.MAX_STATS_SIZE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.*;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;

import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestParquetFileWriter {

  private static final Logger LOG = LoggerFactory.getLogger(TestParquetFileWriter.class);

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

  private static final org.apache.parquet.column.statistics.Statistics<?> EMPTY_STATS = org.apache.parquet.column.statistics.Statistics
      .getBuilderForReading(Types.required(PrimitiveTypeName.BINARY).named("test_binary")).build();

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
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
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
  public void testBloomFilterWriteRead() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary foo; }");
    File testFile = temp.newFile();
    testFile.delete();
    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();
    configuration.set("parquet.bloom.filter.column.names", "foo");
    String[] colPath = {"foo"};
    ColumnDescriptor col = schema.getColumnDescription(colPath);
    BinaryStatistics stats1 = new BinaryStatistics();
    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(3);
    w.startColumn(col, 5, CODEC);
    w.writeDataPage(2, 4, BytesInput.from(BYTES1),stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1),stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    BloomFilter blockSplitBloomFilter = new BlockSplitBloomFilter(0);
    blockSplitBloomFilter.insertHash(blockSplitBloomFilter.hash(Binary.fromString("hello")));
    blockSplitBloomFilter.insertHash(blockSplitBloomFilter.hash(Binary.fromString("world")));
    w.addBloomFilter("foo", blockSplitBloomFilter);
    w.endBlock();
    w.end(new HashMap<>());
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
    ParquetFileReader r = new ParquetFileReader(configuration, readFooter.getFileMetaData(), path,
      Arrays.asList(readFooter.getBlocks().get(0)), Arrays.asList(schema.getColumnDescription(colPath)));
    BloomFilterReader bloomFilterReader = r.getBloomFilterDataReader(readFooter.getBlocks().get(0));
    BloomFilter bloomFilter = bloomFilterReader.readBloomFilter(readFooter.getBlocks().get(0).getColumns().get(0));
    assertTrue(bloomFilter.findHash(blockSplitBloomFilter.hash(Binary.fromString("hello"))));
    assertTrue(bloomFilter.findHash(blockSplitBloomFilter.hash(Binary.fromString("world"))));
  }

  @Test
  public void testWriteReadDataPageV2() throws Exception {
    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    ParquetFileWriter w = new ParquetFileWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(14);

    BytesInput repLevels = BytesInput.fromInt(2);
    BytesInput defLevels = BytesInput.fromInt(1);
    BytesInput data = BytesInput.fromInt(3);
    BytesInput data2 = BytesInput.fromInt(10);

    org.apache.parquet.column.statistics.Statistics<?> statsC1P1 = createStatistics("s", "z", C1);
    org.apache.parquet.column.statistics.Statistics<?> statsC1P2 = createStatistics("b", "d", C1);

    w.startColumn(C1, 6, CODEC);
    long c1Starts = w.getPos();
    w.writeDataPageV2(4, 1, 3, repLevels, defLevels, PLAIN, data,  4, statsC1P1);
    w.writeDataPageV2(3, 0, 3, repLevels, defLevels, PLAIN, data,  4, statsC1P2);
    w.endColumn();
    long c1Ends = w.getPos();

    w.startColumn(C2, 5, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPageV2(5, 2, 3, repLevels, defLevels, PLAIN, data2,  4, EMPTY_STATS);
    w.writeDataPageV2(2, 0, 2, repLevels, defLevels, PLAIN, data2,  4, EMPTY_STATS);
    w.endColumn();
    long c2Ends = w.getPos();

    w.endBlock();
    w.end(new HashMap<>());

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
    assertEquals("footer: "+ readFooter, 1, readFooter.getBlocks().size());
    assertEquals(c1Ends - c1Starts, readFooter.getBlocks().get(0).getColumns().get(0).getTotalSize());
    assertEquals(c2Ends - c2Starts, readFooter.getBlocks().get(0).getColumns().get(1).getTotalSize());
    assertEquals(c2Ends - c1Starts, readFooter.getBlocks().get(0).getTotalByteSize());

    //check for stats
    org.apache.parquet.column.statistics.Statistics<?> expectedStats = createStatistics("b", "z", C1);
    TestUtils.assertStatsValuesEqual(expectedStats, readFooter.getBlocks().get(0).getColumns().get(0).getStatistics());

    HashSet<Encoding> expectedEncoding = new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    assertEquals(expectedEncoding, readFooter.getBlocks().get(0).getColumns().get(0).getEncodings());

    ParquetFileReader reader = new ParquetFileReader(configuration, readFooter.getFileMetaData(), path,
      readFooter.getBlocks(), Arrays.asList(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)));

    PageReadStore pages = reader.readNextRowGroup();
    assertEquals(14, pages.getRowCount());
    validateV2Page(SCHEMA, pages, PATH1, 3, 4, 1, repLevels.toByteArray(), defLevels.toByteArray(), data.toByteArray(), 12);
    validateV2Page(SCHEMA, pages, PATH1, 3, 3, 0, repLevels.toByteArray(), defLevels.toByteArray(),data.toByteArray(), 12);
    validateV2Page(SCHEMA, pages, PATH2, 3, 5, 2, repLevels.toByteArray(), defLevels.toByteArray(), data2.toByteArray(), 12);
    validateV2Page(SCHEMA, pages, PATH2, 2, 2, 0, repLevels.toByteArray(), defLevels.toByteArray(), data2.toByteArray(), 12);
    assertNull(reader.readNextRowGroup());
  }

  @Test
  public void testAlignmentWithPadding() throws Exception {
    File testFile = temp.newFile();

    Path path = new Path(testFile.toURI());
    Configuration conf = new Configuration();
    // Disable writing out checksums as hardcoded byte offsets in assertions below expect it
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);

    // uses the test constructor
    ParquetFileWriter w = new ParquetFileWriter(conf, SCHEMA, path, 120, 60);

    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    long c1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();

    long firstRowGroupEnds = w.getPos(); // should be 109

    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
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
    // Disable writing out checksums as hardcoded byte offsets in assertions below expect it
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);

    // uses the test constructor
    ParquetFileWriter w = new ParquetFileWriter(conf, SCHEMA, path, 100, 50);

    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    long c1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();

    long firstRowGroupEnds = w.getPos(); // should be 109

    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
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
    LongStatistics parquetMRstats = new LongStatistics();

    for (long l: longArray) {
      parquetMRstats.updateStats(l);
    }
    final String createdBy =
        "parquet-mr version 1.8.0 (build d4d5a07ec9bd262ca1e93c309f1d7d4a74ebda4c)";
    Statistics thriftStats =
        org.apache.parquet.format.converter.ParquetMetadataConverter.toParquetStatistics(parquetMRstats);
    LongStatistics convertedBackStats =
        (LongStatistics) org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(
            createdBy, thriftStats, PrimitiveTypeName.INT64);

    assertEquals(parquetMRstats.getMax(), convertedBackStats.getMax());
    assertEquals(parquetMRstats.getMin(), convertedBackStats.getMin());
    assertEquals(parquetMRstats.getNumNulls(), convertedBackStats.getNumNulls());
  }

  @Test
  public void testWriteReadStatistics() throws Exception {
    // this test assumes statistics will be read
    Assume.assumeTrue(!shouldIgnoreStatistics(Version.FULL_VERSION, BINARY));

    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();
    configuration.setBoolean("parquet.strings.signed-min-max.enabled", true);

    MessageType schema = MessageTypeParser.parseMessageType("message m { required group a {required binary b (UTF8);} required group c { required int64 d; }}");
    String[] path1 = {"a", "b"};
    ColumnDescriptor c1 = schema.getColumnDescription(path1);
    String[] path2 = {"c", "d"};
    ColumnDescriptor c2 = schema.getColumnDescription(path2);

    byte[] bytes1 = { 0, 1, 2, 3};
    byte[] bytes2 = { 1, 2, 3, 4};
    byte[] bytes3 = { 2, 3, 4, 5};
    byte[] bytes4 = { 3, 4, 5, 6};
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

    BinaryStatistics statsB1C1P1 = new BinaryStatistics();
    BinaryStatistics statsB1C1P2 = new BinaryStatistics();
    LongStatistics statsB1C2P1 = new LongStatistics();
    LongStatistics statsB1C2P2 = new LongStatistics();
    BinaryStatistics statsB2C1P1 = new BinaryStatistics();
    LongStatistics statsB2C2P1 = new LongStatistics();
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
    BinaryStatistics bs1 = new BinaryStatistics();
    bs1.setMinMax(Binary.fromString("a"), Binary.fromString("z"));
    LongStatistics ls1 = new LongStatistics();
    ls1.setMinMax(-6l, 10l);

    BinaryStatistics bs2 = new BinaryStatistics();
    bs2.setMinMax(Binary.fromString("d"), Binary.fromString("e"));
    LongStatistics ls2 = new LongStatistics();
    ls2.setMinMax(11l, 122l);

    { // assert stats are correct for the first block
      BinaryStatistics bsout = (BinaryStatistics)readFooter.getBlocks().get(0).getColumns().get(0).getStatistics();
      String str = new String(bsout.getMaxBytes());
      String str2 = new String(bsout.getMinBytes());

      TestUtils.assertStatsValuesEqual(bs1, readFooter.getBlocks().get(0).getColumns().get(0).getStatistics());
      TestUtils.assertStatsValuesEqual(ls1, readFooter.getBlocks().get(0).getColumns().get(1).getStatistics());
    }
    { // assert stats are correct for the second block
      TestUtils.assertStatsValuesEqual(bs2, readFooter.getBlocks().get(1).getColumns().get(0).getStatistics());
      TestUtils.assertStatsValuesEqual(ls2, readFooter.getBlocks().get(1).getColumns().get(1).getStatistics());
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
            "required binary content (UTF8);\n" +
            "}";

    Path path = new Path(testFile.toURI());

    MessageType schema = MessageTypeParser.parseMessageType(writeSchema);
    Configuration configuration = new Configuration();
    configuration.setBoolean("parquet.strings.signed-min-max.enabled", true);
    GroupWriteSupport.setSchema(schema, configuration);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, new GroupWriteSupport());

    Group r1 = new SimpleGroup(schema);
    writer.write(r1);
    writer.close();

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);

    // assert the statistics object is not empty
    org.apache.parquet.column.statistics.Statistics stats = readFooter.getBlocks().get(0).getColumns().get(0).getStatistics();
    assertFalse("is empty: " + stats, stats.isEmpty());
    // assert the number of nulls are correct for the first block
    assertEquals("nulls: " + stats, 1, stats.getNumNulls());
  }

  private void validateFooters(final List<Footer> metadata) {
    LOG.debug("{}", metadata);
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

    BinaryStatistics stats1 = new BinaryStatistics();
    BinaryStatistics stats2 = new BinaryStatistics();

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

  private void validateV2Page(MessageType schema, PageReadStore pages, String[] path, int values, int rows, int nullCount,
                              byte[] repetition, byte[] definition, byte[] data, int uncompressedSize) throws IOException {
    PageReader pageReader = pages.getPageReader(schema.getColumnDescription(path));
    DataPageV2 page = (DataPageV2)pageReader.readPage();
    assertEquals(values, page.getValueCount());
    assertEquals(rows, page.getRowCount());
    assertEquals(nullCount, page.getNullCount());
    assertEquals(uncompressedSize, page.getUncompressedSize());
    assertArrayEquals(repetition, page.getRepetitionLevels().toByteArray());
    assertArrayEquals(definition, page.getDefinitionLevels().toByteArray());
    assertArrayEquals(data, page.getData().toByteArray());
  }

  private org.apache.parquet.column.statistics.Statistics<?> createStatistics(String min, String max, ColumnDescriptor col) {
    return org.apache.parquet.column.statistics.Statistics .getBuilderForReading(col.getPrimitiveType())
      .withMin(Binary.fromString(min).getBytes()).withMax(Binary.fromString(max).getBytes()).withNumNulls(0)
      .build();
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

  @Test
  public void testColumnIndexWriteRead() throws Exception {
    File testFile = temp.newFile();
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    ParquetFileWriter w = new ParquetFileWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 5, CODEC);
    long c1p1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), statsC1(null, Binary.fromString("aaa")), 1, BIT_PACKED, BIT_PACKED,
        PLAIN);
    long c1p2Starts = w.getPos();
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), statsC1(Binary.fromString("bbb"), Binary.fromString("ccc")), 3,
        BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2p1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), statsC2(117l, 100l), 1, BIT_PACKED, BIT_PACKED, PLAIN);
    long c2p2Starts = w.getPos();
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), statsC2(null, null, null), 2, BIT_PACKED, BIT_PACKED, PLAIN);
    long c2p3Starts = w.getPos();
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), statsC2(0l), 1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3),
        // Creating huge stats so the column index will reach the limit and won't be written
        statsC1(
            Binary.fromConstantByteArray(new byte[(int) MAX_STATS_SIZE]),
            Binary.fromConstantByteArray(new byte[1])),
        4, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<String, String>());

    try (ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(path, configuration),
        ParquetReadOptions.builder().build())) {
      ParquetMetadata footer = reader.getFooter();
      assertEquals(3, footer.getBlocks().size());
      BlockMetaData blockMeta = footer.getBlocks().get(1);
      assertEquals(2, blockMeta.getColumns().size());

      ColumnIndex columnIndex = reader.readColumnIndex(blockMeta.getColumns().get(0));
      assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
      assertTrue(Arrays.asList(1l, 0l).equals(columnIndex.getNullCounts()));
      assertTrue(Arrays.asList(false, false).equals(columnIndex.getNullPages()));
      List<ByteBuffer> minValues = columnIndex.getMinValues();
      assertEquals(2, minValues.size());
      List<ByteBuffer> maxValues = columnIndex.getMaxValues();
      assertEquals(2, maxValues.size());
      assertEquals("aaa", new String(minValues.get(0).array(), StandardCharsets.UTF_8));
      assertEquals("aaa", new String(maxValues.get(0).array(), StandardCharsets.UTF_8));
      assertEquals("bbb", new String(minValues.get(1).array(), StandardCharsets.UTF_8));
      assertEquals("ccc", new String(maxValues.get(1).array(), StandardCharsets.UTF_8));

      columnIndex = reader.readColumnIndex(blockMeta.getColumns().get(1));
      assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
      assertTrue(Arrays.asList(0l, 3l, 0l).equals(columnIndex.getNullCounts()));
      assertTrue(Arrays.asList(false, true, false).equals(columnIndex.getNullPages()));
      minValues = columnIndex.getMinValues();
      assertEquals(3, minValues.size());
      maxValues = columnIndex.getMaxValues();
      assertEquals(3, maxValues.size());
      assertEquals(100, BytesUtils.bytesToLong(minValues.get(0).array()));
      assertEquals(117, BytesUtils.bytesToLong(maxValues.get(0).array()));
      assertEquals(0, minValues.get(1).array().length);
      assertEquals(0, maxValues.get(1).array().length);
      assertEquals(0, BytesUtils.bytesToLong(minValues.get(2).array()));
      assertEquals(0, BytesUtils.bytesToLong(maxValues.get(2).array()));

      OffsetIndex offsetIndex = reader.readOffsetIndex(blockMeta.getColumns().get(0));
      assertEquals(2, offsetIndex.getPageCount());
      assertEquals(c1p1Starts, offsetIndex.getOffset(0));
      assertEquals(c1p2Starts, offsetIndex.getOffset(1));
      assertEquals(c1p2Starts - c1p1Starts, offsetIndex.getCompressedPageSize(0));
      assertEquals(c1Ends - c1p2Starts, offsetIndex.getCompressedPageSize(1));
      assertEquals(0, offsetIndex.getFirstRowIndex(0));
      assertEquals(1, offsetIndex.getFirstRowIndex(1));

      offsetIndex = reader.readOffsetIndex(blockMeta.getColumns().get(1));
      assertEquals(3, offsetIndex.getPageCount());
      assertEquals(c2p1Starts, offsetIndex.getOffset(0));
      assertEquals(c2p2Starts, offsetIndex.getOffset(1));
      assertEquals(c2p3Starts, offsetIndex.getOffset(2));
      assertEquals(c2p2Starts - c2p1Starts, offsetIndex.getCompressedPageSize(0));
      assertEquals(c2p3Starts - c2p2Starts, offsetIndex.getCompressedPageSize(1));
      assertEquals(c2Ends - c2p3Starts, offsetIndex.getCompressedPageSize(2));
      assertEquals(0, offsetIndex.getFirstRowIndex(0));
      assertEquals(1, offsetIndex.getFirstRowIndex(1));
      assertEquals(3, offsetIndex.getFirstRowIndex(2));

      assertNull(reader.readColumnIndex(footer.getBlocks().get(2).getColumns().get(0)));
    }
  }

  private org.apache.parquet.column.statistics.Statistics<?> statsC1(Binary... values) {
    org.apache.parquet.column.statistics.Statistics<?> stats = org.apache.parquet.column.statistics.Statistics
        .createStats(C1.getPrimitiveType());
    for (Binary value : values) {
      if (value == null) {
        stats.incrementNumNulls();
      } else {
        stats.updateStats(value);
      }
    }
    return stats;
  }

  private org.apache.parquet.column.statistics.Statistics<?> statsC2(Long... values) {
    org.apache.parquet.column.statistics.Statistics<?> stats = org.apache.parquet.column.statistics.Statistics
        .createStats(C2.getPrimitiveType());
    for (Long value : values) {
      if (value == null) {
        stats.incrementNumNulls();
      } else {
        stats.updateStats(value);
      }
    }
    return stats;
  }
}
