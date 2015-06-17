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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import static org.junit.Assert.*;
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.*;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;

import org.apache.parquet.hadoop.example.GroupWriteSupport;

public class TestParquetFileWriter {
  private static final Log LOG = Log.getLog(TestParquetFileWriter.class);
  private String writeSchema;

  @Test
  public void testWriteMode() throws Exception {
    File testDir = new File("target/test/TestParquetFileWriter/");
    testDir.mkdirs();
    File testFile = new File(testDir, "testParquetFile");
    testFile = testFile.getAbsoluteFile();
    testFile.createNewFile();
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
          ParquetFileWriter.Mode.OVERWRITE);
    } catch(IOException ioe2) {
      exceptionThrown = true;
    }
    assertTrue(!exceptionThrown);
    testFile.delete();
  }

  @Test
  public void testWriteRead() throws Exception {

    File testFile = new File("target/test/TestParquetFileWriter/testParquetFile").getAbsoluteFile();
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

    BinaryStatistics stats1 = new BinaryStatistics();
    BinaryStatistics stats2 = new BinaryStatistics();

    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(3);
    w.startColumn(c1, 5, codec);
    long c1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(bytes1), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(bytes1), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(c2, 6, codec);
    long c2Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(bytes2), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(bytes2), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(bytes2), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(c1, 7, codec);
    w.writeDataPage(7, 4, BytesInput.from(bytes3), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(c2, 8, codec);
    w.writeDataPage(8, 4, BytesInput.from(bytes4), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
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
      ParquetFileReader r = new ParquetFileReader(configuration, path, Arrays.asList(readFooter.getBlocks().get(0)), Arrays.asList(schema.getColumnDescription(path1)));
      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(schema, pages, path1, 2, BytesInput.from(bytes1));
      validateContains(schema, pages, path1, 3, BytesInput.from(bytes1));
      assertNull(r.readNextRowGroup());
    }

    { // read all blocks of col #1 and #2

      ParquetFileReader r = new ParquetFileReader(configuration, path, readFooter.getBlocks(), Arrays.asList(schema.getColumnDescription(path1), schema.getColumnDescription(path2)));

      PageReadStore pages = r.readNextRowGroup();
      assertEquals(3, pages.getRowCount());
      validateContains(schema, pages, path1, 2, BytesInput.from(bytes1));
      validateContains(schema, pages, path1, 3, BytesInput.from(bytes1));
      validateContains(schema, pages, path2, 2, BytesInput.from(bytes2));
      validateContains(schema, pages, path2, 3, BytesInput.from(bytes2));
      validateContains(schema, pages, path2, 1, BytesInput.from(bytes2));

      pages = r.readNextRowGroup();
      assertEquals(4, pages.getRowCount());

      validateContains(schema, pages, path1, 7, BytesInput.from(bytes3));
      validateContains(schema, pages, path2, 8, BytesInput.from(bytes4));

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
    Statistics thriftStats = org.apache.parquet.format.converter.ParquetMetadataConverter.toParquetStatistics(parquetMRstats);
    LongStatistics convertedBackStats = (LongStatistics) org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(thriftStats, PrimitiveTypeName.INT64);

    assertEquals(parquetMRstats.getMax(), convertedBackStats.getMax());
    assertEquals(parquetMRstats.getMin(), convertedBackStats.getMin());
    assertEquals(parquetMRstats.getNumNulls(), convertedBackStats.getNumNulls());
  }

  @Test
  public void testWriteReadStatistics() throws Exception {

    File testFile = new File("target/test/TestParquetFileWriter/testParquetFile").getAbsoluteFile();
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

      assertTrue(((BinaryStatistics)readFooter.getBlocks().get(0).getColumns().get(0).getStatistics()).equals(bs1));
      assertTrue(((LongStatistics)readFooter.getBlocks().get(0).getColumns().get(1).getStatistics()).equals(ls1));
    }
    { // assert stats are correct for the second block
      assertTrue(((BinaryStatistics)readFooter.getBlocks().get(1).getColumns().get(0).getStatistics()).equals(bs2));
      assertTrue(((LongStatistics)readFooter.getBlocks().get(1).getColumns().get(1).getStatistics()).equals(ls2));
    }
  }

  @Test
  public void testMetaDataFile() throws Exception {

    File testDir = new File("target/test/TestParquetFileWriter/testMetaDataFileDir").getAbsoluteFile();

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
    ParquetFileWriter.writeMetadataFile(configuration, testDirPath, footers);

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

    File testFile = new File("target/test/TestParquetFileWriter/testParquetFile").getAbsoluteFile();
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

}
