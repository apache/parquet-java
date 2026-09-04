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

import static org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics;
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.MAX_STATS_SIZE;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.ParquetInputFormat.READ_SUPPORT_CLASS;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ConcatenatingKeyValueMetadataMergeStrategy;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.GlobalMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.metadata.StrictKeyValueMetadataMergeStrategy;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.internal.column.columnindex.BinaryTruncator;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that data can be written and read back again.
 * The test suite is parameterized on vector IO being disabled/enabled.
 * This verifies that the vector IO code path is correct, and that
 * the default path continues to work.
 */
public class TestParquetFileWriter {

  private static final Logger LOG = LoggerFactory.getLogger(TestParquetFileWriter.class);

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message m {"
      + "  required group a {"
      + "    required binary b;"
      + "  }"
      + "  required group c {"
      + "    required int64 d;"
      + "  }"
      + "}");
  private static final String[] PATH1 = {"a", "b"};
  private static final ColumnDescriptor C1 = SCHEMA.getColumnDescription(PATH1);
  private static final String[] PATH2 = {"c", "d"};
  private static final ColumnDescriptor C2 = SCHEMA.getColumnDescription(PATH2);

  private static final byte[] BYTES1 = {0, 1, 2, 3};
  private static final byte[] BYTES2 = {1, 2, 3, 4};
  private static final byte[] BYTES3 = {2, 3, 4, 5};
  private static final byte[] BYTES4 = {3, 4, 5, 6};
  private static final CompressionCodecName CODEC = CompressionCodecName.UNCOMPRESSED;

  private static final org.apache.parquet.column.statistics.Statistics<?> EMPTY_STATS =
      org.apache.parquet.column.statistics.Statistics.getBuilderForReading(
              Types.required(PrimitiveTypeName.BINARY).named("test_binary"))
          .build();

  @TempDir
  private java.nio.file.Path tempDir;

  /**
   * Get the configuration for the tests.
   * @param vectoredRead use vector IO for reading
   * @return a configuration which may have vector IO set.
   */
  private Configuration getTestConfiguration(boolean vectoredRead) {
    Configuration conf = new Configuration();
    // set the vector IO option
    conf.setBoolean(ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED, vectoredRead);
    return conf;
  }

  private TrackingByteBufferAllocator allocator;

  @BeforeEach
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @AfterEach
  public void closeAllocator() {
    allocator.close();
  }

  private ParquetFileWriter createWriter(Configuration conf, MessageType schema, Path path) throws IOException {
    return createWriter(conf, schema, path, CREATE);
  }

  private ParquetFileWriter createWriter(
      Configuration conf, MessageType schema, Path path, ParquetFileWriter.Mode mode) throws IOException {
    return new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, conf),
        schema,
        mode,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        null,
        ParquetProperties.builder().withAllocator(allocator).build());
  }

  private ParquetFileWriter createWriter(
      Configuration conf, MessageType schema, Path path, long blockSize, int maxPaddingSize) throws IOException {
    return new ParquetFileWriter(conf, schema, path, blockSize, maxPaddingSize, Integer.MAX_VALUE, allocator);
  }

  @Test
  public void testWriteMode() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { required group a {required binary b;} required group " + "c { required int64 d; }}");
    Configuration conf = new Configuration();
    Path path = existingTempPath();
    assertThatThrownBy(() -> createWriter(conf, schema, path))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("already exists");
    assertThatCode(() -> createWriter(conf, schema, path, OVERWRITE)).doesNotThrowAnyException();
    Files.deleteIfExists(java.nio.file.Paths.get(path.toUri()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteRead(boolean vectoredRead) throws Exception {
    Path path = newTempPath();
    Configuration configuration = getTestConfiguration(vectoredRead);

    ParquetFileWriter w = createWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    long c1Starts = w.getPos();
    long c1p1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Ends = w.getPos();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDictionaryPage(new DictionaryPage(BytesInput.from(BYTES2), 4, RLE_DICTIONARY));
    long c2p1Starts = w.getPos();
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
    // Although writer is already closed in previous end(),
    // explicitly close it again to verify double close behavior.
    w.close();

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
    assertThat(readFooter.getBlocks()).as("footer: " + readFooter).hasSize(2);
    BlockMetaData rowGroup = readFooter.getBlocks().get(0);
    assertThat(rowGroup.getColumns().get(0).getTotalSize()).isEqualTo(c1Ends - c1Starts);
    assertThat(rowGroup.getColumns().get(1).getTotalSize()).isEqualTo(c2Ends - c2Starts);
    assertThat(rowGroup.getTotalByteSize()).isEqualTo(c2Ends - c1Starts);

    assertThat(rowGroup.getColumns().get(0).getStartingPos()).isEqualTo(c1Starts);
    assertThat(rowGroup.getColumns().get(0).getDictionaryPageOffset()).isEqualTo(0);
    assertThat(rowGroup.getColumns().get(0).getFirstDataPageOffset()).isEqualTo(c1p1Starts);
    assertThat(rowGroup.getColumns().get(1).getStartingPos()).isEqualTo(c2Starts);
    assertThat(rowGroup.getColumns().get(1).getDictionaryPageOffset()).isEqualTo(c2Starts);
    assertThat(rowGroup.getColumns().get(1).getFirstDataPageOffset()).isEqualTo(c2p1Starts);

    HashSet<Encoding> expectedEncoding = new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    expectedEncoding.add(BIT_PACKED);
    assertThat(rowGroup.getColumns().get(0).getEncodings()).isEqualTo(expectedEncoding);

    { // read first block of col #1
      try (ParquetFileReader r = new ParquetFileReader(
          configuration,
          readFooter.getFileMetaData(),
          path,
          List.of(rowGroup),
          List.of(SCHEMA.getColumnDescription(PATH1)))) {
        PageReadStore pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(3);
        validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
        assertThat(r.readNextRowGroup()).isNull();
      }
    }

    { // read all blocks of col #1 and #2
      try (ParquetFileReader r = new ParquetFileReader(
          configuration,
          readFooter.getFileMetaData(),
          path,
          readFooter.getBlocks(),
          List.of(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)))) {

        PageReadStore pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(3);
        validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH2, 2, BytesInput.from(BYTES2));
        validateContains(SCHEMA, pages, PATH2, 3, BytesInput.from(BYTES2));
        validateContains(SCHEMA, pages, PATH2, 1, BytesInput.from(BYTES2));

        pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(4);

        validateContains(SCHEMA, pages, PATH1, 7, BytesInput.from(BYTES3));
        validateContains(SCHEMA, pages, PATH2, 8, BytesInput.from(BYTES4));

        assertThat(r.readNextRowGroup()).isNull();
      }
    }
    PrintFooter.main(new String[] {path.toString()});
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteReadWithRecordReader(boolean vectoredRead) throws Exception {
    Path path = newTempPath();
    Configuration configuration = getTestConfiguration(vectoredRead);

    ParquetFileWriter w = createWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 6, CODEC);
    long c2Starts = w.getPos();
    w.writeDictionaryPage(new DictionaryPage(BytesInput.from(BYTES2), 4, RLE_DICTIONARY));
    long c2p1Starts = w.getPos();
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c2Ends = w.getPos();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    long c1Bock2Starts = w.getPos();
    long c1p1Bock2Starts = w.getPos();
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    long c1Block2Ends = w.getPos();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<String, String>());

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
    assertThat(readFooter.getBlocks()).as("footer: " + readFooter).hasSize(2);
    BlockMetaData rowGroup = readFooter.getBlocks().get(0);
    assertThat(rowGroup.getColumns().get(1).getTotalSize()).isEqualTo(c2Ends - c2Starts);

    assertThat(rowGroup.getColumns().get(0).getDictionaryPageOffset()).isEqualTo(0);
    assertThat(rowGroup.getColumns().get(1).getStartingPos()).isEqualTo(c2Starts);
    assertThat(rowGroup.getColumns().get(1).getDictionaryPageOffset()).isEqualTo(c2Starts);
    assertThat(rowGroup.getColumns().get(1).getFirstDataPageOffset()).isEqualTo(c2p1Starts);

    BlockMetaData rowGroup2 = readFooter.getBlocks().get(1);
    assertThat(rowGroup2.getColumns().get(0).getDictionaryPageOffset()).isEqualTo(0);
    assertThat(rowGroup2.getColumns().get(0).getStartingPos()).isEqualTo(c1Bock2Starts);
    assertThat(rowGroup2.getColumns().get(0).getFirstDataPageOffset()).isEqualTo(c1p1Bock2Starts);
    assertThat(rowGroup2.getColumns().get(0).getTotalSize()).isEqualTo(c1Block2Ends - c1Bock2Starts);

    HashSet<Encoding> expectedEncoding = new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    expectedEncoding.add(BIT_PACKED);
    assertThat(rowGroup.getColumns().get(0).getEncodings()).isEqualTo(expectedEncoding);

    ParquetInputSplit split = new ParquetInputSplit(
        path,
        0,
        w.getPos(),
        null,
        readFooter.getBlocks(),
        SCHEMA.toString(),
        readFooter.getFileMetaData().getSchema().toString(),
        readFooter.getFileMetaData().getKeyValueMetaData(),
        null);
    ParquetInputFormat input = new ParquetInputFormat();
    configuration.set(READ_SUPPORT_CLASS, GroupReadSupport.class.getName());
    TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt_0_1_m_1_1");
    TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(configuration, taskAttemptID);
    RecordReader<Void, ArrayWritable> reader = input.createRecordReader(split, taskContext);
    assertThat(reader).isInstanceOf(ParquetRecordReader.class);
    // RowGroup.file_offset is checked here
    reader.initialize(split, taskContext);
    reader.close();
  }

  @Test
  public void testWriteEmptyBlock() throws Exception {
    Path path = newTempPath();
    Configuration configuration = new Configuration();

    ParquetFileWriter w = createWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(0);

    assertThatThrownBy(w::endBlock)
        .isInstanceOf(ParquetEncodingException.class)
        .hasMessage("End block with zero record");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBloomFilterWriteRead(boolean vectoredRead) throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary foo; }");
    Path path = newTempPath();
    Configuration configuration = getTestConfiguration(vectoredRead);
    configuration.set("parquet.bloom.filter.column.names", "foo");
    String[] colPath = {"foo"};
    ColumnDescriptor col = schema.getColumnDescription(colPath);
    BinaryStatistics stats1 = new BinaryStatistics();
    ParquetFileWriter w = createWriter(configuration, schema, path);
    w.start();
    w.startBlock(3);
    w.startColumn(col, 5, CODEC);
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    BloomFilter blockSplitBloomFilter = new BlockSplitBloomFilter(0);
    blockSplitBloomFilter.insertHash(blockSplitBloomFilter.hash(Binary.fromString("hello")));
    blockSplitBloomFilter.insertHash(blockSplitBloomFilter.hash(Binary.fromString("world")));
    w.addBloomFilter("foo", blockSplitBloomFilter);
    w.endBlock();
    w.end(new HashMap<>());
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);

    try (ParquetFileReader r = new ParquetFileReader(
        configuration,
        readFooter.getFileMetaData(),
        path,
        List.of(readFooter.getBlocks().get(0)),
        List.of(schema.getColumnDescription(colPath)))) {
      BloomFilterReader bloomFilterReader =
          r.getBloomFilterDataReader(readFooter.getBlocks().get(0));
      BloomFilter bloomFilter = bloomFilterReader.readBloomFilter(
          readFooter.getBlocks().get(0).getColumns().get(0));
      assertThat(bloomFilter.findHash(blockSplitBloomFilter.hash(Binary.fromString("hello"))))
          .isTrue();
      assertThat(bloomFilter.findHash(blockSplitBloomFilter.hash(Binary.fromString("world"))))
          .isTrue();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteReadDataPageV2(boolean vectoredRead) throws Exception {
    Path path = newTempPath();
    Configuration configuration = getTestConfiguration(vectoredRead);

    ParquetFileWriter w = createWriter(configuration, SCHEMA, path);
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
    w.writeDataPageV2(4, 1, 3, repLevels, defLevels, PLAIN, data, false, 4, statsC1P1);
    w.writeDataPageV2(3, 0, 3, repLevels, defLevels, PLAIN, data, false, 4, statsC1P2);
    w.endColumn();
    long c1Ends = w.getPos();

    w.startColumn(C2, 5, CODEC);
    long c2Starts = w.getPos();
    w.writeDataPageV2(5, 2, 3, repLevels, defLevels, PLAIN, data2, false, 4, EMPTY_STATS);
    w.writeDataPageV2(2, 0, 2, repLevels, defLevels, PLAIN, data2, false, 4, EMPTY_STATS);
    w.endColumn();
    long c2Ends = w.getPos();

    w.endBlock();
    w.end(new HashMap<>());

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);
    assertThat(readFooter.getBlocks()).as("footer: " + readFooter).hasSize(1);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(0).getTotalSize())
        .isEqualTo(c1Ends - c1Starts);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(1).getTotalSize())
        .isEqualTo(c2Ends - c2Starts);
    assertThat(readFooter.getBlocks().get(0).getTotalByteSize()).isEqualTo(c2Ends - c1Starts);

    // check for stats
    org.apache.parquet.column.statistics.Statistics<?> expectedStats = createStatistics("b", "z", C1);
    TestUtils.assertStatsValuesEqual(
        expectedStats, readFooter.getBlocks().get(0).getColumns().get(0).getStatistics());

    HashSet<Encoding> expectedEncoding = new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(0).getEncodings())
        .isEqualTo(expectedEncoding);

    try (ParquetFileReader reader = new ParquetFileReader(
        configuration,
        readFooter.getFileMetaData(),
        path,
        readFooter.getBlocks(),
        List.of(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)))) {
      PageReadStore pages = reader.readNextRowGroup();
      assertThat(pages.getRowCount()).isEqualTo(14);
      validateV2Page(
          SCHEMA,
          pages,
          PATH1,
          3,
          4,
          1,
          repLevels.toByteArray(),
          defLevels.toByteArray(),
          data.toByteArray(),
          12);
      validateV2Page(
          SCHEMA,
          pages,
          PATH1,
          3,
          3,
          0,
          repLevels.toByteArray(),
          defLevels.toByteArray(),
          data.toByteArray(),
          12);
      validateV2Page(
          SCHEMA,
          pages,
          PATH2,
          3,
          5,
          2,
          repLevels.toByteArray(),
          defLevels.toByteArray(),
          data2.toByteArray(),
          12);
      validateV2Page(
          SCHEMA,
          pages,
          PATH2,
          2,
          2,
          0,
          repLevels.toByteArray(),
          defLevels.toByteArray(),
          data2.toByteArray(),
          12);
      assertThat(reader.readNextRowGroup()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAlignmentWithPadding(boolean vectoredRead) throws Exception {
    Path path = newTempPath();
    Configuration conf = getTestConfiguration(vectoredRead);
    // Disable writing out checksums as hardcoded byte offsets in assertions below expect it
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);

    // uses the test constructor
    ParquetFileWriter w = createWriter(conf, SCHEMA, path, 120, 60);

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

    long footerLen;
    try (FSDataInputStream data = fs.open(path)) {
      data.seek(fileLen - 8); // 4-byte offset + "PAR1"
      footerLen = BytesUtils.readIntLittleEndian(data);
    }
    long startFooter = fileLen - footerLen - 8;

    assertThat(startFooter)
        .as("Footer should start after second row group without padding")
        .isEqualTo(secondRowGroupEnds);

    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path);
    assertThat(readFooter.getBlocks()).as("footer: " + readFooter).hasSize(2);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(0).getTotalSize())
        .isEqualTo(c1Ends - c1Starts);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(1).getTotalSize())
        .isEqualTo(c2Ends - c2Starts);
    assertThat(readFooter.getBlocks().get(0).getTotalByteSize()).isEqualTo(c2Ends - c1Starts);
    HashSet<Encoding> expectedEncoding = new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    expectedEncoding.add(BIT_PACKED);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(0).getEncodings())
        .isEqualTo(expectedEncoding);

    // verify block starting positions with padding
    assertThat(readFooter.getBlocks().get(0).getStartingPos())
        .as("First row group should start after magic")
        .isEqualTo(4);
    assertThat(firstRowGroupEnds)
        .as("First row group should end before the block size (120)")
        .isLessThan(120);
    assertThat(readFooter.getBlocks().get(1).getStartingPos())
        .as("Second row group should start at the block size")
        .isEqualTo(120);

    { // read first block of col #1
      try (ParquetFileReader r = new ParquetFileReader(
          conf,
          readFooter.getFileMetaData(),
          path,
          List.of(readFooter.getBlocks().get(0)),
          List.of(SCHEMA.getColumnDescription(PATH1)))) {
        PageReadStore pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(3);
        validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
        assertThat(r.readNextRowGroup()).isNull();
      }
    }

    { // read all blocks of col #1 and #2
      try (ParquetFileReader r = new ParquetFileReader(
          conf,
          readFooter.getFileMetaData(),
          path,
          readFooter.getBlocks(),
          List.of(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)))) {

        PageReadStore pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(3);
        validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH2, 2, BytesInput.from(BYTES2));
        validateContains(SCHEMA, pages, PATH2, 3, BytesInput.from(BYTES2));
        validateContains(SCHEMA, pages, PATH2, 1, BytesInput.from(BYTES2));

        pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(4);

        validateContains(SCHEMA, pages, PATH1, 7, BytesInput.from(BYTES3));
        validateContains(SCHEMA, pages, PATH2, 8, BytesInput.from(BYTES4));

        assertThat(r.readNextRowGroup()).isNull();
      }
    }
    PrintFooter.main(new String[] {path.toString()});
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAlignmentWithNoPaddingNeeded(boolean vectoredRead) throws Exception {
    Path path = newTempPath();
    Configuration conf = getTestConfiguration(vectoredRead);
    // Disable writing out checksums as hardcoded byte offsets in assertions below expect it
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);
    // close any filesystems to ensure that the the FS used by the writer picks up the configuration
    FileSystem.closeAll();

    // uses the test constructor
    ParquetFileWriter w = createWriter(conf, SCHEMA, path, 100, 50);

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

    long footerLen;
    try (FSDataInputStream data = fs.open(path)) {
      data.seek(fileLen - 8); // 4-byte offset + "PAR1"
      footerLen = BytesUtils.readIntLittleEndian(data);
    }
    long startFooter = fileLen - footerLen - 8;

    assertThat(startFooter)
        .as("Footer should start after second row group without padding")
        .isEqualTo(secondRowGroupEnds);

    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path);
    assertThat(readFooter.getBlocks()).as("footer: " + readFooter).hasSize(2);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(0).getTotalSize())
        .isEqualTo(c1Ends - c1Starts);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(1).getTotalSize())
        .isEqualTo(c2Ends - c2Starts);
    assertThat(readFooter.getBlocks().get(0).getTotalByteSize()).isEqualTo(c2Ends - c1Starts);
    HashSet<Encoding> expectedEncoding = new HashSet<Encoding>();
    expectedEncoding.add(PLAIN);
    expectedEncoding.add(BIT_PACKED);
    assertThat(readFooter.getBlocks().get(0).getColumns().get(0).getEncodings())
        .isEqualTo(expectedEncoding);

    // verify block starting positions with padding
    assertThat(readFooter.getBlocks().get(0).getStartingPos())
        .as("First row group should start after magic")
        .isEqualTo(4);
    assertThat(firstRowGroupEnds)
        .as("First row group should end before the block size (120)")
        .isGreaterThan(100);
    assertThat(readFooter.getBlocks().get(1).getStartingPos())
        .as("Second row group should start after no padding")
        .isEqualTo(109);

    { // read first block of col #1
      try (ParquetFileReader r = new ParquetFileReader(
          conf,
          readFooter.getFileMetaData(),
          path,
          List.of(readFooter.getBlocks().get(0)),
          List.of(SCHEMA.getColumnDescription(PATH1)))) {
        PageReadStore pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(3);
        validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
        assertThat(r.readNextRowGroup()).isNull();
      }
    }

    { // read all blocks of col #1 and #2
      try (ParquetFileReader r = new ParquetFileReader(
          conf,
          readFooter.getFileMetaData(),
          path,
          readFooter.getBlocks(),
          List.of(SCHEMA.getColumnDescription(PATH1), SCHEMA.getColumnDescription(PATH2)))) {
        PageReadStore pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(3);
        validateContains(SCHEMA, pages, PATH1, 2, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH1, 3, BytesInput.from(BYTES1));
        validateContains(SCHEMA, pages, PATH2, 2, BytesInput.from(BYTES2));
        validateContains(SCHEMA, pages, PATH2, 3, BytesInput.from(BYTES2));
        validateContains(SCHEMA, pages, PATH2, 1, BytesInput.from(BYTES2));

        pages = r.readNextRowGroup();
        assertThat(pages.getRowCount()).isEqualTo(4);

        validateContains(SCHEMA, pages, PATH1, 7, BytesInput.from(BYTES3));
        validateContains(SCHEMA, pages, PATH2, 8, BytesInput.from(BYTES4));

        assertThat(r.readNextRowGroup()).isNull();
      }
    }
    PrintFooter.main(new String[] {path.toString()});
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConvertToThriftStatistics(boolean vectoredRead) throws Exception {
    long[] longArray =
        new long[] {39L, 99L, 12L, 1000L, 65L, 542L, 2533461316L, -253346131996L, Long.MAX_VALUE, Long.MIN_VALUE
        };
    LongStatistics parquetMRstats = new LongStatistics();

    for (long l : longArray) {
      parquetMRstats.updateStats(l);
    }
    final String createdBy = "parquet-mr version 1.8.0 (build d4d5a07ec9bd262ca1e93c309f1d7d4a74ebda4c)";
    Statistics thriftStats =
        org.apache.parquet.format.converter.ParquetMetadataConverter.toParquetStatistics(parquetMRstats);
    LongStatistics convertedBackStats =
        (LongStatistics) org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(
            createdBy, thriftStats, PrimitiveTypeName.INT64);

    assertThat(convertedBackStats.getMax()).isEqualTo(parquetMRstats.getMax());
    assertThat(convertedBackStats.getMin()).isEqualTo(parquetMRstats.getMin());
    assertThat(convertedBackStats.getNumNulls()).isEqualTo(parquetMRstats.getNumNulls());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteReadStatistics(boolean vectoredRead) throws Exception {
    // this test assumes statistics will be read
    assumeThat(shouldIgnoreStatistics(Version.FULL_VERSION, BINARY)).isFalse();

    Path path = newTempPath();
    Configuration configuration = getTestConfiguration(vectoredRead);
    configuration.setBoolean("parquet.strings.signed-min-max.enabled", true);

    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { required group a {required binary b (UTF8);} required group c { required int64 d; }}");
    String[] path1 = {"a", "b"};
    ColumnDescriptor c1 = schema.getColumnDescription(path1);
    String[] path2 = {"c", "d"};
    ColumnDescriptor c2 = schema.getColumnDescription(path2);

    byte[] bytes1 = {0, 1, 2, 3};
    byte[] bytes2 = {1, 2, 3, 4};
    byte[] bytes3 = {2, 3, 4, 5};
    byte[] bytes4 = {3, 4, 5, 6};
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

    ParquetFileWriter w = createWriter(configuration, schema, path);
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
      BinaryStatistics bsout = (BinaryStatistics)
          readFooter.getBlocks().get(0).getColumns().get(0).getStatistics();
      String str = new String(bsout.getMaxBytes());
      String str2 = new String(bsout.getMinBytes());

      TestUtils.assertStatsValuesEqual(
          bs1, readFooter.getBlocks().get(0).getColumns().get(0).getStatistics());
      TestUtils.assertStatsValuesEqual(
          ls1, readFooter.getBlocks().get(0).getColumns().get(1).getStatistics());
    }
    { // assert stats are correct for the second block
      TestUtils.assertStatsValuesEqual(
          bs2, readFooter.getBlocks().get(1).getColumns().get(0).getStatistics());
      TestUtils.assertStatsValuesEqual(
          ls2, readFooter.getBlocks().get(1).getColumns().get(1).getStatistics());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMetaDataFile(boolean vectoredRead) throws Exception {

    Path testDirPath = new Path(Files.createTempDirectory(tempDir, "folder").toUri());
    Configuration configuration = getTestConfiguration(vectoredRead);

    final FileSystem fs = testDirPath.getFileSystem(configuration);
    enforceEmptyDir(configuration, testDirPath);

    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { required group a {required binary b;} required group c { required int64 d; }}");
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
    assertThat(footers).hasSize(1);

    final FileStatus metadataFile =
        fs.getFileStatus(new Path(testDirPath, ParquetFileWriter.PARQUET_METADATA_FILE));
    final FileStatus metadataFileLight =
        fs.getFileStatus(new Path(testDirPath, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE));
    final List<Footer> metadata = ParquetFileReader.readSummaryFile(configuration, metadataFile);

    validateFooters(metadata);

    footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(
        configuration, List.of(fs.listStatus(testDirPath, HiddenFileFilter.INSTANCE)), false);
    validateFooters(footers);

    fs.delete(metadataFile.getPath(), false);
    fs.delete(metadataFileLight.getPath(), false);

    footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(
        configuration, List.of(fs.listStatus(testDirPath)), false);
    validateFooters(footers);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteReadStatisticsAllNulls(boolean vectoredRead) throws Exception {
    // this test assumes statistics will be read
    assumeThat(shouldIgnoreStatistics(Version.FULL_VERSION, BINARY)).isFalse();

    Path path = newTempPath();

    String writeSchema = "message example {\n" + "required binary content (UTF8);\n" + "}";

    MessageType schema = MessageTypeParser.parseMessageType(writeSchema);
    Configuration configuration = getTestConfiguration(vectoredRead);
    configuration.setBoolean("parquet.strings.signed-min-max.enabled", true);
    GroupWriteSupport.setSchema(schema, configuration);

    // close any filesystems to ensure that the FS used by the writer picks up the configuration
    FileSystem.closeAll();
    ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, new GroupWriteSupport());

    Group r1 = new SimpleGroup(schema);
    writer.write(r1);
    writer.close();

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);

    // assert the statistics object is not empty
    org.apache.parquet.column.statistics.Statistics stats =
        readFooter.getBlocks().get(0).getColumns().get(0).getStatistics();
    assertThat(stats.isEmpty()).as("is empty: " + stats).isFalse();
    // assert the number of nulls are correct for the first block
    assertThat(stats.getNumNulls()).as("nulls: " + stats).isEqualTo(1);
  }

  private void validateFooters(final List<Footer> metadata) {
    LOG.debug("{}", metadata);
    assertThat(metadata).as(String.valueOf(metadata)).hasSize(3);
    for (Footer footer : metadata) {
      final File file = new File(footer.getFile().toUri());
      assertThat(file.getName().startsWith("part")).as(file.getName()).isTrue();
      assertThat(file.exists()).as(file.getPath()).isTrue();
      final ParquetMetadata parquetMetadata = footer.getParquetMetadata();
      assertThat(parquetMetadata.getBlocks()).hasSize(2);
      final Map<String, String> keyValueMetaData =
          parquetMetadata.getFileMetaData().getKeyValueMetaData();
      assertThat(keyValueMetaData.get("foo")).isEqualTo("bar");
      assertThat(keyValueMetaData.get(footer.getFile().getName()))
          .isEqualTo(footer.getFile().getName());
    }
  }

  private void createFile(Configuration configuration, Path path, MessageType schema) throws IOException {
    String[] path1 = {"a", "b"};
    ColumnDescriptor c1 = schema.getColumnDescription(path1);
    String[] path2 = {"c", "d"};
    ColumnDescriptor c2 = schema.getColumnDescription(path2);

    byte[] bytes1 = {0, 1, 2, 3};
    byte[] bytes2 = {1, 2, 3, 4};
    byte[] bytes3 = {2, 3, 4, 5};
    byte[] bytes4 = {3, 4, 5, 6};
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

    BinaryStatistics stats1 = new BinaryStatistics();
    BinaryStatistics stats2 = new BinaryStatistics();

    ParquetFileWriter w = createWriter(configuration, schema, path);
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

  private void validateV2Page(
      MessageType schema,
      PageReadStore pages,
      String[] path,
      int values,
      int rows,
      int nullCount,
      byte[] repetition,
      byte[] definition,
      byte[] data,
      int uncompressedSize)
      throws IOException {
    PageReader pageReader = pages.getPageReader(schema.getColumnDescription(path));
    DataPageV2 page = (DataPageV2) pageReader.readPage();
    assertThat(page.getValueCount()).isEqualTo(values);
    assertThat(page.getRowCount()).isEqualTo(rows);
    assertThat(page.getNullCount()).isEqualTo(nullCount);
    assertThat(page.getUncompressedSize()).isEqualTo(uncompressedSize);
    assertThat(page.getRepetitionLevels().toByteArray()).isEqualTo(repetition);
    assertThat(page.getDefinitionLevels().toByteArray()).isEqualTo(definition);
    assertThat(page.getData().toByteArray()).isEqualTo(data);
  }

  private org.apache.parquet.column.statistics.Statistics<?> createStatistics(
      String min, String max, ColumnDescriptor col) {
    return org.apache.parquet.column.statistics.Statistics.getBuilderForReading(col.getPrimitiveType())
        .withMin(Binary.fromString(min).getBytes())
        .withMax(Binary.fromString(max).getBytes())
        .withNumNulls(0)
        .build();
  }

  private void validateContains(MessageType schema, PageReadStore pages, String[] path, int values, BytesInput bytes)
      throws IOException {
    PageReader pageReader = pages.getPageReader(schema.getColumnDescription(path));
    DataPage page = pageReader.readPage();
    assertThat(page.getValueCount()).isEqualTo(values);
    assertThat(((DataPageV1) page).getBytes().toByteArray()).isEqualTo(bytes.toByteArray());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeMetadata(boolean vectoredRead) {
    FileMetaData md1 = new FileMetaData(
        new MessageType(
            "root1", new PrimitiveType(REPEATED, BINARY, "a"), new PrimitiveType(OPTIONAL, BINARY, "b")),
        new HashMap<String, String>(),
        "test");
    FileMetaData md2 = new FileMetaData(
        new MessageType("root2", new PrimitiveType(REQUIRED, BINARY, "c")),
        new HashMap<String, String>(),
        "test2");
    GlobalMetaData merged = ParquetFileWriter.mergeInto(md2, ParquetFileWriter.mergeInto(md1, null));
    assertThat(new MessageType(
            "root1",
            new PrimitiveType(REPEATED, BINARY, "a"),
            new PrimitiveType(OPTIONAL, BINARY, "b"),
            new PrimitiveType(REQUIRED, BINARY, "c")))
        .isEqualTo(merged.getSchema());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeFooters(boolean vectoredRead) {
    List<BlockMetaData> oneBlocks = new ArrayList<BlockMetaData>();
    oneBlocks.add(new BlockMetaData());
    oneBlocks.add(new BlockMetaData());
    List<BlockMetaData> twoBlocks = new ArrayList<BlockMetaData>();
    twoBlocks.add(new BlockMetaData());
    List<BlockMetaData> expected = new ArrayList<BlockMetaData>();
    expected.addAll(oneBlocks);
    expected.addAll(twoBlocks);

    Footer one = new Footer(
        new Path("file:/tmp/output/one.parquet"),
        new ParquetMetadata(
            new FileMetaData(
                new MessageType(
                    "root1",
                    new PrimitiveType(REPEATED, BINARY, "a"),
                    new PrimitiveType(OPTIONAL, BINARY, "b")),
                new HashMap<String, String>(),
                "test"),
            oneBlocks));

    Footer two = new Footer(
        new Path("/tmp/output/two.parquet"),
        new ParquetMetadata(
            new FileMetaData(
                new MessageType("root2", new PrimitiveType(REQUIRED, BINARY, "c")),
                new HashMap<String, String>(),
                "test2"),
            twoBlocks));

    List<Footer> footers = new ArrayList<Footer>();
    footers.add(one);
    footers.add(two);

    ParquetMetadata merged = ParquetFileWriter.mergeFooters(new Path("/tmp"), footers);

    assertThat(merged.getFileMetaData().getSchema())
        .isEqualTo(new MessageType(
            "root1",
            new PrimitiveType(REPEATED, BINARY, "a"),
            new PrimitiveType(OPTIONAL, BINARY, "b"),
            new PrimitiveType(REQUIRED, BINARY, "c")));

    assertThat(merged.getBlocks()).as("Should have all blocks").isEqualTo(expected);
  }

  /**
   * {@link ParquetFileWriter#mergeFooters(Path, List)} expects a fully-qualified
   * path for the root and crashes if a relative one is provided.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteMetadataFileWithRelativeOutputPath(boolean vectoredRead) throws IOException {
    Configuration conf = getTestConfiguration(vectoredRead);
    FileSystem fs = FileSystem.get(conf);
    Path relativeRoot = new Path("target/_test_relative");
    Path qualifiedRoot = fs.makeQualified(relativeRoot);

    ParquetMetadata mock = Mockito.mock(ParquetMetadata.class);
    FileMetaData fileMetaData = new FileMetaData(
        new MessageType("root1", new PrimitiveType(REPEATED, BINARY, "a")),
        new HashMap<String, String>(),
        "test");
    Mockito.when(mock.getFileMetaData()).thenReturn(fileMetaData);

    List<Footer> footers = new ArrayList<Footer>();
    Footer footer = new Footer(new Path(qualifiedRoot, "one"), mock);
    footers.add(footer);

    // This should not throw an exception
    ParquetFileWriter.writeMetadataFile(conf, relativeRoot, footers, JobSummaryLevel.ALL);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testColumnIndexWriteRead(boolean vectoredRead) throws Exception {
    // Don't truncate
    testColumnIndexWriteRead(vectoredRead, Integer.MAX_VALUE);
    // Truncate to DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH
    testColumnIndexWriteRead(vectoredRead, ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH);
  }

  private void testColumnIndexWriteRead(boolean vectoredRead, int columnIndexTruncateLen) throws Exception {
    Path path = newTempPath();
    Configuration configuration = getTestConfiguration(vectoredRead);

    ParquetFileWriter w = new ParquetFileWriter(
        configuration,
        SCHEMA,
        path,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        columnIndexTruncateLen,
        allocator);
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
    w.writeDataPage(
        2,
        4,
        BytesInput.from(BYTES1),
        statsC1(null, Binary.fromString("aaa")),
        1,
        BIT_PACKED,
        BIT_PACKED,
        PLAIN);
    long c1p2Starts = w.getPos();
    w.writeDataPage(
        3,
        4,
        BytesInput.from(BYTES1),
        statsC1(Binary.fromString("bbb"), Binary.fromString("ccc")),
        3,
        BIT_PACKED,
        BIT_PACKED,
        PLAIN);
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
    w.writeDataPage(
        7,
        4,
        BytesInput.from(BYTES3),
        // Creating huge stats so the column index will reach the limit and won't be written
        statsC1(
            Binary.fromConstantByteArray(new byte[(int) MAX_STATS_SIZE]),
            Binary.fromConstantByteArray(new byte[1])),
        4,
        BIT_PACKED,
        BIT_PACKED,
        PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<String, String>());

    try (ParquetFileReader reader = new ParquetFileReader(
        HadoopInputFile.fromPath(path, configuration),
        ParquetReadOptions.builder().build())) {
      ParquetMetadata footer = reader.getFooter();
      assertThat(footer.getBlocks()).hasSize(3);
      BlockMetaData blockMeta = footer.getBlocks().get(1);
      assertThat(blockMeta.getColumns()).hasSize(2);

      ColumnIndex columnIndex =
          reader.readColumnIndex(blockMeta.getColumns().get(0));
      assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
      assertThat(columnIndex.getNullCounts()).containsExactly(1L, 0L);
      assertThat(columnIndex.getNullPages()).containsExactly(false, false);
      List<ByteBuffer> minValues = columnIndex.getMinValues();
      assertThat(minValues).hasSize(2);
      List<ByteBuffer> maxValues = columnIndex.getMaxValues();
      assertThat(maxValues).hasSize(2);
      assertThat(new String(minValues.get(0).array(), StandardCharsets.UTF_8))
          .isEqualTo("aaa");
      assertThat(new String(maxValues.get(0).array(), StandardCharsets.UTF_8))
          .isEqualTo("aaa");
      assertThat(new String(minValues.get(1).array(), StandardCharsets.UTF_8))
          .isEqualTo("bbb");
      assertThat(new String(maxValues.get(1).array(), StandardCharsets.UTF_8))
          .isEqualTo("ccc");

      columnIndex = reader.readColumnIndex(blockMeta.getColumns().get(1));
      assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
      assertThat(columnIndex.getNullCounts()).containsExactly(0L, 3L, 0L);
      assertThat(columnIndex.getNullPages()).containsExactly(false, true, false);
      minValues = columnIndex.getMinValues();
      assertThat(minValues).hasSize(3);
      maxValues = columnIndex.getMaxValues();
      assertThat(maxValues).hasSize(3);
      assertThat(BytesUtils.bytesToLong(minValues.get(0).array())).isEqualTo(100);
      assertThat(BytesUtils.bytesToLong(maxValues.get(0).array())).isEqualTo(117);
      assertThat(minValues.get(1).array().length).isEqualTo(0);
      assertThat(maxValues.get(1).array().length).isEqualTo(0);
      assertThat(BytesUtils.bytesToLong(minValues.get(2).array())).isEqualTo(0);
      assertThat(BytesUtils.bytesToLong(maxValues.get(2).array())).isEqualTo(0);

      OffsetIndex offsetIndex =
          reader.readOffsetIndex(blockMeta.getColumns().get(0));
      assertThat(offsetIndex.getPageCount()).isEqualTo(2);
      assertThat(offsetIndex.getOffset(0)).isEqualTo(c1p1Starts);
      assertThat(offsetIndex.getOffset(1)).isEqualTo(c1p2Starts);
      assertThat(offsetIndex.getCompressedPageSize(0)).isEqualTo(c1p2Starts - c1p1Starts);
      assertThat(offsetIndex.getCompressedPageSize(1)).isEqualTo(c1Ends - c1p2Starts);
      assertThat(offsetIndex.getFirstRowIndex(0)).isEqualTo(0);
      assertThat(offsetIndex.getFirstRowIndex(1)).isEqualTo(1);

      offsetIndex = reader.readOffsetIndex(blockMeta.getColumns().get(1));
      assertThat(offsetIndex.getPageCount()).isEqualTo(3);
      assertThat(offsetIndex.getOffset(0)).isEqualTo(c2p1Starts);
      assertThat(offsetIndex.getOffset(1)).isEqualTo(c2p2Starts);
      assertThat(offsetIndex.getOffset(2)).isEqualTo(c2p3Starts);
      assertThat(offsetIndex.getCompressedPageSize(0)).isEqualTo(c2p2Starts - c2p1Starts);
      assertThat(offsetIndex.getCompressedPageSize(1)).isEqualTo(c2p3Starts - c2p2Starts);
      assertThat(offsetIndex.getCompressedPageSize(2)).isEqualTo(c2Ends - c2p3Starts);
      assertThat(offsetIndex.getFirstRowIndex(0)).isEqualTo(0);
      assertThat(offsetIndex.getFirstRowIndex(1)).isEqualTo(1);
      assertThat(offsetIndex.getFirstRowIndex(2)).isEqualTo(3);

      if (columnIndexTruncateLen == Integer.MAX_VALUE) {
        assertThat(reader.readColumnIndex(
                footer.getBlocks().get(2).getColumns().get(0)))
            .isNull();
      } else {
        blockMeta = footer.getBlocks().get(2);
        assertThat(reader.readColumnIndex(blockMeta.getColumns().get(0)))
            .isNotNull();
        columnIndex = reader.readColumnIndex(blockMeta.getColumns().get(0));
        assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
        assertThat(columnIndex.getNullCounts()).containsExactly(0L);
        assertThat(columnIndex.getNullPages()).containsExactly(false);
        minValues = columnIndex.getMinValues();
        assertThat(minValues).hasSize(1);
        maxValues = columnIndex.getMaxValues();
        assertThat(maxValues).hasSize(1);

        BinaryTruncator truncator =
            BinaryTruncator.getTruncator(SCHEMA.getType(PATH1).asPrimitiveType());
        assertThat(new String(minValues.get(0).array(), StandardCharsets.UTF_8))
            .isEqualTo(new String(new byte[1], StandardCharsets.UTF_8));
        byte[] truncatedMaxValue = truncator
            .truncateMax(
                Binary.fromConstantByteArray(new byte[(int) MAX_STATS_SIZE]), columnIndexTruncateLen)
            .getBytes();
        assertThat(new String(maxValues.get(0).array(), StandardCharsets.UTF_8))
            .isEqualTo(new String(truncatedMaxValue, StandardCharsets.UTF_8));

        assertThat(reader.readColumnIndex(blockMeta.getColumns().get(1)))
            .isNull();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeMetadataWithConflictingKeyValues(boolean vectoredRead) {
    Map<String, String> keyValues1 = new HashMap<String, String>() {
      {
        put("a", "b");
      }
    };
    Map<String, String> keyValues2 = new HashMap<String, String>() {
      {
        put("a", "c");
      }
    };
    FileMetaData md1 = new FileMetaData(
        new MessageType(
            "root1", new PrimitiveType(REPEATED, BINARY, "a"), new PrimitiveType(OPTIONAL, BINARY, "b")),
        keyValues1,
        "test");
    FileMetaData md2 = new FileMetaData(
        new MessageType(
            "root1", new PrimitiveType(REPEATED, BINARY, "a"), new PrimitiveType(OPTIONAL, BINARY, "b")),
        keyValues2,
        "test");
    GlobalMetaData merged = ParquetFileWriter.mergeInto(md2, ParquetFileWriter.mergeInto(md1, null));
    assertThatThrownBy(() -> merged.merge(new StrictKeyValueMetadataMergeStrategy()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("could not merge metadata");

    Map<String, String> mergedKeyValues =
        merged.merge(new ConcatenatingKeyValueMetadataMergeStrategy()).getKeyValueMetaData();
    assertThat(mergedKeyValues).hasSize(1);
    String mergedValue = mergedKeyValues.get("a");
    assertThat(mergedValue).isIn("b,c", "c,b");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeMetadataWithNoConflictingKeyValues(boolean vectoredRead) {
    Map<String, String> keyValues1 = new HashMap<String, String>() {
      {
        put("a", "b");
      }
    };
    Map<String, String> keyValues2 = new HashMap<String, String>() {
      {
        put("c", "d");
      }
    };
    FileMetaData md1 = new FileMetaData(
        new MessageType(
            "root1", new PrimitiveType(REPEATED, BINARY, "a"), new PrimitiveType(OPTIONAL, BINARY, "b")),
        keyValues1,
        "test");
    FileMetaData md2 = new FileMetaData(
        new MessageType(
            "root1", new PrimitiveType(REPEATED, BINARY, "a"), new PrimitiveType(OPTIONAL, BINARY, "b")),
        keyValues2,
        "test");
    GlobalMetaData merged = ParquetFileWriter.mergeInto(md2, ParquetFileWriter.mergeInto(md1, null));
    Map<String, String> mergedValues =
        merged.merge(new StrictKeyValueMetadataMergeStrategy()).getKeyValueMetaData();
    assertThat(mergedValues.get("a")).isEqualTo("b");
    assertThat(mergedValues.get("c")).isEqualTo("d");
  }

  private org.apache.parquet.column.statistics.Statistics<?> statsC1(Binary... values) {
    org.apache.parquet.column.statistics.Statistics<?> stats =
        org.apache.parquet.column.statistics.Statistics.createStats(C1.getPrimitiveType());
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
    org.apache.parquet.column.statistics.Statistics<?> stats =
        org.apache.parquet.column.statistics.Statistics.createStats(C2.getPrimitiveType());
    for (Long value : values) {
      if (value == null) {
        stats.incrementNumNulls();
      } else {
        stats.updateStats(value);
      }
    }
    return stats;
  }

  private Path newTempPath() {
    return new Path(tempDir.resolve(java.util.UUID.randomUUID() + ".tmp").toUri());
  }

  private Path existingTempPath() throws IOException {
    return new Path(Files.createTempFile(tempDir, "test", ".tmp").toUri());
  }
}
