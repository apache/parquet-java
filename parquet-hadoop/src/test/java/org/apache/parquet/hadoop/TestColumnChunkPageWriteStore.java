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

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class TestColumnChunkPageWriteStore {

  // OutputFile implementation to expose the PositionOutputStream internally used by the writer
  private static class OutputFileForTesting implements OutputFile {
    private PositionOutputStream out;
    private final HadoopOutputFile file;

    OutputFileForTesting(Path path, Configuration conf) throws IOException {
      file = HadoopOutputFile.fromPath(path, conf);
    }

    PositionOutputStream out() {
      return out;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
      return out = file.create(blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
      return out = file.createOrOverwrite(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return file.supportsBlockSize();
    }

    @Override
    public long defaultBlockSize() {
      return file.defaultBlockSize();
    }

    @Override
    public String getPath() {
      return file.getPath();
    }
  }

  private int pageSize = 1024;
  private int initialSize = 1024;
  private Configuration conf;
  private TrackingByteBufferAllocator allocator;

  @BeforeEach
  public void initConfiguration() {
    this.conf = new Configuration();
  }

  @AfterEach
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void test() throws Exception {
    test(conf, allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator()));
  }

  @Test
  public void testWithDirectBuffers() throws Exception {
    Configuration config = new Configuration(conf);
    // we want to test the path with direct buffers so we need to enable this config as well
    // even though this file is not encrypted
    config.set(ParquetInputFormat.OFF_HEAP_DECRYPT_BUFFER_ENABLED, "true");
    test(config, allocator = TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator()));
  }

  public void test(Configuration config, ByteBufferAllocator allocator) throws Exception {
    Path file = new Path("target/test/TestColumnChunkPageWriteStore/test.parquet");
    Path root = file.getParent();
    FileSystem fs = file.getFileSystem(config);
    if (fs.exists(root)) {
      fs.delete(root, true);
    }
    fs.mkdirs(root);
    MessageType schema = MessageTypeParser.parseMessageType("message test { repeated binary bar; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    Encoding dataEncoding = PLAIN;
    int valueCount = 10;
    int d = 1;
    int r = 2;
    int v = 3;
    BytesInput definitionLevels = BytesInput.fromInt(d);
    BytesInput repetitionLevels = BytesInput.fromInt(r);
    Statistics<?> statistics = Statistics.getBuilderForReading(
            Types.required(PrimitiveTypeName.BINARY).named("test_binary"))
        .build();
    BytesInput data = BytesInput.fromInt(v);
    int rowCount = 5;
    int nullCount = 1;
    statistics.incrementNumNulls(nullCount);
    statistics.setMinMaxFromBytes(new byte[] {0, 1, 2}, new byte[] {0, 1, 2, 3});
    long pageOffset;
    long pageSize;

    {
      OutputFileForTesting outputFile = new OutputFileForTesting(file, config);
      ParquetFileWriter writer = new ParquetFileWriter(
          outputFile,
          schema,
          Mode.CREATE,
          ParquetWriter.DEFAULT_BLOCK_SIZE,
          ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
          null,
          ParquetProperties.builder().withAllocator(allocator).build());
      writer.start();
      writer.startBlock(rowCount);
      pageOffset = outputFile.out().getPos();
      try (ColumnChunkPageWriteStore store =
          new ColumnChunkPageWriteStore(compressor(GZIP), schema, allocator, Integer.MAX_VALUE)) {
        PageWriter pageWriter = store.getPageWriter(col);
        pageWriter.writePageV2(
            rowCount,
            nullCount,
            valueCount,
            repetitionLevels,
            definitionLevels,
            dataEncoding,
            data,
            statistics);
        store.flushToFileWriter(writer);
        pageSize = outputFile.out().getPos() - pageOffset;
      }
      writer.endBlock();
      writer.end(new HashMap<String, String>());
    }

    {
      ParquetMetadata footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
      ParquetFileReader reader = new ParquetFileReader(
          config, footer.getFileMetaData(), file, footer.getBlocks(), schema.getColumns());
      PageReadStore rowGroup = reader.readNextRowGroup();
      PageReader pageReader = rowGroup.getPageReader(col);
      DataPageV2 page = (DataPageV2) pageReader.readPage();
      assertThat(page.getRowCount()).isEqualTo(rowCount);
      assertThat(page.getNullCount()).isEqualTo(nullCount);
      assertThat(page.getValueCount()).isEqualTo(valueCount);
      assertThat(intValue(page.getDefinitionLevels())).isEqualTo(d);
      assertThat(intValue(page.getRepetitionLevels())).isEqualTo(r);
      assertThat(page.getDataEncoding()).isEqualTo(dataEncoding);
      assertThat(intValue(page.getData())).isEqualTo(v);

      // Checking column/offset indexes for the one page
      ColumnChunkMetaData column = footer.getBlocks().get(0).getColumns().get(0);
      ColumnIndex columnIndex = reader.readColumnIndex(column);
      assertThat(columnIndex.getMinValues().get(0).array()).isEqualTo(statistics.getMinBytes());
      assertThat(columnIndex.getMaxValues().get(0).array()).isEqualTo(statistics.getMaxBytes());
      assertThat(columnIndex.getNullCounts().get(0).longValue()).isEqualTo(statistics.getNumNulls());
      assertThat(columnIndex.getNullPages().get(0)).isFalse();
      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      assertThat(offsetIndex.getPageCount()).isEqualTo(1);
      assertThat(offsetIndex.getCompressedPageSize(0)).isEqualTo(pageSize);
      assertThat(offsetIndex.getFirstRowIndex(0)).isEqualTo(0);
      assertThat(offsetIndex.getOffset(0)).isEqualTo(pageOffset);

      reader.close();
    }
  }

  private int intValue(BytesInput in) throws IOException {
    return in.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  @Test
  public void testColumnOrderV1() throws IOException {
    ParquetFileWriter mockFileWriter = Mockito.mock(ParquetFileWriter.class);
    InOrder inOrder = inOrder(mockFileWriter);
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(UTF8)
        .named("a_string")
        .required(INT32)
        .named("an_int")
        .required(INT64)
        .named("a_long")
        .required(FLOAT)
        .named("a_float")
        .required(DOUBLE)
        .named("a_double")
        .named("order_test");

    BytesInput fakeData = BytesInput.fromInt(34);
    int fakeCount = 3;
    BinaryStatistics fakeStats = new BinaryStatistics();

    try (ColumnChunkPageWriteStore store = new ColumnChunkPageWriteStore(
        compressor(UNCOMPRESSED),
        schema,
        allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator()),
        Integer.MAX_VALUE)) {

      for (ColumnDescriptor col : schema.getColumns()) {
        PageWriter pageWriter = store.getPageWriter(col);
        pageWriter.writePage(fakeData, fakeCount, fakeStats, RLE, RLE, PLAIN);
      }

      // flush to the mock writer
      store.flushToFileWriter(mockFileWriter);
    }

    for (ColumnDescriptor col : schema.getColumns()) {
      inOrder.verify(mockFileWriter)
          .writeColumnChunk(
              eq(col),
              eq((long) fakeCount),
              eq(UNCOMPRESSED),
              isNull(DictionaryPage.class),
              any(),
              eq(fakeData.size()),
              eq(fakeData.size()),
              eq(fakeStats),
              any(),
              any(),
              same(ColumnIndexBuilder.getNoOpBuilder()), // Deprecated writePage -> no column index
              same(OffsetIndexBuilder.getNoOpBuilder()), // Deprecated writePage -> no offset index
              any(),
              any(),
              any(),
              any());
    }
  }

  private BytesInputCompressor compressor(CompressionCodecName codec) {
    return new CodecFactory(conf, pageSize).getCompressor(codec);
  }

  @Test
  public void perColumnCodecDefaultUsedWhenNotSet() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required binary col_a; required int32 col_b; }");
    ParquetProperties props = ParquetProperties.builder().build();

    Map<String, CompressionCodecName> codecs = writeAndReadCodecs(schema, SNAPPY, props);

    assertThat(codecs.get("col_a")).isEqualTo(SNAPPY);
    assertThat(codecs.get("col_b")).isEqualTo(SNAPPY);
  }

  @Test
  public void perColumnCodecOverridesDefaultForOneColumn() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required binary col_a; required int32 col_b; }");
    ParquetProperties props =
        ParquetProperties.builder().withCompressionCodec("col_a", ZSTD).build();

    Map<String, CompressionCodecName> codecs = writeAndReadCodecs(schema, SNAPPY, props);

    assertThat(codecs.get("col_a")).isEqualTo(ZSTD);
    assertThat(codecs.get("col_b")).isEqualTo(SNAPPY);
  }

  @Test
  public void perColumnCodecAllColumnsOverridden() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required binary col_a; required int32 col_b; }");
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .withCompressionCodec("col_b", GZIP)
        .build();

    Map<String, CompressionCodecName> codecs = writeAndReadCodecs(schema, SNAPPY, props);

    assertThat(codecs.get("col_a")).isEqualTo(ZSTD);
    assertThat(codecs.get("col_b")).isEqualTo(GZIP);
  }

  @Test
  public void perColumnLevelWithCodecRoundTrip() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required binary col_a; required int32 col_b; }");
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .withCompressionLevel("col_a", 10)
        .build();

    Map<String, CompressionCodecName> codecs = writeAndReadCodecs(schema, SNAPPY, props);

    assertThat(codecs.get("col_a")).isEqualTo(ZSTD);
    assertThat(codecs.get("col_b")).isEqualTo(SNAPPY);
  }

  @Test
  public void perColumnLevelInvalidZstdLevelThrows() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary col_a; }");
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .withCompressionLevel("col_a", 23)
        .build();

    assertThatThrownBy(() -> writeAndReadCodecs(schema, SNAPPY, props))
        .isInstanceOf(BadConfigurationException.class)
        .hasMessageContaining("23");
  }

  @Test
  public void perColumnLevelInvalidGzipLevelThrows() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary col_a; }");
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", GZIP)
        .withCompressionLevel("col_a", 10)
        .build();

    assertThatThrownBy(() -> writeAndReadCodecs(schema, SNAPPY, props))
        .isInstanceOf(BadConfigurationException.class)
        .hasMessageContaining("10");
  }

  private Map<String, CompressionCodecName> writeAndReadCodecs(
      MessageType schema, CompressionCodecName defaultCodec, ParquetProperties props) throws Exception {
    Path file = new Path("target/test/TestColumnChunkPageWriteStore/perColumnCodec.parquet");
    FileSystem fs = file.getFileSystem(conf);
    fs.delete(file, false);
    fs.mkdirs(file.getParent());

    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
    CodecFactory codecFactory = new CodecFactory(conf, pageSize);

    OutputFileForTesting outputFile = new OutputFileForTesting(file, conf);
    ParquetFileWriter writer = new ParquetFileWriter(
        outputFile,
        schema,
        Mode.CREATE,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
        null,
        ParquetProperties.builder().withAllocator(allocator).build());
    writer.start();
    writer.startBlock(1);

    try (ColumnChunkPageWriteStore store = ColumnChunkPageWriteStore.builder()
        .withCompressorProvider(ColumnChunkPageWriteStore.compressorProvider(codecFactory, defaultCodec, props))
        .withSchema(schema)
        .withAllocator(allocator)
        .withColumnIndexTruncateLength(Integer.MAX_VALUE)
        .build()) {
      for (ColumnDescriptor col : schema.getColumns()) {
        Statistics<?> stats =
            Statistics.getBuilderForReading(col.getPrimitiveType()).build();
        store.getPageWriter(col).writePage(BytesInput.fromInt(42), 1, 1, stats, RLE, RLE, PLAIN);
      }
      store.flushToFileWriter(writer);
    }

    writer.endBlock();
    writer.end(new HashMap<>());

    ParquetMetadata footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
    Map<String, CompressionCodecName> result = new HashMap<>();
    for (ColumnChunkMetaData col : footer.getBlocks().get(0).getColumns()) {
      result.put(col.getPath().toDotString(), col.getCodec());
    }
    return result;
  }
}
