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
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.inOrder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.LittleEndianDataInputStream;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

  @Before
  public void initConfiguration() {
    this.conf = new Configuration();
  }

  @After
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
    Path file = createTestFile("test.parquet");
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
      ColumnChunkPageWriteStore.Builder builder = ColumnChunkPageWriteStore.build(
              compressor(GZIP), schema, allocator)
          .withColumnIndexTruncateLength(Integer.MAX_VALUE);
      try (ColumnChunkPageWriteStore store = builder.build()) {
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
      assertEquals(rowCount, page.getRowCount());
      assertEquals(nullCount, page.getNullCount());
      assertEquals(valueCount, page.getValueCount());
      assertEquals(d, intValue(page.getDefinitionLevels()));
      assertEquals(r, intValue(page.getRepetitionLevels()));
      assertEquals(dataEncoding, page.getDataEncoding());
      assertEquals(v, intValue(page.getData()));

      // Checking column/offset indexes for the one page
      ColumnChunkMetaData column = footer.getBlocks().get(0).getColumns().get(0);
      ColumnIndex columnIndex = reader.readColumnIndex(column);
      assertArrayEquals(
          statistics.getMinBytes(), columnIndex.getMinValues().get(0).array());
      assertArrayEquals(
          statistics.getMaxBytes(), columnIndex.getMaxValues().get(0).array());
      assertEquals(
          statistics.getNumNulls(), columnIndex.getNullCounts().get(0).longValue());
      assertFalse(columnIndex.getNullPages().get(0));
      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      assertEquals(1, offsetIndex.getPageCount());
      assertEquals(pageSize, offsetIndex.getCompressedPageSize(0));
      assertEquals(0, offsetIndex.getFirstRowIndex(0));
      assertEquals(pageOffset, offsetIndex.getOffset(0));

      reader.close();
    }
  }

  private int intValue(BytesInput in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    in.writeAllTo(baos);
    LittleEndianDataInputStream os = new LittleEndianDataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    int i = os.readInt();
    os.close();
    return i;
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

    ColumnChunkPageWriteStore.Builder builder = ColumnChunkPageWriteStore.build(
            compressor(UNCOMPRESSED),
            schema,
            TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator()))
        .withColumnIndexTruncateLength(Integer.MAX_VALUE);
    try (ColumnChunkPageWriteStore store = builder.build()) {

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

  @Test
  public void testV2PageCompressThreshold() throws Exception {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 data; }");
    ColumnDescriptor col = schema.getColumns().get(0);

    int valueCount = 100;
    int rowCount = 100;
    int nullCount = 0;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] value =
        ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0).array();
    for (int i = 0; i < valueCount; i++) {
      baos.write(value);
    }
    BytesInput data = BytesInput.from(baos.toByteArray());

    BytesInput definitionLevels = BytesInput.empty();
    BytesInput repetitionLevels = BytesInput.empty();
    Statistics<?> statistics = Statistics.getBuilderForReading(
            Types.required(INT32).named("data"))
        .build();
    statistics.setMinMaxFromBytes(value, value);

    Path file = writeSingleColumnData(
        "lowCompressThreshold",
        schema,
        col,
        rowCount,
        nullCount,
        valueCount,
        repetitionLevels,
        definitionLevels,
        data,
        statistics,
        0.01);
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
    ColumnChunkMetaData columnMeta = footer.getBlocks().get(0).getColumns().get(0);

    long uncompressedSize = columnMeta.getTotalUncompressedSize();
    long compressedSize = columnMeta.getTotalSize();

    assertEquals(
        "Data should be stored uncompressed when compression ratio exceeds threshold",
        uncompressedSize,
        compressedSize);

    file = writeSingleColumnData(
        "highCompressThreshold",
        schema,
        col,
        rowCount,
        nullCount,
        valueCount,
        repetitionLevels,
        definitionLevels,
        data,
        statistics,
        10);
    footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
    columnMeta = footer.getBlocks().get(0).getColumns().get(0);

    uncompressedSize = columnMeta.getTotalUncompressedSize();
    compressedSize = columnMeta.getTotalSize();

    assertTrue(
        "Data should be stored compressed when compression ratio does not exceed threshold",
        uncompressedSize > compressedSize);
  }

  private Path writeSingleColumnData(
      String filePrefix,
      MessageType schema,
      ColumnDescriptor col,
      int rowCount,
      int nullCount,
      int valueCount,
      BytesInput repetitionLevels,
      BytesInput definitionLevels,
      BytesInput data,
      Statistics<?> statistics,
      double v2PageCompressThreshold)
      throws Exception {
    Path file = createTestFile(filePrefix);
    OutputFileForTesting outputFile = new OutputFileForTesting(file, conf);
    ParquetProperties props = ParquetProperties.builder()
        .withAllocator(TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator()))
        .withV2PageCompressThreshold(v2PageCompressThreshold)
        .build();

    ParquetFileWriter writer = new ParquetFileWriter(
        outputFile,
        schema,
        Mode.OVERWRITE,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
        null,
        props);
    writer.start();
    writer.startBlock(rowCount);

    ColumnChunkPageWriteStore.Builder builder = ColumnChunkPageWriteStore.build(
            compressor(GZIP), schema, props.getAllocator())
        .withColumnIndexTruncateLength(Integer.MAX_VALUE)
        .withV2PageCompressThreshold(v2PageCompressThreshold);

    try (ColumnChunkPageWriteStore store = builder.build()) {
      PageWriter pageWriter = store.getPageWriter(col);
      pageWriter.writePageV2(
          rowCount, nullCount, valueCount, repetitionLevels, definitionLevels, PLAIN, data, statistics);
      store.flushToFileWriter(writer);
    }

    writer.endBlock();
    writer.end(new HashMap<>());

    return file;
  }

  private BytesInputCompressor compressor(CompressionCodecName codec) {
    return new CodecFactory(conf, pageSize).getCompressor(codec);
  }

  private Path createTestFile(String prefix) throws Exception {
    return new Path(Files.createTempFile(prefix, ".tmp").toAbsolutePath().toString());
  }
}
