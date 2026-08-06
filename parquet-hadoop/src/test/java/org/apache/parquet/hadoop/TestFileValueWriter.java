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

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that {@link FileValueWriter} routes a {@code FILE} payload to inline storage or to a
 * self-reference according to the configured threshold, and that both forms describe the same logical
 * bytes.
 */
public class TestFileValueWriter {

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message m {"
      + "  optional group file (FILE) {"
      + "    optional int64 offset;"
      + "    optional int64 size;"
      + "    optional binary inline;"
      + "  }"
      + "}");

  private static final ColumnDescriptor INLINE_COLUMN = SCHEMA.getColumnDescription(new String[] {"file", "inline"});

  private static final CompressionCodecName CODEC = CompressionCodecName.SNAPPY;

  private static final Statistics<?> EMPTY_STATS = Statistics.getBuilderForReading(
          Types.required(PrimitiveTypeName.BINARY).named("inline"))
      .build();

  @TempDir
  java.nio.file.Path tempDir;

  @Test
  public void testDefaultThresholdIsPageSize() {
    assertThat(ParquetProperties.builder().build().getFileSelfReferenceThreshold())
        .isEqualTo(ParquetProperties.DEFAULT_PAGE_SIZE);
  }

  @Test
  public void testThresholdMustNotBeNegative() {
    assertThatThrownBy(() -> ParquetProperties.builder().withFileSelfReferenceThreshold(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testPayloadAtThresholdIsInlinedAndAboveIsSelfReference() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(tempDir.resolve("routing.parquet").toUri());
    CodecFactory codecFactory = new CodecFactory(conf, DEFAULT_BLOCK_SIZE);

    int threshold = 64;
    Binary atThreshold = Binary.fromConstantByteArray(payload(threshold));
    Binary aboveThreshold = Binary.fromConstantByteArray(payload(threshold + 1));

    ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, conf), SCHEMA, CREATE, DEFAULT_BLOCK_SIZE, MAX_PADDING_SIZE_DEFAULT);
    writer.start();
    writer.startBlock(2);

    FileValueWriter valueWriter = new FileValueWriter(
        writer, codecFactory.getCompressor(CODEC), null, columnOrdinalOf(INLINE_COLUMN), threshold);

    FileValueWriter.Placement inlined = valueWriter.write(atThreshold);
    FileValueWriter.Placement outOfLine = valueWriter.write(aboveThreshold);

    // A payload exactly at the threshold stays inline; one byte more goes out of line.
    assertThat(inlined.isInline()).isTrue();
    assertThat(inlined.getInlineBytes()).isEqualTo(atThreshold);
    assertThatThrownBy(inlined::getOffset).isInstanceOf(IllegalStateException.class);

    assertThat(outOfLine.isInline()).isFalse();
    assertThat(outOfLine.getSize()).isGreaterThan(0L);
    assertThatThrownBy(outOfLine::getInlineBytes).isInstanceOf(IllegalStateException.class);

    // Write the inline column chunk so the reader has metadata carrying the inherited codec.
    writer.startColumn(INLINE_COLUMN, 1, CODEC);
    writer.writeDataPage(
        1,
        (int) inlined.getInlineBytes().length(),
        codecFactory
            .getCompressor(CODEC)
            .compress(BytesInput.from(inlined.getInlineBytes().toByteBuffer())),
        EMPTY_STATS,
        Encoding.BIT_PACKED,
        Encoding.BIT_PACKED,
        Encoding.PLAIN);
    writer.endColumn();
    writer.endBlock();
    writer.end(new java.util.HashMap<>());

    // The out-of-line payload resolves back to the original bytes.
    InputFile inputFile = HadoopInputFile.fromPath(path, conf);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, ParquetReadOptions.builder().build())) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);
      ColumnChunkMetaData inlineMeta = findColumn(block, INLINE_COLUMN);
      BytesInput resolved = reader.resolveSelfReference(inlineMeta, outOfLine.getOffset(), outOfLine.getSize());
      assertThat(resolved.toByteArray()).isEqualTo(aboveThreshold.getBytes());
    }
    codecFactory.release();
  }

  @Test
  public void testZeroThresholdAlwaysUsesSelfReferences() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(tempDir.resolve("always_out_of_line.parquet").toUri());
    CodecFactory codecFactory = new CodecFactory(conf, DEFAULT_BLOCK_SIZE);

    byte[][] payloads = {payload(1), payload(1000)};

    ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, conf), SCHEMA, CREATE, DEFAULT_BLOCK_SIZE, MAX_PADDING_SIZE_DEFAULT);
    writer.start();
    writer.startBlock(payloads.length);

    FileValueWriter valueWriter =
        new FileValueWriter(writer, codecFactory.getCompressor(CODEC), null, columnOrdinalOf(INLINE_COLUMN), 0);

    List<FileValueWriter.Placement> placements = new ArrayList<>();
    for (byte[] p : payloads) {
      placements.add(valueWriter.write(Binary.fromConstantByteArray(p)));
    }
    // Every payload went out of line, including the single-byte one. An empty payload would still be
    // inlined, since its length is not greater than the threshold.
    assertThat(placements).allMatch(p -> !p.isInline());

    writer.startColumn(INLINE_COLUMN, 0, CODEC);
    writer.writeDataPage(
        0,
        0,
        codecFactory.getCompressor(CODEC).compress(BytesInput.empty()),
        EMPTY_STATS,
        Encoding.BIT_PACKED,
        Encoding.BIT_PACKED,
        Encoding.PLAIN);
    writer.endColumn();
    writer.endBlock();
    writer.end(new java.util.HashMap<>());

    InputFile inputFile = HadoopInputFile.fromPath(path, conf);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, ParquetReadOptions.builder().build())) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);
      ColumnChunkMetaData inlineMeta = findColumn(block, INLINE_COLUMN);
      for (int i = 0; i < payloads.length; i++) {
        FileValueWriter.Placement placement = placements.get(i);
        BytesInput resolved =
            reader.resolveSelfReference(inlineMeta, placement.getOffset(), placement.getSize());
        assertThat(resolved.toByteArray()).isEqualTo(payloads[i]);
      }
    }
    codecFactory.release();
  }

  @Test
  public void testMaxThresholdAlwaysInlines() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(tempDir.resolve("always_inline.parquet").toUri());
    CodecFactory codecFactory = new CodecFactory(conf, DEFAULT_BLOCK_SIZE);

    ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, conf), SCHEMA, CREATE, DEFAULT_BLOCK_SIZE, MAX_PADDING_SIZE_DEFAULT);
    writer.start();
    writer.startBlock(1);

    FileValueWriter valueWriter = new FileValueWriter(
        writer, codecFactory.getCompressor(CODEC), null, columnOrdinalOf(INLINE_COLUMN), Integer.MAX_VALUE);

    long posBefore = writer.getPos();
    FileValueWriter.Placement placement = valueWriter.write(Binary.fromConstantByteArray(payload(1 << 20)));

    assertThat(placement.isInline()).isTrue();
    // Nothing was written to the file body, because the payload is carried by the value itself.
    assertThat(writer.getPos()).isEqualTo(posBefore);

    writer.abort();
    codecFactory.release();
  }

  @Test
  public void testNullPayloadIsRejected() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(tempDir.resolve("null_payload.parquet").toUri());
    CodecFactory codecFactory = new CodecFactory(conf, DEFAULT_BLOCK_SIZE);

    ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, conf), SCHEMA, CREATE, DEFAULT_BLOCK_SIZE, MAX_PADDING_SIZE_DEFAULT);
    writer.start();
    writer.startBlock(1);

    FileValueWriter valueWriter = new FileValueWriter(
        writer, codecFactory.getCompressor(CODEC), null, columnOrdinalOf(INLINE_COLUMN), 64);
    assertThatThrownBy(() -> valueWriter.write(null)).isInstanceOf(IllegalArgumentException.class);

    writer.abort();
    codecFactory.release();
  }

  private static int columnOrdinalOf(ColumnDescriptor column) {
    List<ColumnDescriptor> columns = SCHEMA.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).equals(column)) {
        return i;
      }
    }
    throw new IllegalStateException("Column not found in schema: " + column);
  }

  private static ColumnChunkMetaData findColumn(BlockMetaData block, ColumnDescriptor column) {
    ColumnPath target = ColumnPath.get(column.getPath());
    for (ColumnChunkMetaData meta : block.getColumns()) {
      if (meta.getPath().equals(target)) {
        return meta;
      }
    }
    throw new IllegalStateException("Column chunk not found: " + target);
  }

  private static byte[] payload(int length) {
    StringBuilder sb = new StringBuilder();
    while (sb.length() < length) {
      sb.append("file-payload-");
    }
    return sb.substring(0, length).getBytes(StandardCharsets.UTF_8);
  }
}
