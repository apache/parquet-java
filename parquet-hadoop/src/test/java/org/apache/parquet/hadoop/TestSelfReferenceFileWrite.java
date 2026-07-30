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

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test that writes normal column data <i>and</i> FILE self-reference payloads into the
 * same Parquet file with {@link ParquetFileWriter}, then reopens it and both reads the data pages
 * back and resolves the self-references via {@link ParquetFileReader#resolveSelfReference}. This
 * exercises the interaction between the storage-inheritance APIs and the ordinary file-write path:
 * self-reference payloads are written into the file body while a block is open, and the
 * {@code offset}/{@code size} they return would be recorded in the {@code offset} and {@code size}
 * columns of a FILE group.
 */
public class TestSelfReferenceFileWrite {

  // A FILE group whose values are self-references: the inline column supplies the codec/encryption
  // reference point, and offset/size locate the stored payload within this file.
  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message m {"
      + "  required int64 id;"
      + "  optional group file (FILE) {"
      + "    optional int64 offset;"
      + "    optional int64 size;"
      + "    optional binary inline;"
      + "  }"
      + "}");

  private static final ColumnDescriptor ID_COLUMN = SCHEMA.getColumnDescription(new String[] {"id"});
  // The inline column is the storage-inheritance reference point for the FILE group.
  private static final ColumnDescriptor INLINE_COLUMN =
      SCHEMA.getColumnDescription(new String[] {"file", "inline"});

  private static final CompressionCodecName CODEC = CompressionCodecName.SNAPPY;

  private static final Statistics<?> EMPTY_STATS = Statistics.getBuilderForReading(
          Types.required(PrimitiveTypeName.INT64).named("id"))
      .build();

  @TempDir
  java.nio.file.Path tempDir;

  @Test
  public void testWriteDataAlongsideSelfReferences() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(tempDir.resolve("self_ref.parquet").toUri());

    // Payloads that will be stored as self-references, inheriting the SNAPPY codec of the inline
    // column. Made highly compressible so the stored size differs from the resolved size.
    byte[][] payloads = {
      repeat("hello self-reference ", 200),
      repeat("second blob ", 400),
      new byte[0], // empty payload is a valid self-reference
    };

    byte[] idPageBytes = {0, 1, 2, 3, 4, 5, 6, 7};

    CodecFactory codecFactory = new CodecFactory(conf, DEFAULT_BLOCK_SIZE);
    List<SelfReferenceStorage.StoredRange> ranges = new ArrayList<>();

    ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, conf), SCHEMA, CREATE, DEFAULT_BLOCK_SIZE, MAX_PADDING_SIZE_DEFAULT);

    writer.start();
    writer.startBlock(payloads.length);

    // Self-references are written into the file body while the block is open. In a real writer
    // these would be interleaved with the column data; the returned offset/size feed the FILE
    // group's offset/size columns.
    int inlineColumnOrdinal = columnOrdinalOf(INLINE_COLUMN);
    for (int i = 0; i < payloads.length; i++) {
      ranges.add(writer.writeSelfReference(
          BytesInput.from(payloads[i]),
          codecFactory.getCompressor(CODEC),
          null, // unencrypted file
          inlineColumnOrdinal,
          i));
    }

    // Write a normal data page for the id column in the same block.
    writer.startColumn(ID_COLUMN, 4, CompressionCodecName.UNCOMPRESSED);
    writer.writeDataPage(4, idPageBytes.length, BytesInput.from(idPageBytes), EMPTY_STATS, PLAIN, PLAIN, PLAIN);
    writer.endColumn();

    // Write the inline column chunk so the reader has a ColumnChunkMetaData carrying the SNAPPY
    // codec that the self-references inherit. (The inline values themselves are empty here because
    // the payload lives in the self-reference blocks.)
    writer.startColumn(INLINE_COLUMN, 0, CODEC);
    BytesInput emptyInline = codecFactory.getCompressor(CODEC).compress(BytesInput.empty());
    writer.writeDataPage(0, 0, emptyInline, EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    writer.endColumn();

    writer.endBlock();
    writer.end(new java.util.HashMap<>());

    // The stored ranges are non-overlapping and ordered as written.
    assertThat(ranges.get(0).getOffset()).isLessThan(ranges.get(1).getOffset());
    assertThat(ranges.get(0).getOffset() + ranges.get(0).getSize())
        .isLessThanOrEqualTo(ranges.get(1).getOffset());

    // Reopen and verify both the data page and the self-references coexist and resolve correctly.
    InputFile inputFile = HadoopInputFile.fromPath(path, conf);
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
      ParquetMetadata footer = reader.getFooter();
      assertThat(footer.getBlocks()).hasSize(1);
      BlockMetaData block = footer.getBlocks().get(0);

      // The normal id column reads back exactly as written.
      ColumnChunkMetaData inlineMeta = findColumn(block, INLINE_COLUMN);
      assertThat(inlineMeta.getCodec()).isEqualTo(CODEC);

      try (ParquetFileReader dataReader = ParquetFileReader.open(inputFile, options)) {
        PageReadStore pages = dataReader.readNextRowGroup();
        PageReader idPages = pages.getPageReader(ID_COLUMN);
        DataPage idPage = idPages.readPage();
        assertThat(((DataPageV1) idPage).getBytes().toByteArray()).isEqualTo(idPageBytes);
      }

      // Each self-reference resolves back to its original payload, inheriting the inline column's
      // codec.
      for (int i = 0; i < payloads.length; i++) {
        SelfReferenceStorage.StoredRange range = ranges.get(i);
        BytesInput resolved =
            reader.resolveSelfReference(inlineMeta, range.getOffset(), range.getSize(), i);
        assertThat(resolved.toByteArray()).isEqualTo(payloads[i]);
      }
    }

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
    org.apache.parquet.hadoop.metadata.ColumnPath target =
        org.apache.parquet.hadoop.metadata.ColumnPath.get(column.getPath());
    for (ColumnChunkMetaData meta : block.getColumns()) {
      if (meta.getPath().equals(target)) {
        return meta;
      }
    }
    throw new IllegalStateException("Column chunk not found: " + target);
  }

  private static byte[] repeat(String token, int times) {
    StringBuilder sb = new StringBuilder(token.length() * times);
    for (int i = 0; i < times; i++) {
      sb.append(token);
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }
}
