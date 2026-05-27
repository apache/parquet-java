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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end round-trip test for the Approach 2 (micro-row-group) writer path.
 *
 * <p>Writes a small Parquet file via the public {@link ExampleParquetWriter} builder with
 * {@code withMicroRowGroupRowCount(MICRO_SIZE)} set, which routes the flush through
 * {@link InternalParquetRecordWriter}'s Approach 2 path and ultimately through
 * {@link ParquetFileWriter#writeMicroRowGroups}. Then reopens the file and asserts:
 * <ul>
 *   <li>The footer carries {@code K} blocks, each with {@link BlockMetaData#isApproach2()}
 *       returning {@code true} and {@link BlockMetaData#getRowIndexOffset()} equal to the
 *       cumulative row offset.</li>
 *   <li>Each column chunk in each block has
 *       {@code firstDataPageOffset == ColumnChunkMetaData.SENTINEL_OFFSET}.</li>
 *   <li>Each per-block per-column {@link OffsetIndex} reports
 *       {@link OffsetIndex#getFirstRowIndex(int)} values in absolute (file-global) row
 *       coordinates whose union covers the block's window.</li>
 *   <li>Reading records back via the high-level {@link ParquetReader} returns all rows
 *       in order with the expected INT32 values, which exercises the reader's Approach 2
 *       dispatch in {@link ParquetFileReader#readNextRowGroup()}.</li>
 * </ul>
 */
public class TestApproach2WriterReadWrite {

  /** Single-column INT32 schema is enough; multi-column is a follow-up. */
  private static final MessageType SCHEMA = Types.buildMessage()
      .required(PrimitiveTypeName.INT32)
      .named("value")
      .named("test");

  private static final int TOTAL_ROWS = 3500;
  private static final long MICRO_SIZE = 1000L;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testApproach2RoundTrip() throws IOException {
    Path path = newTempPath();
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);

    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    // rowGroupSize is set high so the size-based flush never triggers within TOTAL_ROWS —
    // the flush we exercise is the close()-triggered final flush, which gives us
    // exactly one physical group of TOTAL_ROWS rows split into K micro-blocks.
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withRowGroupSize(1L << 24)
        .withMicroRowGroupRowCount(MICRO_SIZE)
        .build()) {
      for (int i = 0; i < TOTAL_ROWS; i++) {
        writer.write(factory.newGroup().append("value", i));
      }
    }

    // Footer assertions
    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      ParquetMetadata footer = reader.getFooter();
      // ceil(3500 / 1000) = 4 micro-blocks: 1000 / 1000 / 1000 / 500
      assertEquals("expected 4 micro-row-groups", 4, footer.getBlocks().size());

      long[] expectedRowCounts = {1000L, 1000L, 1000L, 500L};
      long expectedAbsRowOffset = 0L;
      for (int b = 0; b < footer.getBlocks().size(); b++) {
        BlockMetaData block = footer.getBlocks().get(b);
        assertEquals("block " + b + " rowCount", expectedRowCounts[b], block.getRowCount());
        assertTrue("block " + b + " isApproach2", block.isApproach2());
        assertEquals(
            "block " + b + " rowIndexOffset",
            expectedAbsRowOffset,
            block.getRowIndexOffset());

        assertEquals("block " + b + " column count", 1, block.getColumns().size());
        ColumnChunkMetaData col = block.getColumns().get(0);
        assertEquals(
            "block " + b + " column firstDataPageOffset is SENTINEL",
            ColumnChunkMetaData.SENTINEL_OFFSET,
            col.getFirstDataPageOffset());
        assertTrue(
            "block " + b + " column isPhysicallyShared",
            col.isPhysicallyShared());

        // Each micro-block's OffsetIndex must list at least one page, and every page
        // listed must start strictly before the block's absolute end (otherwise the page
        // could not contribute any rows to this block). Boundary pages from the previous
        // block may start before this block's absolute start — that is intentional.
        OffsetIndex oi = reader.readOffsetIndex(col);
        assertNotNull("block " + b + " offset index", oi);
        assertTrue("block " + b + " has pages", oi.getPageCount() >= 1);
        long blockAbsEnd = expectedAbsRowOffset + expectedRowCounts[b];
        for (int p = 0; p < oi.getPageCount(); p++) {
          long absFirstRow = oi.getFirstRowIndex(p);
          assertTrue(
              "block " + b + " page " + p + " first_row_index < block end",
              absFirstRow < blockAbsEnd);
        }

        expectedAbsRowOffset += expectedRowCounts[b];
      }
    }

    // Record-level round-trip via the high-level reader: this exercises
    // ParquetFileReader.readNextRowGroup()'s Approach 2 dispatch transparently.
    int seen = 0;
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), path).withConf(conf).build()) {
      Group g;
      while ((g = reader.read()) != null) {
        assertEquals("row " + seen, seen, g.getInteger("value", 0));
        seen++;
      }
    }
    assertEquals("total rows", TOTAL_ROWS, seen);
  }

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }
}
