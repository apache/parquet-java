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

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests {@link ParquetFileReader#getRowRanges(int)} and
 * {@link ParquetFileReader#getCompressedBytesForRowRanges(int, RowRanges)}.
 */
public class TestParquetFileReaderRowRanges {

  private static final int ROW_COUNT = 10_000;
  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message test { required int64 id; required int64 grp; }");

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private Path file;

  @Before
  public void writeFile() throws IOException {
    File f = temp.newFile();
    f.delete();
    file = new Path(f.toURI());

    // Small page size produces many pages per column chunk; low-cardinality `grp`
    // ensures dictionary encoding kicks in so we can verify dictionary-page exclusion.
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withType(SCHEMA)
        .withWriteMode(OVERWRITE)
        .withRowGroupSize(64L * 1024 * 1024)
        .withPageSize(4 * 1024)
        .withDictionaryEncoding(true)
        .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
      for (int i = 0; i < ROW_COUNT; i++) {
        writer.write(factory.newGroup().append("id", (long) i).append("grp", (long) (i % 8)));
      }
    }
  }

  private ParquetFileReader openReader() throws IOException {
    Configuration conf = new Configuration();
    ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
    return ParquetFileReader.open(HadoopInputFile.fromPath(file, conf), options);
  }

  @Test
  public void getRowRangesWithoutFilterCoversAllRows() throws IOException {
    try (ParquetFileReader reader = openReader()) {
      assertEquals(1, reader.getRowGroups().size());
      BlockMetaData block = reader.getRowGroups().get(0);

      RowRanges ranges = reader.getRowRanges(0);

      assertEquals(block.getRowCount(), ranges.rowCount());
      assertTrue(ranges.isOverlapping(0L, block.getRowCount() - 1));
    }
  }

  @Test
  public void getCompressedBytesForEmptyRangesIsZero() throws IOException {
    try (ParquetFileReader reader = openReader()) {
      assertEquals(0L, reader.getCompressedBytesForRowRanges(0, RowRanges.EMPTY));
    }
  }

  @Test
  public void getCompressedBytesForFullRangesEqualsOffsetIndexSum() throws IOException {
    try (ParquetFileReader reader = openReader()) {
      BlockMetaData block = reader.getRowGroups().get(0);
      RowRanges full = reader.getRowRanges(0);

      long expected = 0L;
      long columnChunkTotal = 0L;
      for (ColumnChunkMetaData col : block.getColumns()) {
        OffsetIndex oi = reader.readOffsetIndex(col);
        for (int p = 0; p < oi.getPageCount(); p++) {
          expected += oi.getCompressedPageSize(p);
        }
        columnChunkTotal += col.getTotalSize();
      }

      assertEquals(expected, reader.getCompressedBytesForRowRanges(0, full));

      // Dictionary pages aren't represented in OffsetIndex, so the per-page sum
      // must be strictly smaller than the column-chunk totals (which include them).
      assertTrue(
          "expected dictionary-page exclusion: " + expected + " < " + columnChunkTotal,
          expected < columnChunkTotal);
    }
  }

  @Test
  public void getCompressedBytesForPartialRangesIsBetweenZeroAndFull() throws IOException {
    try (ParquetFileReader reader = openReader()) {
      BlockMetaData block = reader.getRowGroups().get(0);
      RowRanges full = reader.getRowRanges(0);
      long fullBytes = reader.getCompressedBytesForRowRanges(0, full);

      // Build a partial RowRanges from the first half of the pages of an arbitrary column;
      // since all columns share row counts, the resulting range applies to every column.
      OffsetIndex anyOi = reader.readOffsetIndex(block.getColumns().get(0));
      int halfPageCount = Math.max(1, anyOi.getPageCount() / 2);
      PrimitiveIterator.OfInt pages = IntStream.range(0, halfPageCount).iterator();
      RowRanges partial = RowRanges.create(block.getRowCount(), pages, anyOi);

      long partialBytes = reader.getCompressedBytesForRowRanges(0, partial);

      assertTrue("partial bytes should be > 0: " + partialBytes, partialBytes > 0);
      assertTrue(
          "partial bytes should be < full bytes: " + partialBytes + " < " + fullBytes,
          partialBytes < fullBytes);
    }
  }
}
