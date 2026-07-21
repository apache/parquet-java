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
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for GH-3487: Verify that PageReadStore buffers are properly released when
 * reading multiple row groups sequentially. Before the fix, calling readNextRowGroup()
 * or readNextFilteredRowGroup() multiple times would leak the ByteBuffers of previous
 * row groups because the old currentRowGroup was replaced without being closed first.
 * <p>
 * These tests use {@link TrackingByteBufferAllocator} which throws
 * {@link TrackingByteBufferAllocator.LeakedByteBufferException} if any allocated
 * ByteBuffers remain unreleased when the allocator is closed.
 */
public class TestParquetFileReaderBufferLeak {

  private static final MessageType SCHEMA = Types.buildMessage()
      .required(INT64)
      .named("id")
      .required(INT64)
      .named("value")
      .named("msg");

  private static final Configuration CONF = new Configuration();

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  /**
   * Helper: write a parquet file with multiple row groups using the high-level writer API.
   * Uses a small page size and row group size to ensure multiple row groups are created.
   * Returns the path and the number of row groups written.
   */
  private Path writeMultiRowGroupFile(int numRecords) throws IOException {
    GroupWriteSupport.setSchema(SCHEMA, CONF);

    File testFile = temp.newFile();
    testFile.delete();
    Path path = new Path(testFile.toURI());

    try (TrackingByteBufferAllocator writeAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(path, CONF))
          .withAllocator(writeAllocator)
          .withConf(CONF)
          .withType(SCHEMA)
          .withWriteMode(OVERWRITE)
          .withRowGroupSize(200) // Small row group size to force multiple row groups
          .withPageSize(100) // Small page size
          .build()) {
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
        for (int i = 0; i < numRecords; i++) {
          writer.write(factory.newGroup().append("id", (long) i).append("value", (long) (i * 10)));
        }
      }
    }

    return path;
  }

  /**
   * Helper to verify a file has the expected number of row groups.
   */
  private int getRowGroupCount(Path path) throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(path, CONF);
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      return reader.getFooter().getBlocks().size();
    }
  }

  /**
   * Verify that reading all row groups via readNextRowGroup() does not leak buffers.
   * Before the fix, each call to readNextRowGroup() would replace currentRowGroup
   * without closing the previous one, leaking its ByteBuffers.
   */
  @Test
  public void testReadNextRowGroupReleasesBuffersOfPreviousRowGroup() throws Exception {
    Path path = writeMultiRowGroupFile(500);
    int expectedRowGroups = getRowGroupCount(path);
    assertTrue("Expected multiple row groups but got " + expectedRowGroups, expectedRowGroups > 1);

    try (TrackingByteBufferAllocator readAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      ParquetReadOptions options =
          ParquetReadOptions.builder().withAllocator(readAllocator).build();
      InputFile inputFile = HadoopInputFile.fromPath(path, CONF);

      try (ParquetFileReader reader = new ParquetFileReader(inputFile, options)) {
        PageReadStore pages;
        int rowGroupCount = 0;
        while ((pages = reader.readNextRowGroup()) != null) {
          assertNotNull(pages);
          rowGroupCount++;
        }
        assertEquals(expectedRowGroups, rowGroupCount);
      }
      // After reader.close(), all buffers including the last row group's should be released.
      // readAllocator.close() will throw LeakedByteBufferException if any leak remains.
    }
  }

  /**
   * Verify that close() releases the buffers of the last row group read via readNextRowGroup().
   * This tests the case where only some row groups are read before the reader is closed.
   */
  @Test
  public void testCloseReleasesCurrentRowGroupBuffers() throws Exception {
    Path path = writeMultiRowGroupFile(500);
    int expectedRowGroups = getRowGroupCount(path);
    assertTrue("Expected multiple row groups but got " + expectedRowGroups, expectedRowGroups > 1);

    try (TrackingByteBufferAllocator readAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      ParquetReadOptions options =
          ParquetReadOptions.builder().withAllocator(readAllocator).build();
      InputFile inputFile = HadoopInputFile.fromPath(path, CONF);

      try (ParquetFileReader reader = new ParquetFileReader(inputFile, options)) {
        // Read only the first row group
        PageReadStore pages = reader.readNextRowGroup();
        assertNotNull(pages);
        assertTrue(pages.getRowCount() > 0);
        // Don't read remaining row groups - close should release the current row group's buffers
      }
    }
  }

  /**
   * Verify that readNextFilteredRowGroup() releases buffers of the previous row group
   * when the filter does not trigger column-index filtering (falls back to readNextRowGroup).
   */
  @Test
  public void testReadNextFilteredRowGroupReleasesBuffersWhenNoFilter() throws Exception {
    Path path = writeMultiRowGroupFile(500);
    int expectedRowGroups = getRowGroupCount(path);
    assertTrue("Expected multiple row groups but got " + expectedRowGroups, expectedRowGroups > 1);

    try (TrackingByteBufferAllocator readAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      // No record filter configured -> readNextFilteredRowGroup falls back to readNextRowGroup
      ParquetReadOptions options =
          ParquetReadOptions.builder().withAllocator(readAllocator).build();
      InputFile inputFile = HadoopInputFile.fromPath(path, CONF);

      try (ParquetFileReader reader = new ParquetFileReader(inputFile, options)) {
        PageReadStore pages;
        int rowGroupCount = 0;
        while ((pages = reader.readNextFilteredRowGroup()) != null) {
          assertNotNull(pages);
          rowGroupCount++;
        }
        assertEquals(expectedRowGroups, rowGroupCount);
      }
    }
  }

  /**
   * Verify that readNextFilteredRowGroup() with a filter and column-index filtering enabled
   * still properly releases previous row group buffers. The filter is set to match all rows,
   * so it falls back to readNextRowGroup via the all-rows-match path.
   */
  @Test
  public void testReadNextFilteredRowGroupReleasesBuffersWithFilter() throws Exception {
    Path path = writeMultiRowGroupFile(500);
    int expectedRowGroups = getRowGroupCount(path);
    assertTrue("Expected multiple row groups but got " + expectedRowGroups, expectedRowGroups > 1);

    try (TrackingByteBufferAllocator readAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      // Use a filter that matches all rows: id > -1 (all values are >= 0)
      FilterCompat.Filter filter = FilterCompat.get(FilterApi.gt(FilterApi.longColumn("id"), -1L));
      ParquetReadOptions options = ParquetReadOptions.builder()
          .withAllocator(readAllocator)
          .withRecordFilter(filter)
          .useColumnIndexFilter(true)
          .useStatsFilter(true)
          .build();
      InputFile inputFile = HadoopInputFile.fromPath(path, CONF);

      try (ParquetFileReader reader = new ParquetFileReader(inputFile, options)) {
        PageReadStore pages;
        int rowGroupCount = 0;
        while ((pages = reader.readNextFilteredRowGroup()) != null) {
          assertNotNull(pages);
          rowGroupCount++;
        }
        // All row groups should be read since the filter matches everything
        assertEquals(expectedRowGroups, rowGroupCount);
      }
    }
  }

  /**
   * Verify that readNextFilteredRowGroup() with a filter that eliminates some rows
   * through column-index filtering releases buffers properly. This exercises the
   * internalReadFilteredRowGroup code path where rowCount != block.getRowCount().
   */
  @Test
  public void testReadNextFilteredRowGroupWithColumnIndexFilteringReleasesBuffers() throws Exception {
    Path path = writeMultiRowGroupFile(500);
    int totalRowGroups = getRowGroupCount(path);
    assertTrue("Expected multiple row groups but got " + totalRowGroups, totalRowGroups > 1);

    try (TrackingByteBufferAllocator readAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      // Use a filter that eliminates some rows based on statistics/column index:
      // id < 10 should only match early row groups and exclude pages with higher values
      FilterCompat.Filter filter = FilterCompat.get(FilterApi.lt(FilterApi.longColumn("id"), 10L));
      ParquetReadOptions options = ParquetReadOptions.builder()
          .withAllocator(readAllocator)
          .withRecordFilter(filter)
          .useColumnIndexFilter(true)
          .useStatsFilter(true)
          .build();
      InputFile inputFile = HadoopInputFile.fromPath(path, CONF);

      try (ParquetFileReader reader = new ParquetFileReader(inputFile, options)) {
        PageReadStore pages;
        int rowGroupCount = 0;
        while ((pages = reader.readNextFilteredRowGroup()) != null) {
          assertNotNull(pages);
          assertTrue(pages.getRowCount() > 0);
          rowGroupCount++;
        }
        // At least some row groups should be returned (those containing id < 10)
        assertTrue("Expected at least 1 row group returned", rowGroupCount >= 1);
      }
    }
  }

  /**
   * Verify that buffers are released even when only some row groups are read
   * and the reader is closed without reading to the end.
   */
  @Test
  public void testPartialReadThenCloseReleasesBuffers() throws Exception {
    Path path = writeMultiRowGroupFile(500);
    int expectedRowGroups = getRowGroupCount(path);
    assertTrue("Expected at least 3 row groups but got " + expectedRowGroups, expectedRowGroups >= 3);

    try (TrackingByteBufferAllocator readAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      ParquetReadOptions options =
          ParquetReadOptions.builder().withAllocator(readAllocator).build();
      InputFile inputFile = HadoopInputFile.fromPath(path, CONF);

      try (ParquetFileReader reader = new ParquetFileReader(inputFile, options)) {
        // Read row group 1
        PageReadStore pages = reader.readNextRowGroup();
        assertNotNull(pages);

        // Read row group 2 (should release row group 1 buffers)
        pages = reader.readNextRowGroup();
        assertNotNull(pages);

        // Close without reading remaining row groups - close() should release row group 2 buffers
      }
    }
  }

  /**
   * Verify that calling readNextRowGroup after all groups are exhausted doesn't cause issues.
   * The null return when there are no more row groups should not leak any buffers.
   */
  @Test
  public void testExhaustedReaderNoLeak() throws Exception {
    Path path = writeMultiRowGroupFile(500);

    try (TrackingByteBufferAllocator readAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      ParquetReadOptions options =
          ParquetReadOptions.builder().withAllocator(readAllocator).build();
      InputFile inputFile = HadoopInputFile.fromPath(path, CONF);

      try (ParquetFileReader reader = new ParquetFileReader(inputFile, options)) {
        // Read all row groups
        while (reader.readNextRowGroup() != null) {
          // drain
        }
        // Additional calls should return null without leaking
        assertNull(reader.readNextRowGroup());
        assertNull(reader.readNextRowGroup());
      }
    }
  }
}
