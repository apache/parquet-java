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

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetFileReaderVectoredIO {
  private static final int ROW_COUNT = 128;
  private static final int OTHER_COLUMN_BASE = 10000;
  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
      "message test { required int32 id; required binary padding (UTF8); required int32 other; }");
  private static final MessageType PROJECTED_SCHEMA = new MessageType(
      SCHEMA.getName(), SCHEMA.getFields().get(0), SCHEMA.getFields().get(2));
  private static final MessageType ID_ONLY_SCHEMA =
      new MessageType(SCHEMA.getName(), SCHEMA.getFields().get(0));

  @TempDir
  private java.nio.file.Path tempDir;

  private Path path;
  private HadoopInputFile inputFile;

  @BeforeEach
  public void writeTestFile() throws IOException {
    path = new Path(tempDir.resolve("vectored.parquet").toUri());
    Configuration configuration = new Configuration();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(configuration)
        .withType(SCHEMA)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withRowGroupSize(256 * 1024)
        .withPageSize(128)
        .withPageRowCountLimit(8)
        .withDictionaryEncoding(false)
        .build()) {
      SimpleGroupFactory groups = new SimpleGroupFactory(SCHEMA);
      for (int row = 0; row < ROW_COUNT; row++) {
        writer.write(groups.newGroup()
            .append("id", row)
            .append("padding", "padding_" + row)
            .append("other", OTHER_COLUMN_BASE + row));
      }
    }
    inputFile = HadoopInputFile.fromPath(path, configuration);
  }

  @Test
  public void testSplitsAdjacentColumnsAtMaximumAllocation() throws Exception {
    List<ColumnChunkMetaData> columns = ParquetFileReader.readFooter(new Configuration(), path)
        .getBlocks()
        .get(0)
        .getColumns();
    int maximumAllocation = 0;
    long totalColumnBytes = 0;
    for (ColumnChunkMetaData column : columns) {
      maximumAllocation = Math.max(maximumAllocation, Math.toIntExact(column.getTotalSize()));
      totalColumnBytes += column.getTotalSize();
    }
    assertThat(totalColumnBytes > maximumAllocation).isTrue();

    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(FailureMode.NONE);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, false), stream)) {
      allocator.reset();
      try (PageReadStore pages = reader.readNextRowGroup()) {
        assertRows(pages, SCHEMA, false);
      }
    }

    assertEquals(1, stream.vectorCalls);
    assertThat(stream.rangeLengths.size() > 1).isTrue();
    for (int rangeLength : stream.rangeLengths) {
      assertThat(rangeLength <= maximumAllocation).isTrue();
    }
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testSplitsSingleOversizedColumnIntoBoundedVectoredRanges() throws Exception {
    int maximumAllocation = 128;
    ColumnChunkMetaData column = ParquetFileReader.readFooter(new Configuration(), path)
        .getBlocks()
        .get(0)
        .getColumns()
        .get(0);
    assertThat(column.getTotalSize() > maximumAllocation).isTrue();

    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(FailureMode.NONE);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, false), stream)) {
      allocator.reset();
      reader.setRequestedSchema(ID_ONLY_SCHEMA);
      try (PageReadStore pages = reader.readNextRowGroup()) {
        assertRows(pages, ID_ONLY_SCHEMA, false);
      }
    }

    assertEquals(1, stream.vectorCalls);
    assertEquals((column.getTotalSize() + maximumAllocation - 1) / maximumAllocation, stream.rangeLengths.size());
    long nextOffset = column.getStartingPos();
    for (int rangeIndex = 0; rangeIndex < stream.rangeLengths.size(); rangeIndex++) {
      assertEquals(nextOffset, stream.rangeOffsets.get(rangeIndex).longValue());
      int rangeLength = stream.rangeLengths.get(rangeIndex);
      assertThat(rangeLength <= maximumAllocation).isTrue();
      nextOffset += rangeLength;
    }
    assertEquals(column.getStartingPos() + column.getTotalSize(), nextOffset);
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testSplitsProductionSizedColumnIntoEightMegabyteVectoredRanges() throws Exception {
    int maximumAllocation = 8 * 1024 * 1024;
    Path largePath = new Path(tempDir.resolve("large-vectored.parquet").toUri());
    Configuration configuration = new Configuration();
    char[] paddingCharacters = new char[136000];
    Arrays.fill(paddingCharacters, 'x');
    String padding = new String(paddingCharacters);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(largePath)
        .withConf(configuration)
        .withType(SCHEMA)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withRowGroupSize(32 * 1024 * 1024)
        .withPageSize(256 * 1024)
        .withPageRowCountLimit(2)
        .withDictionaryEncoding(false)
        .build()) {
      SimpleGroupFactory groups = new SimpleGroupFactory(SCHEMA);
      for (int row = 0; row < ROW_COUNT; row++) {
        writer.write(groups.newGroup()
            .append("id", row)
            .append("padding", padding)
            .append("other", OTHER_COLUMN_BASE + row));
      }
    }

    HadoopInputFile largeInputFile = HadoopInputFile.fromPath(largePath, configuration);
    ColumnChunkMetaData paddingColumn = ParquetFileReader.readFooter(configuration, largePath)
        .getBlocks()
        .get(0)
        .getColumns()
        .get(1);
    assertThat(paddingColumn.getTotalSize() > 2L * maximumAllocation).isTrue();

    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream =
        new RecordingSeekableInputStream(largeInputFile.newStream(), FailureMode.NONE);
    try (ParquetFileReader reader =
        ParquetFileReader.open(largeInputFile, readOptions(allocator, maximumAllocation, false), stream)) {
      allocator.reset();
      stream.resetOrdinaryReads();
      try (PageReadStore pages = reader.readNextRowGroup()) {
        assertRows(pages, SCHEMA, false, ROW_COUNT, padding);
      }
      assertEquals(0, stream.normalSeekCalls);
      assertEquals(0, stream.normalReadCalls);
    }

    assertEquals(1, stream.vectorCalls);
    assertThat(stream.rangeLengths.size() >= 3).isTrue();
    for (int rangeLength : stream.rangeLengths) {
      assertThat(rangeLength <= maximumAllocation).isTrue();
    }
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testFilteredVectoredRangesRespectMaximumAllocation() throws Exception {
    int maximumAllocation = 512;
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(FailureMode.NONE);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, true), stream)) {
      allocator.reset();
      reader.setRequestedSchema(PROJECTED_SCHEMA);
      try (PageReadStore pages = reader.readNextFilteredRowGroup()) {
        assertRows(pages, PROJECTED_SCHEMA, true);
      }
    }

    assertEquals(1, stream.vectorCalls);
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testSplitsOversizedFilteredPageIntoBoundedVectoredRanges() throws Exception {
    int maximumAllocation = 32;
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(FailureMode.NONE);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, true), stream)) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);
      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      boolean hasOversizedPage = false;
      for (int page = 0; page < offsetIndex.getPageCount(); page++) {
        hasOversizedPage |= offsetIndex.getCompressedPageSize(page) > maximumAllocation;
      }
      assertThat(hasOversizedPage).isTrue();

      allocator.reset();
      reader.setRequestedSchema(PROJECTED_SCHEMA);
      try (PageReadStore pages = reader.readNextFilteredRowGroup()) {
        assertRows(pages, PROJECTED_SCHEMA, true);
      }
    }

    assertEquals(1, stream.vectorCalls);
    assertThat(stream.rangeLengths.size() > 2).isTrue();
    for (int rangeLength : stream.rangeLengths) {
      assertThat(rangeLength <= maximumAllocation).isTrue();
    }
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testSplitsAdjacentFilteredPageRangesAtMaximumAllocation() throws Exception {
    int maximumAllocation = 1416;
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(FailureMode.NONE);
    FilterPredicate predicate = or(ltEq(intColumn("id"), 99), gtEq(intColumn("id"), 120));
    ParquetReadOptions options = ParquetReadOptions.builder()
        .withUseHadoopVectoredIo(true)
        .withAllocator(allocator)
        .withMaxAllocationInBytes(maximumAllocation)
        .useColumnIndexFilter(true)
        .withRecordFilter(FilterCompat.get(predicate))
        .build();

    try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options, stream)) {
      allocator.reset();
      try (PageReadStore pages = reader.readNextFilteredRowGroup()) {
        assertRows(pages, SCHEMA, true, 112);
      }
    }

    assertEquals(1, stream.vectorCalls);
    assertThat(stream.rangeLengths.size() > 1).isTrue();
    for (int rangeLength : stream.rangeLengths) {
      assertThat(rangeLength <= maximumAllocation).isTrue();
    }
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testLeavesNonVectoredReadsUnchanged() throws Exception {
    int maximumAllocation = 128;
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(FailureMode.NONE);
    ParquetReadOptions options = ParquetReadOptions.builder()
        .withUseHadoopVectoredIo(false)
        .withAllocator(allocator)
        .withMaxAllocationInBytes(maximumAllocation)
        .build();

    try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options, stream)) {
      allocator.reset();
      stream.resetOrdinaryReads();
      try (PageReadStore pages = reader.readNextRowGroup()) {
        assertRows(pages, SCHEMA, false);
      }
      assertEquals(1, stream.normalSeekCalls);
    }

    assertEquals(0, stream.vectorCalls);
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testUnsupportedBackendPreservesContiguousOrdinaryRead() throws Exception {
    int maximumAllocation = 128;
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(FailureMode.UNSUPPORTED_BACKEND);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, false), stream)) {
      allocator.reset();
      stream.resetOrdinaryReads();
      try (PageReadStore pages = reader.readNextRowGroup()) {
        assertRows(pages, SCHEMA, false);
      }
      assertEquals(1, stream.normalSeekCalls);
    }

    assertEquals(0, stream.vectorCalls);
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testUnsupportedBackendPreservesContiguousFilteredOrdinaryRead() throws Exception {
    int maximumAllocation = 32;
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(FailureMode.UNSUPPORTED_BACKEND);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, true), stream)) {
      reader.setRequestedSchema(ID_ONLY_SCHEMA);
      preloadFilteredIndexes(reader, ID_ONLY_SCHEMA);
      allocator.reset();
      stream.resetOrdinaryReads();
      try (PageReadStore pages = reader.readNextFilteredRowGroup()) {
        assertRows(pages, ID_ONLY_SCHEMA, true);
      }
      // The predicate selects two disjoint page spans; each span should require exactly one seek.
      assertEquals(2, stream.normalSeekCalls);
    }

    assertEquals(0, stream.vectorCalls);
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  @Test
  public void testFailsFastAfterPartiallyConsumingVectoredData() throws Exception {
    assertFailsAfterVectoredSubmission(FailureMode.SECOND_ILLEGAL_ARGUMENT, IllegalArgumentException.class);
    assertFailsAfterVectoredSubmission(FailureMode.SECOND_UNSUPPORTED, UnsupportedOperationException.class);
  }

  @Test
  public void testFailsFastWhenFirstVectoredRangeFails() throws Exception {
    assertFailsAfterVectoredSubmission(FailureMode.FIRST_ILLEGAL_ARGUMENT, IllegalArgumentException.class);
    assertFailsAfterVectoredSubmission(FailureMode.FIRST_UNSUPPORTED, UnsupportedOperationException.class);
  }

  @Test
  public void testFailsFastWhenFirstVectoredRangeFailsWhileSiblingRemainsPending() throws Exception {
    assertFailsAfterVectoredSubmission(
        FailureMode.FIRST_ILLEGAL_ARGUMENT_PENDING_SIBLING, IllegalArgumentException.class);
    assertFailsAfterVectoredSubmission(
        FailureMode.FIRST_UNSUPPORTED_PENDING_SIBLING, UnsupportedOperationException.class);
  }

  @Test
  public void testDrainsPendingVectoredReadsBeforeClosingBackendThatDoesNotCancelThem() throws Exception {
    assertDrainsPendingVectoredReadsBeforeClosing(
        FailureMode.FIRST_IO_EXCEPTION_PENDING_SIBLING_NO_CLOSE_CANCELLATION, IOException.class);
  }

  @Test
  public void testDrainsPendingVectoredReadsAfterSocketTimeout() throws Exception {
    assertDrainsPendingVectoredReadsBeforeClosing(
        FailureMode.FIRST_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION, SocketTimeoutException.class);
  }

  @Test
  public void testContinuesDrainingVectoredReadsAfterSiblingSocketTimeout() throws Exception {
    assertDrainsPendingVectoredReadsBeforeClosing(
        FailureMode.FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION,
        IOException.class);
  }

  private void assertDrainsPendingVectoredReadsBeforeClosing(
      FailureMode failureMode, Class<? extends IOException> failureClass) throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(failureMode);
    int maximumAllocation =
        failureMode == FailureMode.FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION
            ? 128
            : 4096;
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, true), stream)) {
      reader.setRequestedSchema(PROJECTED_SCHEMA);
      preloadFilteredIndexes(reader, PROJECTED_SCHEMA);
      stream.resetOrdinaryReads();

      CompletableFuture<IOException> readAttempt = CompletableFuture.supplyAsync(() -> {
        try {
          reader.readNextFilteredRowGroup();
          throw new AssertionError("Expected the first vectored range to fail");
        } catch (IOException failure) {
          assertThat(Thread.currentThread().isInterrupted())
              .as("A sibling socket timeout must not interrupt the scan thread")
              .isFalse();
          return failure;
        }
      });

      try {
        CompletableFuture.anyOf(stream.pendingDrainStarted, readAttempt).get(10, TimeUnit.SECONDS);
        assertThat(readAttempt.isDone())
            .as("The original failure must wait for unfinished sibling reads")
            .isFalse();
        assertThat(stream.pendingFutureCount() > 0).isTrue();
      } finally {
        stream.allowPendingPhysicalReads.complete(null);
      }

      IOException failure = readAttempt.get(10, TimeUnit.SECONDS);
      assertThat(failureClass.isInstance(failure)).isTrue();
      assertThat(failure.getMessage().contains("injected asynchronous vectored"))
          .isTrue();
      if (failureMode
          == FailureMode.FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION) {
        assertThat(stream.rangeLengths.size() >= 3).isTrue();
        assertEquals(1, failure.getSuppressed().length);
        assertThat(failure.getSuppressed()[0] instanceof SocketTimeoutException)
            .isTrue();
      }
      assertEquals(0, stream.pendingFutureCount());
      assertEquals(0, stream.normalSeekCalls);
      assertEquals(0, stream.normalReadCalls);
    }

    assertEquals(1, stream.vectorCalls);
    assertEquals(0, stream.postClosePhysicalReads.get());
    assertThat(stream.completedPendingPhysicalReads.get() > 0).isTrue();
  }

  @Test
  public void testFailsFastWhenSplitColumnFailsBeforeBuilderIsPopulated() throws Exception {
    assertFailsAfterSplitColumnSubmission(FailureMode.SECOND_ILLEGAL_ARGUMENT, IllegalArgumentException.class);
    assertFailsAfterSplitColumnSubmission(FailureMode.SECOND_UNSUPPORTED, UnsupportedOperationException.class);
  }

  @Test
  public void testFailsFastWhenOversizedColumnRangeFails() throws Exception {
    assertFailsAfterConsumingColumnBeforeSplitRange(
        FailureMode.SECOND_ILLEGAL_ARGUMENT, IllegalArgumentException.class);
    assertFailsAfterConsumingColumnBeforeSplitRange(
        FailureMode.SECOND_UNSUPPORTED, UnsupportedOperationException.class);
  }

  @Test
  public void testFailsFastWhenVectoredSubmissionFails() throws Exception {
    assertFailsAfterVectoredSubmission(FailureMode.SUBMISSION_ILLEGAL_ARGUMENT, IllegalArgumentException.class);
    assertFailsAfterVectoredSubmission(FailureMode.SUBMISSION_UNSUPPORTED, UnsupportedOperationException.class);
  }

  @Test
  public void testFailsFastWhenVectoredSubmissionFailsAfterSchedulingPendingRead() throws Exception {
    assertFailsAfterVectoredSubmission(
        FailureMode.PARTIAL_SUBMISSION_ILLEGAL_ARGUMENT, IllegalArgumentException.class);
    assertFailsAfterVectoredSubmission(
        FailureMode.PARTIAL_SUBMISSION_UNSUPPORTED, UnsupportedOperationException.class);
  }

  private void assertFailsAfterVectoredSubmission(FailureMode failureMode, Class<?> causeType) throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(failureMode);
    if (failureMode.hasPublishedPendingRead()) {
      stream.allowPendingPhysicalReads.complete(null);
    }
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile, readOptions(allocator, 4096, true), stream)) {
      reader.setRequestedSchema(PROJECTED_SCHEMA);
      preloadFilteredIndexes(reader, PROJECTED_SCHEMA);
      stream.resetOrdinaryReads();
      IOException failure = assertThrows(IOException.class, reader::readNextFilteredRowGroup);
      assertThat(failure.getMessage().contains("asynchronous reads may have been submitted"))
          .isTrue();
      assertThat(causeType.isInstance(failure.getCause())).isTrue();
      assertEquals(0, stream.normalSeekCalls);
      assertEquals(0, stream.normalReadCalls);
      if (failureMode.hasPendingRead()) {
        if (failureMode.hasPublishedPendingRead()) {
          assertEquals(0, stream.pendingFutureCount());
        } else {
          assertThat(stream.pendingFutureCount() > 0).isTrue();
        }
      }
    }
    assertEquals(1, stream.vectorCalls);
    assertEquals(0, stream.pendingFutureCount());
  }

  private void assertFailsAfterSplitColumnSubmission(FailureMode failureMode, Class<?> causeType) throws Exception {
    int maximumAllocation = 128;
    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(failureMode);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, false), stream)) {
      allocator.reset();
      reader.setRequestedSchema(ID_ONLY_SCHEMA);
      stream.resetOrdinaryReads();
      IOException failure = assertThrows(IOException.class, reader::readNextRowGroup);
      assertThat(failure.getMessage().contains("asynchronous reads may have been submitted"))
          .isTrue();
      assertThat(causeType.isInstance(failure.getCause())).isTrue();
      assertEquals(0, stream.normalSeekCalls);
      assertEquals(0, stream.normalReadCalls);
    }

    assertEquals(1, stream.vectorCalls);
    assertThat(stream.rangeLengths.size() > 1).isTrue();
    assertThat(allocator.maximumAllocation <= maximumAllocation).isTrue();
  }

  private void assertFailsAfterConsumingColumnBeforeSplitRange(FailureMode failureMode, Class<?> causeType)
      throws Exception {
    List<ColumnChunkMetaData> columns = ParquetFileReader.readFooter(new Configuration(), path)
        .getBlocks()
        .get(0)
        .getColumns();
    int maximumAllocation = Math.toIntExact(columns.get(0).getTotalSize());
    assertThat(columns.get(1).getTotalSize() > maximumAllocation).isTrue();

    RecordingAllocator allocator = new RecordingAllocator();
    RecordingSeekableInputStream stream = newStream(failureMode);
    try (ParquetFileReader reader =
        ParquetFileReader.open(inputFile, readOptions(allocator, maximumAllocation, false), stream)) {
      stream.resetOrdinaryReads();
      IOException failure = assertThrows(IOException.class, reader::readNextRowGroup);
      assertThat(failure.getMessage().contains("asynchronous reads may have been submitted"))
          .isTrue();
      assertThat(causeType.isInstance(failure.getCause())).isTrue();
      assertEquals(0, stream.normalSeekCalls);
      assertEquals(0, stream.normalReadCalls);
    }
    assertEquals(1, stream.vectorCalls);
  }

  private static void preloadFilteredIndexes(ParquetFileReader reader, MessageType projection) {
    reader.getFilteredRecordCount();
    for (ColumnChunkMetaData column : reader.getFooter().getBlocks().get(0).getColumns()) {
      if (projection.containsField(column.getPath().toDotString())) {
        reader.getColumnIndexStore(0).getOffsetIndex(column.getPath());
      }
    }
  }

  private RecordingSeekableInputStream newStream(FailureMode failureMode) throws IOException {
    return new RecordingSeekableInputStream(inputFile.newStream(), failureMode);
  }

  private static ParquetReadOptions readOptions(
      RecordingAllocator allocator, int maximumAllocation, boolean filterPages) {
    ParquetReadOptions.Builder builder = ParquetReadOptions.builder()
        .withUseHadoopVectoredIo(true)
        .withAllocator(allocator)
        .withMaxAllocationInBytes(maximumAllocation);
    if (filterPages) {
      FilterPredicate predicate =
          or(ltEq(intColumn("id"), 99), and(gtEq(intColumn("id"), 108), ltEq(intColumn("id"), 115)));
      builder.useColumnIndexFilter(true).withRecordFilter(FilterCompat.get(predicate));
    }
    return builder.build();
  }

  private static void assertRows(PageReadStore pages, MessageType projection, boolean filtered) {
    assertRows(pages, projection, filtered, filtered ? 108 : ROW_COUNT);
  }

  private static void assertRows(PageReadStore pages, MessageType projection, boolean filtered, long expectedRows) {
    assertRows(pages, projection, filtered, expectedRows, null);
  }

  private static void assertRows(
      PageReadStore pages, MessageType projection, boolean filtered, long expectedRows, String expectedPadding) {
    assertEquals(expectedRows, pages.getRowCount());

    PrimitiveIterator.OfLong rowIndexes = filtered ? pages.getRowIndexes().get() : null;
    MessageColumnIO columns = new ColumnIOFactory().getColumnIO(projection, SCHEMA);
    RecordReader<Group> records = columns.getRecordReader(pages, new GroupRecordConverter(projection));
    for (long row = 0; row < expectedRows; row++) {
      long expectedIndex = filtered ? rowIndexes.nextLong() : row;
      Group record = records.read();
      assertEquals(expectedIndex, record.getInteger("id", 0));
      if (projection.containsField("padding")) {
        assertEquals(
            expectedPadding == null ? "padding_" + expectedIndex : expectedPadding,
            record.getString("padding", 0));
      }
      if (projection.containsField("other")) {
        assertEquals(OTHER_COLUMN_BASE + expectedIndex, record.getInteger("other", 0));
      }
    }
  }

  private enum FailureMode {
    NONE,
    UNSUPPORTED_BACKEND,
    FIRST_ILLEGAL_ARGUMENT,
    FIRST_UNSUPPORTED,
    FIRST_ILLEGAL_ARGUMENT_PENDING_SIBLING,
    FIRST_UNSUPPORTED_PENDING_SIBLING,
    FIRST_IO_EXCEPTION_PENDING_SIBLING_NO_CLOSE_CANCELLATION,
    FIRST_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION,
    FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION,
    SECOND_ILLEGAL_ARGUMENT,
    SECOND_UNSUPPORTED,
    SUBMISSION_ILLEGAL_ARGUMENT,
    SUBMISSION_UNSUPPORTED,
    PARTIAL_SUBMISSION_ILLEGAL_ARGUMENT,
    PARTIAL_SUBMISSION_UNSUPPORTED;

    private boolean hasPendingRead() {
      return hasPublishedPendingRead()
          || this == PARTIAL_SUBMISSION_ILLEGAL_ARGUMENT
          || this == PARTIAL_SUBMISSION_UNSUPPORTED;
    }

    private boolean hasPublishedPendingRead() {
      return this == FIRST_ILLEGAL_ARGUMENT_PENDING_SIBLING
          || this == FIRST_UNSUPPORTED_PENDING_SIBLING
          || this == FIRST_IO_EXCEPTION_PENDING_SIBLING_NO_CLOSE_CANCELLATION
          || this == FIRST_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION
          || this == FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION;
    }
  }

  private static final class RecordingAllocator extends HeapByteBufferAllocator {
    private int maximumAllocation;

    @Override
    public ByteBuffer allocate(int size) {
      maximumAllocation = Math.max(maximumAllocation, size);
      return super.allocate(size);
    }

    private void reset() {
      maximumAllocation = 0;
    }
  }

  private static final class RecordingSeekableInputStream extends DelegatingSeekableInputStream {
    private final SeekableInputStream delegate;
    private final FailureMode failureMode;
    private final List<Long> rangeOffsets = new ArrayList<>();
    private final List<Integer> rangeLengths = new ArrayList<>();
    private final List<CompletableFuture<ByteBuffer>> pendingFutures = new ArrayList<>();
    private final CompletableFuture<Void> allowPendingPhysicalReads = new CompletableFuture<>();
    private final CompletableFuture<Void> pendingDrainStarted = new CompletableFuture<>();
    private final CompletableFuture<Void> socketTimeoutDrainStarted = new CompletableFuture<>();
    private final AtomicInteger completedPendingPhysicalReads = new AtomicInteger();
    private final AtomicInteger postClosePhysicalReads = new AtomicInteger();
    private volatile boolean closed;
    private int vectorCalls;
    private int normalSeekCalls;
    private int normalReadCalls;

    private RecordingSeekableInputStream(SeekableInputStream delegate, FailureMode failureMode) {
      super(delegate);
      this.delegate = delegate;
      this.failureMode = failureMode;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      normalSeekCalls++;
      delegate.seek(newPos);
    }

    @Override
    public void readFully(ByteBuffer buffer) throws IOException {
      normalReadCalls++;
      delegate.readFully(buffer);
    }

    @Override
    public boolean readVectoredAvailable(ByteBufferAllocator allocator) {
      return failureMode != FailureMode.UNSUPPORTED_BACKEND;
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
      vectorCalls++;
      if (failureMode == FailureMode.SUBMISSION_ILLEGAL_ARGUMENT) {
        throw new IllegalArgumentException("injected vectored submission failure");
      }
      if (failureMode == FailureMode.SUBMISSION_UNSUPPORTED) {
        throw new UnsupportedOperationException("injected vectored submission failure");
      }
      if (failureMode == FailureMode.PARTIAL_SUBMISSION_ILLEGAL_ARGUMENT
          || failureMode == FailureMode.PARTIAL_SUBMISSION_UNSUPPORTED) {
        // Hadoop's bridge does not publish backend futures until submission returns successfully.
        pendingFutures.add(new CompletableFuture<>());
        throw failure();
      }

      long originalPosition = delegate.getPos();
      try {
        for (int index = 0; index < ranges.size(); index++) {
          ParquetFileRange range = ranges.get(index);
          rangeOffsets.add(range.getOffset());
          rangeLengths.add(range.getLength());
          boolean socketTimeoutSibling = index == 1
              && failureMode
                  == FailureMode
                      .FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION;
          CompletableFuture<ByteBuffer> future = hasPendingSibling(index)
              ? new PendingPhysicalReadFuture(
                  socketTimeoutSibling ? socketTimeoutDrainStarted : pendingDrainStarted,
                  socketTimeoutSibling
                      ? new SocketTimeoutException("injected sibling socket timeout")
                      : null)
              : new CompletableFuture<>();
          if (shouldFail(index)) {
            if (failureMode == FailureMode.FIRST_IO_EXCEPTION_PENDING_SIBLING_NO_CLOSE_CANCELLATION
                || failureMode
                    == FailureMode
                        .FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION) {
              future.completeExceptionally(new IOException("injected asynchronous vectored IO failure"));
            } else if (failureMode
                == FailureMode.FIRST_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION) {
              future.completeExceptionally(
                  new SocketTimeoutException("injected asynchronous vectored socket timeout"));
            } else {
              future.completeExceptionally(failure());
            }
          } else if (hasPendingSibling(index)) {
            pendingFutures.add(future);
            if (!socketTimeoutSibling) {
              ByteBuffer buffer = allocator.allocate(range.getLength());
              CompletableFuture.runAsync(() -> {
                allowPendingPhysicalReads.join();
                if (closed) {
                  postClosePhysicalReads.incrementAndGet();
                }
                completedPendingPhysicalReads.incrementAndGet();
                future.complete(buffer);
              });
            }
          } else {
            ByteBuffer buffer = allocator.allocate(range.getLength());
            delegate.seek(range.getOffset());
            delegate.readFully(buffer);
            buffer.flip();
            future.complete(buffer);
          }
          range.setDataReadFuture(future);
        }
      } finally {
        delegate.seek(originalPosition);
      }
    }

    private boolean shouldFail(int index) {
      return index == 0
              && (failureMode == FailureMode.FIRST_ILLEGAL_ARGUMENT
                  || failureMode == FailureMode.FIRST_UNSUPPORTED
                  || failureMode == FailureMode.FIRST_ILLEGAL_ARGUMENT_PENDING_SIBLING
                  || failureMode == FailureMode.FIRST_UNSUPPORTED_PENDING_SIBLING
                  || failureMode
                      == FailureMode.FIRST_IO_EXCEPTION_PENDING_SIBLING_NO_CLOSE_CANCELLATION
                  || failureMode
                      == FailureMode.FIRST_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION
                  || failureMode
                      == FailureMode
                          .FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION)
          || index == 1
              && (failureMode == FailureMode.SECOND_ILLEGAL_ARGUMENT
                  || failureMode == FailureMode.SECOND_UNSUPPORTED);
    }

    private boolean hasPendingSibling(int index) {
      return index > 0
          && (failureMode == FailureMode.FIRST_ILLEGAL_ARGUMENT_PENDING_SIBLING
              || failureMode == FailureMode.FIRST_UNSUPPORTED_PENDING_SIBLING
              || failureMode == FailureMode.FIRST_IO_EXCEPTION_PENDING_SIBLING_NO_CLOSE_CANCELLATION
              || failureMode == FailureMode.FIRST_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION
              || failureMode
                  == FailureMode
                      .FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION);
    }

    private RuntimeException failure() {
      if (failureMode == FailureMode.FIRST_UNSUPPORTED
          || failureMode == FailureMode.FIRST_UNSUPPORTED_PENDING_SIBLING
          || failureMode == FailureMode.SECOND_UNSUPPORTED
          || failureMode == FailureMode.PARTIAL_SUBMISSION_UNSUPPORTED) {
        return new UnsupportedOperationException("injected asynchronous vectored failure");
      }
      return new IllegalArgumentException("injected asynchronous vectored failure");
    }

    private void resetOrdinaryReads() {
      normalSeekCalls = 0;
      normalReadCalls = 0;
    }

    private int pendingFutureCount() {
      int count = 0;
      for (CompletableFuture<ByteBuffer> pendingFuture : pendingFutures) {
        if (!pendingFuture.isDone()) {
          count++;
        }
      }
      return count;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      if (failureMode != FailureMode.FIRST_IO_EXCEPTION_PENDING_SIBLING_NO_CLOSE_CANCELLATION
          && failureMode != FailureMode.FIRST_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION
          && failureMode
              != FailureMode
                  .FIRST_IO_EXCEPTION_THEN_SOCKET_TIMEOUT_PENDING_SIBLING_NO_CLOSE_CANCELLATION) {
        for (CompletableFuture<ByteBuffer> pendingFuture : pendingFutures) {
          pendingFuture.cancel(false);
        }
      }
      super.close();
    }
  }

  private static final class PendingPhysicalReadFuture extends CompletableFuture<ByteBuffer> {
    private final CompletableFuture<Void> drainStarted;
    private final IOException failure;

    private PendingPhysicalReadFuture(CompletableFuture<Void> drainStarted, IOException failure) {
      this.drainStarted = drainStarted;
      this.failure = failure;
    }

    @Override
    public ByteBuffer get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      drainStarted.complete(null);
      if (failure != null) {
        completeExceptionally(failure);
      }
      return super.get(timeout, unit);
    }
  }
}
