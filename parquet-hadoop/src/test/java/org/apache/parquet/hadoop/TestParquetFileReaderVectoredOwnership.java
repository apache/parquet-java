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

import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParquetFileReaderVectoredOwnership {
  private static final int ROWS = 128;
  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message test { required int32 id; required binary padding (UTF8); }");

  @TempDir
  private java.nio.file.Path tempDir;

  private HadoopInputFile inputFile;
  private ParquetMetadata footer;

  @BeforeEach
  void writeFile() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(tempDir.resolve("ownership.parquet").toUri());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withType(SCHEMA)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withRowGroupSize(256 * 1024)
        .withPageSize(128)
        .withPageRowCountLimit(8)
        .withDictionaryEncoding(false)
        .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
      for (int row = 0; row < ROWS; row++) {
        writer.write(factory.newGroup().append("id", row).append("padding", "padding_" + row));
      }
    }
    inputFile = HadoopInputFile.fromPath(path, conf);
    footer = ParquetFileReader.readFooter(conf, path);
  }

  @Test
  void testReleasesOriginalVectoredBuffersWhenRowGroupCloses() throws Exception {
    assertSuccessfulReadReleasesBuffers(ReadMode.ORIGINALS);
  }

  @Test
  void testReleasesMergedOriginalAndChecksumBufferWhenResultsAreSlices() throws Exception {
    assertSuccessfulReadReleasesBuffers(ReadMode.SLICES);
  }

  @Test
  void testReleasesSlicedBuffersForFilteredRowGroup() throws Exception {
    CountingAllocator delegate = new CountingAllocator();
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate)) {
      ParquetReadOptions readOptions = ParquetReadOptions.builder()
          .withAllocator(tracking)
          .withUseHadoopVectoredIo(true)
          .withMaxAllocationInBytes(128)
          .useColumnIndexFilter(true)
          .withRecordFilter(FilterCompat.get(ltEq(intColumn("id"), 63)))
          .build();
      try (ParquetFileReader reader = ParquetFileReader.open(
          inputFile, footer, readOptions, new OwnedStream(inputFile.newStream(), ReadMode.SLICES))) {
        try (PageReadStore pages = reader.readFilteredRowGroup(0)) {
          assertTrue(pages.getRowCount() > 0 && pages.getRowCount() < ROWS);
          assertTrue(pages.getRowIndexes().isPresent());
          PrimitiveIterator.OfLong indexes = pages.getRowIndexes().get();
          RecordReader<Group> records = new ColumnIOFactory()
              .getColumnIO(SCHEMA)
              .getRecordReader(pages, new GroupRecordConverter(SCHEMA));
          for (long row = 0; row < pages.getRowCount(); row++) {
            long expectedIndex = indexes.nextLong();
            Group record = records.read();
            assertEquals(expectedIndex, record.getInteger("id", 0));
            assertEquals("padding_" + expectedIndex, record.getString("padding", 0));
          }
        }
        assertEquals(delegate.allocations.get(), delegate.releases.get());
      }
    }
  }

  private void assertSuccessfulReadReleasesBuffers(ReadMode mode) throws Exception {
    CountingAllocator delegate = new CountingAllocator();
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate)) {
      OwnedStream stream = new OwnedStream(inputFile.newStream(), mode);
      try (ParquetFileReader reader = ParquetFileReader.open(inputFile, footer, options(tracking), stream)) {
        try (PageReadStore pages = reader.readRowGroup(0)) {
          assertEquals(ROWS, pages.getRowCount());
          RecordReader<Group> records = new ColumnIOFactory()
              .getColumnIO(SCHEMA)
              .getRecordReader(pages, new GroupRecordConverter(SCHEMA));
          for (int row = 0; row < ROWS; row++) {
            Group record = records.read();
            assertEquals(row, record.getInteger("id", 0));
            assertEquals("padding_" + row, record.getString("padding", 0));
          }
          assertEquals(0, delegate.releases.get());
        }
        assertTrue(stream.rangeCount > 1);
        if (mode == ReadMode.SLICES) {
          assertEquals(2, delegate.allocations.get());
        } else {
          assertEquals(stream.rangeCount, delegate.allocations.get());
        }
        assertEquals(delegate.allocations.get(), delegate.releases.get());
      }
    }
  }

  @Test
  void testReleasesTransferredBuffersWhenPageHeaderParsingFails() throws Exception {
    CountingAllocator delegate = new CountingAllocator();
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate)) {
      try (ParquetFileReader reader = ParquetFileReader.open(
          inputFile, footer, options(tracking), new OwnedStream(inputFile.newStream(), ReadMode.CORRUPT))) {
        assertThrows(IOException.class, () -> reader.readRowGroup(0));
        assertEquals(delegate.allocations.get(), delegate.releases.get());
      }
      assertTrue(delegate.allocations.get() > 0);
      assertEquals(delegate.allocations.get(), delegate.releases.get());
    }
  }

  @Test
  void testReleasesFailedAndSuccessfulAllocationsAfterAllRangesFinish() throws Exception {
    CountingAllocator delegate = new CountingAllocator();
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate)) {
      OwnedStream stream = new OwnedStream(inputFile.newStream(), ReadMode.FAILED_RANGE);
      try (ParquetFileReader reader = ParquetFileReader.open(inputFile, footer, options(tracking), stream)) {
        try {
          assertThrows(IOException.class, () -> reader.readRowGroup(0));
          // All range futures already finished. Reclamation must not wait for deferred stream closure,
          // otherwise an outer allocator.close() can race the delayed release.
          assertEquals(delegate.allocations.get(), delegate.releases.get());
        } finally {
          stream.allowClose.countDown();
        }
      }
      assertTrue(delegate.allocations.get() > 1);
      assertEquals(delegate.allocations.get(), delegate.releases.get());
    }
  }

  private static ParquetReadOptions options(ByteBufferAllocator allocator) {
    return ParquetReadOptions.builder()
        .withAllocator(allocator)
        .withUseHadoopVectoredIo(true)
        .withMaxAllocationInBytes(128)
        .build();
  }

  private enum ReadMode {
    ORIGINALS,
    SLICES,
    CORRUPT,
    FAILED_RANGE
  }

  private static final class CountingAllocator extends HeapByteBufferAllocator {
    private final AtomicInteger allocations = new AtomicInteger();
    private final AtomicInteger releases = new AtomicInteger();

    @Override
    public ByteBuffer allocate(int size) {
      allocations.incrementAndGet();
      return super.allocate(size);
    }

    @Override
    public void release(ByteBuffer buffer) {
      releases.incrementAndGet();
    }
  }

  /** Supports vectored IO independently of the Hadoop version used to run the test. */
  private static final class OwnedStream extends DelegatingSeekableInputStream {
    private final SeekableInputStream delegate;
    private final ReadMode mode;
    private final CountDownLatch allowClose = new CountDownLatch(1);
    private int rangeCount;

    OwnedStream(SeekableInputStream delegate, ReadMode mode) {
      super(delegate);
      this.delegate = delegate;
      this.mode = mode;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }

    @Override
    public boolean readVectoredAvailable(ByteBufferAllocator allocator) {
      return true;
    }

    @Override
    public void close() throws IOException {
      try {
        if (mode == ReadMode.FAILED_RANGE) {
          try {
            if (!allowClose.await(10, TimeUnit.SECONDS)) {
              throw new IOException("Test did not unblock stream closure");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for test stream closure", e);
          }
        }
      } finally {
        super.close();
      }
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
      rangeCount = ranges.size();
      ByteBuffer merged = null;
      if (mode == ReadMode.SLICES) {
        // Model filesystem coalescing and checksum verification: several result slices share one original,
        // while an additional checksum allocation does not appear in any returned range.
        int totalLength =
            ranges.stream().mapToInt(ParquetFileRange::getLength).sum();
        merged = allocator.allocate(totalLength);
        allocator.allocate(8);
      }
      for (int index = 0; index < ranges.size(); index++) {
        ParquetFileRange range = ranges.get(index);
        ByteBuffer buffer;
        if (merged == null) {
          buffer = allocator.allocate(range.getLength());
        } else {
          buffer = merged.slice();
          buffer.limit(range.getLength());
          buffer = buffer.slice();
          merged.position(merged.position() + range.getLength());
        }
        if (mode == ReadMode.CORRUPT) {
          buffer.position(range.getLength()); // Zero-filled bytes are not a valid page header.
        } else {
          delegate.seek(range.getOffset());
          delegate.readFully(buffer);
        }
        buffer.flip();
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        if (mode == ReadMode.FAILED_RANGE && index == 0) {
          future.completeExceptionally(new IOException("injected range failure after allocation"));
        } else {
          future.complete(buffer);
        }
        range.setDataReadFuture(future);
      }
    }
  }
}
