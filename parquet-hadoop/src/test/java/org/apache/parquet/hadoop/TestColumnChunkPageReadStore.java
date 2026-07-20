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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;

public class TestColumnChunkPageReadStore {

  private static final ColumnDescriptor COLUMN = new ColumnDescriptor(
      new String[] {"x"}, new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "x"), 0, 0);

  private static final BytesInputDecompressor NOOP_DECOMPRESSOR = new BytesInputDecompressor() {
    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) {
      return bytes;
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) {}

    @Override
    public void release() {}
  };

  @Test
  public void closeWithoutSetReleaserDoesNotThrow() {
    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      ParquetReadOptions options =
          ParquetReadOptions.builder().withAllocator(allocator).build();

      ColumnChunkPageReadStore store = new ColumnChunkPageReadStore(0L);
      store.addColumn(COLUMN, newReaderWithoutPages(options));

      // setReleaser() is intentionally NOT called here.
      store.close();
    }
  }

  @Test
  public void closeReleasesReleaserEvenWhenReaderThrows() throws Exception {
    RuntimeException releaseFailure = new RuntimeException("release boom");
    ByteBufferAllocator throwingAllocator = throwingAllocator(releaseFailure);

    try (TrackingByteBufferAllocator storeAllocator =
        TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator())) {
      ColumnChunkPageReadStore store = new ColumnChunkPageReadStore(1L);
      store.addColumn(COLUMN, newReaderWithQueuedBuffer(throwingAllocator));

      // The store-level releaser holds a tracked buffer that must still be released even though the reader
      // fails first.
      ByteBufferReleaser storeReleaser = new ByteBufferReleaser(storeAllocator);
      storeReleaser.releaseLater(storeAllocator.allocate(8));
      store.setReleaser(storeReleaser);

      assertThatThrownBy(store::close)
          .isInstanceOf(RuntimeException.class)
          .hasMessage("Unable to close resource")
          .cause()
          .isSameAs(releaseFailure);
    }
  }

  @Test
  public void closeReportsBothReaderAndReleaserFailures() {
    RuntimeException readerFailure = new RuntimeException("reader boom");
    RuntimeException releaserFailure = new RuntimeException("releaser boom");

    ColumnChunkPageReadStore store = new ColumnChunkPageReadStore(1L);
    store.addColumn(COLUMN, newReaderWithQueuedBuffer(throwingAllocator(readerFailure)));

    // The store-level releaser also fails to release its queued buffer.
    ByteBufferAllocator throwingReleaserAllocator = throwingAllocator(releaserFailure);
    ByteBufferReleaser storeReleaser = new ByteBufferReleaser(throwingReleaserAllocator);
    storeReleaser.releaseLater(throwingReleaserAllocator.allocate(8));
    store.setReleaser(storeReleaser);

    assertThatThrownBy(store::close)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Unable to close resource")
        .cause()
        .isSameAs(readerFailure)
        .hasSuppressedException(releaserFailure);
  }

  private static ByteBufferAllocator throwingAllocator(RuntimeException releaseFailure) {
    return new ByteBufferAllocator() {
      @Override
      public ByteBuffer allocate(int size) {
        return ByteBuffer.allocateDirect(size);
      }

      @Override
      public void release(ByteBuffer b) {
        throw releaseFailure;
      }

      @Override
      public boolean isDirect() {
        return true;
      }
    };
  }

  private static ColumnChunkPageReader newReaderWithoutPages(ParquetReadOptions options) {
    return new ColumnChunkPageReader(
        NOOP_DECOMPRESSOR, Collections.<DataPage>emptyList(), null, null, 0L, null, null, 0, 0, options);
  }

  private static ColumnChunkPageReader newReaderWithQueuedBuffer(ByteBufferAllocator allocator) {
    ParquetReadOptions options = ParquetReadOptions.builder()
        .withAllocator(allocator)
        .useOffHeapDecryptBuffer(true)
        .build();

    ByteBuffer pageBytes = ByteBuffer.allocateDirect(4);
    pageBytes.putInt(0, 42);
    DataPageV1 page = new DataPageV1(BytesInput.from(pageBytes), 1, 4, null, RLE, RLE, PLAIN);

    ColumnChunkPageReader reader = new ColumnChunkPageReader(
        NOOP_DECOMPRESSOR,
        Collections.<DataPage>singletonList(page),
        null,
        null,
        1L,
        null,
        null,
        0,
        0,
        options);

    // Reading the page through the off-heap path queues a buffer into the reader's internal releaser, so
    // releaseBuffers() will later invoke the throwing allocator's release().
    if (reader.readPage() == null) {
      throw new IllegalStateException("Expected a page to be read");
    }
    return reader;
  }
}
