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
package org.apache.parquet.column.values.bytestreamsplit;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;

public abstract class ByteStreamSplitValuesWriter extends ValuesWriter {

  /**
   * Batch size for buffered scatter writes. Values are accumulated in a batch buffer
   * and flushed as bulk {@code write(byte[], off, len)} calls to each stream, replacing
   * N individual single-byte writes with one bulk write per stream per flush.
   */
  private static final int BATCH_SIZE = 128;

  protected final int numStreams;
  protected final int elementSizeInBytes;
  private final CapacityByteArrayOutputStream[] byteStreams;

  // Batch buffers for int (4-byte) and long (8-byte) scatter writes.
  // Only one of these is ever non-null per instance.
  private int[] intBatch;
  private long[] longBatch;
  private byte[] scatterBuf;
  private int batchCount;

  public ByteStreamSplitValuesWriter(
      int elementSizeInBytes, int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
    if (elementSizeInBytes <= 0) {
      throw new ParquetEncodingException(String.format("Element byte size is invalid: %d", elementSizeInBytes));
    }
    this.numStreams = elementSizeInBytes;
    this.elementSizeInBytes = elementSizeInBytes;
    this.byteStreams = new CapacityByteArrayOutputStream[elementSizeInBytes];

    // Round-up the capacity hint.
    final int capacityPerStream = (pageSize + this.numStreams - 1) / this.numStreams;
    final int initialCapacityPerStream = (initialCapacity + this.numStreams - 1) / this.numStreams;
    for (int i = 0; i < this.numStreams; ++i) {
      this.byteStreams[i] =
          new CapacityByteArrayOutputStream(initialCapacityPerStream, capacityPerStream, allocator);
    }
  }

  @Override
  public long getBufferedSize() {
    // Include unflushed batch values without triggering a flush
    long totalSize = (long) batchCount * elementSizeInBytes;
    for (CapacityByteArrayOutputStream stream : this.byteStreams) {
      totalSize += stream.size();
    }
    return totalSize;
  }

  @Override
  public BytesInput getBytes() {
    flushBatch();
    BytesInput[] allInputs = new BytesInput[this.numStreams];
    for (int i = 0; i < this.numStreams; ++i) {
      allInputs[i] = BytesInput.from(this.byteStreams[i]);
    }
    return BytesInput.concat(allInputs);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.BYTE_STREAM_SPLIT;
  }

  @Override
  public void reset() {
    batchCount = 0;
    for (CapacityByteArrayOutputStream stream : this.byteStreams) {
      stream.reset();
    }
  }

  @Override
  public void close() {
    batchCount = 0;
    for (CapacityByteArrayOutputStream stream : byteStreams) {
      stream.close();
    }
  }

  protected void scatterBytes(byte[] bytes) {
    if (bytes.length != this.numStreams) {
      throw new ParquetEncodingException(String.format(
          "Number of bytes doesn't match the number of streams. Num butes: %d, Num streams: %d",
          bytes.length, this.numStreams));
    }
    for (int i = 0; i < bytes.length; ++i) {
      this.byteStreams[i].write(bytes[i]);
    }
  }

  /**
   * Buffer a 4-byte integer value for batched scatter to the byte streams.
   * Values are accumulated until the batch is full, then flushed as bulk
   * {@code write(byte[], off, len)} calls — one per stream.
   */
  protected void bufferInt(int v) {
    if (intBatch == null) {
      intBatch = new int[BATCH_SIZE];
      scatterBuf = new byte[BATCH_SIZE];
    }
    intBatch[batchCount++] = v;
    if (batchCount == BATCH_SIZE) {
      flushIntBatch();
    }
  }

  /**
   * Buffer an 8-byte long value for batched scatter to the byte streams.
   */
  protected void bufferLong(long v) {
    if (longBatch == null) {
      longBatch = new long[BATCH_SIZE];
      scatterBuf = new byte[BATCH_SIZE];
    }
    longBatch[batchCount++] = v;
    if (batchCount == BATCH_SIZE) {
      flushLongBatch();
    }
  }

  private void flushBatch() {
    if (batchCount == 0) return;
    if (intBatch != null) {
      flushIntBatch();
    } else if (longBatch != null) {
      flushLongBatch();
    }
  }

  private void flushIntBatch() {
    if (batchCount == 0) return;
    final int count = batchCount;
    for (int stream = 0; stream < 4; stream++) {
      final int shift = stream << 3; // stream * 8
      for (int i = 0; i < count; i++) {
        scatterBuf[i] = (byte) (intBatch[i] >>> shift);
      }
      byteStreams[stream].write(scatterBuf, 0, count);
    }
    batchCount = 0;
  }

  private void flushLongBatch() {
    if (batchCount == 0) return;
    final int count = batchCount;
    for (int stream = 0; stream < 8; stream++) {
      final int shift = stream << 3; // stream * 8
      for (int i = 0; i < count; i++) {
        scatterBuf[i] = (byte) (longBatch[i] >>> shift);
      }
      byteStreams[stream].write(scatterBuf, 0, count);
    }
    batchCount = 0;
  }

  @Override
  public long getAllocatedSize() {
    long totalCapacity = 0;
    for (CapacityByteArrayOutputStream stream : byteStreams) {
      totalCapacity += stream.getCapacity();
    }
    return totalCapacity;
  }

  public static class FloatByteStreamSplitValuesWriter extends ByteStreamSplitValuesWriter {

    public FloatByteStreamSplitValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      super(Float.BYTES, initialCapacity, pageSize, allocator);
    }

    @Override
    public void writeFloat(float v) {
      bufferInt(Float.floatToIntBits(v));
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format("%s FloatByteStreamSplitWriter %d bytes", prefix, getAllocatedSize());
    }
  }

  public static class DoubleByteStreamSplitValuesWriter extends ByteStreamSplitValuesWriter {

    public DoubleByteStreamSplitValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      super(Double.BYTES, initialCapacity, pageSize, allocator);
    }

    @Override
    public void writeDouble(double v) {
      bufferLong(Double.doubleToLongBits(v));
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format("%s DoubleByteStreamSplitWriter %d bytes", prefix, getAllocatedSize());
    }
  }

  public static class IntegerByteStreamSplitValuesWriter extends ByteStreamSplitValuesWriter {
    public IntegerByteStreamSplitValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      super(4, initialCapacity, pageSize, allocator);
    }

    @Override
    public void writeInteger(int v) {
      bufferInt(v);
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format("%s IntegerByteStreamSplitWriter %d bytes", prefix, getAllocatedSize());
    }
  }

  public static class LongByteStreamSplitValuesWriter extends ByteStreamSplitValuesWriter {
    public LongByteStreamSplitValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      super(8, initialCapacity, pageSize, allocator);
    }

    @Override
    public void writeLong(long v) {
      bufferLong(v);
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format("%s LongByteStreamSplitWriter %d bytes", prefix, getAllocatedSize());
    }
  }

  public static class FixedLenByteArrayByteStreamSplitValuesWriter extends ByteStreamSplitValuesWriter {
    private final int length;

    public FixedLenByteArrayByteStreamSplitValuesWriter(
        int length, int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      super(length, initialCapacity, pageSize, allocator);
      this.length = length;
    }

    @Override
    public final void writeBytes(Binary v) {
      assert (v.length() == length)
          : ("Fixed Binary size " + v.length() + " does not match field type length " + length);
      super.scatterBytes(v.getBytesUnsafe());
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s FixedLenByteArrayByteStreamSplitValuesWriter %d bytes", prefix, getAllocatedSize());
    }
  }
}
