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
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;

public abstract class ByteStreamSplitValuesWriter extends ValuesWriter {

  /**
   * Batch size for buffered scatter writes. Values are accumulated in a batch buffer
   * and flushed as bulk {@code write(byte[], off, len)} calls to each stream.
   */
  private static final int BATCH_SIZE = 64;

  protected final int numStreams;
  protected final int elementSizeInBytes;
  protected final CapacityByteArrayOutputStream[] byteStreams;

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
    long totalSize = 0;
    for (CapacityByteArrayOutputStream stream : this.byteStreams) {
      totalSize += stream.size();
    }
    return totalSize;
  }

  @Override
  public BytesInput getBytes() {
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
    for (CapacityByteArrayOutputStream stream : this.byteStreams) {
      stream.reset();
    }
  }

  @Override
  public void close() {
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
      super.scatterBytes(BytesUtils.intToBytes(Float.floatToIntBits(v)));
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
      super.scatterBytes(BytesUtils.longToBytes(Double.doubleToLongBits(v)));
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
      super.scatterBytes(BytesUtils.intToBytes(v));
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
      super.scatterBytes(BytesUtils.longToBytes(v));
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format("%s LongByteStreamSplitWriter %d bytes", prefix, getAllocatedSize());
    }
  }

  public static class FixedLenByteArrayByteStreamSplitValuesWriter extends ByteStreamSplitValuesWriter {
    private final int length;
    private byte[][] batchBufs; // [stream][batchIndex] scratch buffers
    private int flbaBatchCount;

    public FixedLenByteArrayByteStreamSplitValuesWriter(
        int length, int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      super(length, initialCapacity, pageSize, allocator);
      this.length = length;
    }

    @Override
    public final void writeBytes(Binary v) {
      assert (v.length() == length)
          : ("Fixed Binary size " + v.length() + " does not match field type length " + length);
      if (batchBufs == null) {
        batchBufs = new byte[length][BATCH_SIZE];
      }
      byte[] bytes = v.getBytesUnsafe();
      for (int stream = 0; stream < length; stream++) {
        batchBufs[stream][flbaBatchCount] = bytes[stream];
      }
      flbaBatchCount++;
      if (flbaBatchCount == BATCH_SIZE) {
        flushFlbaBatch();
      }
    }

    @Override
    public void writeBinaries(Binary[] values, int offset, int len) {
      if (batchBufs == null) {
        batchBufs = new byte[length][BATCH_SIZE];
      }
      for (int i = offset; i < offset + len; i++) {
        Binary v = values[i];
        assert (v.length() == length)
            : ("Fixed Binary size " + v.length() + " does not match field type length " + length);
        byte[] bytes = v.getBytesUnsafe();
        for (int stream = 0; stream < length; stream++) {
          batchBufs[stream][flbaBatchCount] = bytes[stream];
        }
        flbaBatchCount++;
        if (flbaBatchCount == BATCH_SIZE) {
          flushFlbaBatch();
        }
      }
    }

    private void flushFlbaBatch() {
      if (flbaBatchCount == 0) return;
      final int count = flbaBatchCount;
      for (int stream = 0; stream < length; stream++) {
        byteStreams[stream].write(batchBufs[stream], 0, count);
      }
      flbaBatchCount = 0;
    }

    @Override
    public BytesInput getBytes() {
      flushFlbaBatch();
      return super.getBytes();
    }

    @Override
    public void reset() {
      flbaBatchCount = 0;
      super.reset();
    }

    @Override
    public void close() {
      flbaBatchCount = 0;
      super.close();
    }

    @Override
    public long getBufferedSize() {
      return super.getBufferedSize() + (long) flbaBatchCount * length;
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s FixedLenByteArrayByteStreamSplitValuesWriter %d bytes", prefix, getAllocatedSize());
    }
  }
}
