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
package org.apache.parquet.column.values.pfor;

import static org.apache.parquet.column.values.pfor.PforConstants.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;

/**
 * PFOR (Patched Frame of Reference) values writer for INT32 and INT64 columns.
 *
 * <p>PFOR compresses integer columns by subtracting the minimum value (FOR),
 * selecting an optimal bit width via a histogram-based cost model, bit-packing
 * the deltas, and storing outlier values (exceptions) separately.
 *
 * <p>Writing is incremental: values are buffered in a fixed-size vector buffer,
 * and each full vector is encoded and flushed to the output stream immediately.
 * On {@link #getBytes()}, any remaining partial vector is flushed, and the
 * final page bytes are assembled.
 *
 * <p>Interleaved Page Layout:
 * <pre>
 * ┌─────────┬──────────────────────┬──────────────┬──────────────┬─────┐
 * │ Header  │ Offset Array         │ Vector 0     │ Vector 1     │ ... │
 * │ 7 bytes │ 4B &times; numVectors │ (interleaved)│ (interleaved)│     │
 * └─────────┴──────────────────────┴──────────────┴──────────────┴─────┘
 * </pre>
 *
 * <p>Each vector contains interleaved:
 * PforVectorInfo(7B/11B) + PackedValues + ExceptionPositions + ExceptionValues
 */
public abstract class PforValuesWriter extends ValuesWriter {

  protected final int initialCapacity;
  protected final int pageSize;
  protected final ByteBufferAllocator allocator;
  protected final int vectorSize;
  protected final int logVectorSize;

  PforValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
    PforConstants.validateVectorSize(vectorSize);
    this.initialCapacity = initialCapacity;
    this.pageSize = pageSize;
    this.allocator = allocator;
    this.vectorSize = vectorSize;
    this.logVectorSize = Integer.numberOfTrailingZeros(vectorSize);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.PFOR;
  }

  /** INT32 writer. Buffers one vector at a time, encodes and flushes when full. */
  public static class IntPforValuesWriter extends PforValuesWriter {
    private final int[] vectorBuffer;
    private int bufferCount;
    private int totalCount;
    private CapacityByteArrayOutputStream encodedVectors;
    private final List<Integer> vectorByteSizes;

    // Reusable per-vector buffers to avoid allocations on every encodeAndFlushVector call
    private final int[] deltasBuffer;
    private final short[] excPosBuffer;
    private final int[] excValBuffer;
    private final byte[] metadataBuf;
    private final byte[] packBuf;
    private final int[] packPadBuf;

    public IntPforValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      this(initialCapacity, pageSize, allocator, DEFAULT_VECTOR_SIZE);
    }

    public IntPforValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
      super(initialCapacity, pageSize, allocator, vectorSize);
      this.vectorBuffer = new int[vectorSize];
      this.bufferCount = 0;
      this.totalCount = 0;
      this.encodedVectors = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
      this.vectorByteSizes = new ArrayList<>();
      this.deltasBuffer = new int[vectorSize];
      this.excPosBuffer = new short[vectorSize];
      this.excValBuffer = new int[vectorSize];
      this.metadataBuf = new byte[INT32_VECTOR_INFO_SIZE];
      this.packBuf = new byte[Integer.SIZE]; // max bit width for int = 32 bytes
      this.packPadBuf = new int[8];
    }

    @Override
    public void writeInteger(int v) {
      vectorBuffer[bufferCount++] = v;
      totalCount++;
      if (bufferCount == vectorSize) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }
    }

    private void encodeAndFlushVector(int vectorLen) {
      // Find minimum value (frame of reference)
      int minValue = vectorBuffer[0];
      for (int i = 1; i < vectorLen; i++) {
        if (vectorBuffer[i] < minValue) {
          minValue = vectorBuffer[i];
        }
      }

      // Compute unsigned deltas into reusable buffer
      for (int i = 0; i < vectorLen; i++) {
        deltasBuffer[i] = vectorBuffer[i] - minValue;
      }

      // Find optimal bit width via cost model
      PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltasBuffer, vectorLen);
      int bitWidth = result.bitWidth;
      int numExceptions = result.numExceptions;

      // Collect exceptions: values whose delta doesn't fit in bitWidth bits
      int excIdx = 0;
      if (numExceptions > 0) {
        int mask = (bitWidth == 32) ? -1 : (1 << bitWidth) - 1;
        for (int i = 0; i < vectorLen; i++) {
          if (Integer.compareUnsigned(deltasBuffer[i], mask) > 0) {
            excPosBuffer[excIdx] = (short) i;
            excValBuffer[excIdx] = vectorBuffer[i];
            excIdx++;
            deltasBuffer[i] = 0;
          }
        }
      }

      long startSize = encodedVectors.size();

      // PforVectorInfo: frame_of_reference(4) + bit_width(1) + num_exceptions(2) = 7B
      metadataBuf[0] = (byte) (minValue & 0xFF);
      metadataBuf[1] = (byte) ((minValue >>> 8) & 0xFF);
      metadataBuf[2] = (byte) ((minValue >>> 16) & 0xFF);
      metadataBuf[3] = (byte) ((minValue >>> 24) & 0xFF);
      metadataBuf[4] = (byte) bitWidth;
      metadataBuf[5] = (byte) (numExceptions & 0xFF);
      metadataBuf[6] = (byte) ((numExceptions >>> 8) & 0xFF);
      encodedVectors.write(metadataBuf, 0, INT32_VECTOR_INFO_SIZE);

      // Pack deltas
      if (bitWidth > 0) {
        packIntsWithBytePacker(deltasBuffer, vectorLen, bitWidth);
      }

      // Exception positions then values
      if (numExceptions > 0) {
        for (int i = 0; i < numExceptions; i++) {
          int pos = excPosBuffer[i] & 0xFFFF;
          metadataBuf[0] = (byte) (pos & 0xFF);
          metadataBuf[1] = (byte) ((pos >>> 8) & 0xFF);
          encodedVectors.write(metadataBuf, 0, Short.BYTES);
        }

        for (int i = 0; i < numExceptions; i++) {
          int val = excValBuffer[i];
          metadataBuf[0] = (byte) (val & 0xFF);
          metadataBuf[1] = (byte) ((val >>> 8) & 0xFF);
          metadataBuf[2] = (byte) ((val >>> 16) & 0xFF);
          metadataBuf[3] = (byte) ((val >>> 24) & 0xFF);
          encodedVectors.write(metadataBuf, 0, Integer.BYTES);
        }
      }

      vectorByteSizes.add((int) (encodedVectors.size() - startSize));
    }

    private void packIntsWithBytePacker(int[] values, int count, int bitWidth) {
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
      int numFullGroups = count / 8;
      int remaining = count % 8;

      for (int g = 0; g < numFullGroups; g++) {
        packer.pack8Values(values, g * 8, packBuf, 0);
        encodedVectors.write(packBuf, 0, bitWidth);
      }

      if (remaining > 0) {
        System.arraycopy(values, numFullGroups * 8, packPadBuf, 0, remaining);
        for (int i = remaining; i < 8; i++) {
          packPadBuf[i] = 0;
        }
        packer.pack8Values(packPadBuf, 0, packBuf, 0);
        int totalPackedBytes = (count * bitWidth + 7) / 8;
        int alreadyWritten = numFullGroups * bitWidth;
        encodedVectors.write(packBuf, 0, totalPackedBytes - alreadyWritten);
      }
    }

    @Override
    public long getBufferedSize() {
      return encodedVectors.size() + (long) bufferCount * Integer.BYTES;
    }

    @Override
    public BytesInput getBytes() {
      if (bufferCount > 0) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }

      int numVectors = vectorByteSizes.size();

      // Header: packing_mode(1) + log_vector_size(1) + value_byte_width(1) + num_elements(4) = 7B
      ByteBuffer header = ByteBuffer.allocate(PFOR_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      header.put((byte) PFOR_PACKING_MODE_FOR);
      header.put((byte) logVectorSize);
      header.put((byte) INT32_VALUE_BYTE_WIDTH);
      header.putInt(totalCount);

      if (totalCount == 0) {
        return BytesInput.from(header.array());
      }

      int offsetArraySize = numVectors * Integer.BYTES;
      ByteBuffer offsets = ByteBuffer.allocate(offsetArraySize).order(ByteOrder.LITTLE_ENDIAN);
      int currentOffset = offsetArraySize;
      for (int v = 0; v < numVectors; v++) {
        offsets.putInt(currentOffset);
        currentOffset += vectorByteSizes.get(v);
      }

      return BytesInput.concat(
          BytesInput.from(header.array()), BytesInput.from(offsets.array()), BytesInput.from(encodedVectors));
    }

    @Override
    public void reset() {
      bufferCount = 0;
      totalCount = 0;
      encodedVectors.reset();
      vectorByteSizes.clear();
    }

    @Override
    public void close() {
      encodedVectors.close();
    }

    @Override
    public long getAllocatedSize() {
      return (long) vectorBuffer.length * Integer.BYTES + encodedVectors.getCapacity();
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s IntPforValuesWriter %d values, %d bytes allocated", prefix, totalCount, getAllocatedSize());
    }
  }

  /** INT64 writer. Same structure as IntPforValuesWriter but uses longs. */
  public static class LongPforValuesWriter extends PforValuesWriter {
    private final long[] vectorBuffer;
    private int bufferCount;
    private int totalCount;
    private CapacityByteArrayOutputStream encodedVectors;
    private final List<Integer> vectorByteSizes;

    // Reusable per-vector buffers
    private final long[] deltasBuffer;
    private final short[] excPosBuffer;
    private final long[] excValBuffer;
    private final byte[] metadataBuf;
    private final byte[] packBuf;
    private final long[] packPadBuf;

    public LongPforValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      this(initialCapacity, pageSize, allocator, DEFAULT_VECTOR_SIZE);
    }

    public LongPforValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
      super(initialCapacity, pageSize, allocator, vectorSize);
      this.vectorBuffer = new long[vectorSize];
      this.bufferCount = 0;
      this.totalCount = 0;
      this.encodedVectors = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
      this.vectorByteSizes = new ArrayList<>();
      this.deltasBuffer = new long[vectorSize];
      this.excPosBuffer = new short[vectorSize];
      this.excValBuffer = new long[vectorSize];
      this.metadataBuf = new byte[INT64_VECTOR_INFO_SIZE];
      this.packBuf = new byte[Long.SIZE]; // max bit width for long = 64 bytes
      this.packPadBuf = new long[8];
    }

    @Override
    public void writeLong(long v) {
      vectorBuffer[bufferCount++] = v;
      totalCount++;
      if (bufferCount == vectorSize) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }
    }

    private void encodeAndFlushVector(int vectorLen) {
      long minValue = vectorBuffer[0];
      for (int i = 1; i < vectorLen; i++) {
        if (vectorBuffer[i] < minValue) {
          minValue = vectorBuffer[i];
        }
      }

      for (int i = 0; i < vectorLen; i++) {
        deltasBuffer[i] = vectorBuffer[i] - minValue;
      }

      PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForLong(deltasBuffer, vectorLen);
      int bitWidth = result.bitWidth;
      int numExceptions = result.numExceptions;

      int excIdx = 0;
      if (numExceptions > 0) {
        long mask = (bitWidth == 64) ? -1L : (1L << bitWidth) - 1L;
        for (int i = 0; i < vectorLen; i++) {
          if (Long.compareUnsigned(deltasBuffer[i], mask) > 0) {
            excPosBuffer[excIdx] = (short) i;
            excValBuffer[excIdx] = vectorBuffer[i];
            excIdx++;
            deltasBuffer[i] = 0;
          }
        }
      }

      long startSize = encodedVectors.size();

      // PforVectorInfo: frame_of_reference(8) + bit_width(1) + num_exceptions(2) = 11B
      metadataBuf[0] = (byte) (minValue & 0xFF);
      metadataBuf[1] = (byte) ((minValue >>> 8) & 0xFF);
      metadataBuf[2] = (byte) ((minValue >>> 16) & 0xFF);
      metadataBuf[3] = (byte) ((minValue >>> 24) & 0xFF);
      metadataBuf[4] = (byte) ((minValue >>> 32) & 0xFF);
      metadataBuf[5] = (byte) ((minValue >>> 40) & 0xFF);
      metadataBuf[6] = (byte) ((minValue >>> 48) & 0xFF);
      metadataBuf[7] = (byte) ((minValue >>> 56) & 0xFF);
      metadataBuf[8] = (byte) bitWidth;
      metadataBuf[9] = (byte) (numExceptions & 0xFF);
      metadataBuf[10] = (byte) ((numExceptions >>> 8) & 0xFF);
      encodedVectors.write(metadataBuf, 0, INT64_VECTOR_INFO_SIZE);

      if (bitWidth > 0) {
        packLongsWithBytePacker(deltasBuffer, vectorLen, bitWidth);
      }

      if (numExceptions > 0) {
        for (int i = 0; i < numExceptions; i++) {
          int pos = excPosBuffer[i] & 0xFFFF;
          metadataBuf[0] = (byte) (pos & 0xFF);
          metadataBuf[1] = (byte) ((pos >>> 8) & 0xFF);
          encodedVectors.write(metadataBuf, 0, Short.BYTES);
        }

        for (int i = 0; i < numExceptions; i++) {
          long val = excValBuffer[i];
          metadataBuf[0] = (byte) (val & 0xFF);
          metadataBuf[1] = (byte) ((val >>> 8) & 0xFF);
          metadataBuf[2] = (byte) ((val >>> 16) & 0xFF);
          metadataBuf[3] = (byte) ((val >>> 24) & 0xFF);
          metadataBuf[4] = (byte) ((val >>> 32) & 0xFF);
          metadataBuf[5] = (byte) ((val >>> 40) & 0xFF);
          metadataBuf[6] = (byte) ((val >>> 48) & 0xFF);
          metadataBuf[7] = (byte) ((val >>> 56) & 0xFF);
          encodedVectors.write(metadataBuf, 0, Long.BYTES);
        }
      }

      vectorByteSizes.add((int) (encodedVectors.size() - startSize));
    }

    private void packLongsWithBytePacker(long[] values, int count, int bitWidth) {
      BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
      int numFullGroups = count / 8;
      int remaining = count % 8;

      for (int g = 0; g < numFullGroups; g++) {
        packer.pack8Values(values, g * 8, packBuf, 0);
        encodedVectors.write(packBuf, 0, bitWidth);
      }

      if (remaining > 0) {
        System.arraycopy(values, numFullGroups * 8, packPadBuf, 0, remaining);
        for (int i = remaining; i < 8; i++) {
          packPadBuf[i] = 0;
        }
        packer.pack8Values(packPadBuf, 0, packBuf, 0);
        int totalPackedBytes = (count * bitWidth + 7) / 8;
        int alreadyWritten = numFullGroups * bitWidth;
        encodedVectors.write(packBuf, 0, totalPackedBytes - alreadyWritten);
      }
    }

    @Override
    public long getBufferedSize() {
      return encodedVectors.size() + (long) bufferCount * Long.BYTES;
    }

    @Override
    public BytesInput getBytes() {
      if (bufferCount > 0) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }

      int numVectors = vectorByteSizes.size();

      ByteBuffer header = ByteBuffer.allocate(PFOR_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      header.put((byte) PFOR_PACKING_MODE_FOR);
      header.put((byte) logVectorSize);
      header.put((byte) INT64_VALUE_BYTE_WIDTH);
      header.putInt(totalCount);

      if (totalCount == 0) {
        return BytesInput.from(header.array());
      }

      int offsetArraySize = numVectors * Integer.BYTES;
      ByteBuffer offsets = ByteBuffer.allocate(offsetArraySize).order(ByteOrder.LITTLE_ENDIAN);
      int currentOffset = offsetArraySize;
      for (int v = 0; v < numVectors; v++) {
        offsets.putInt(currentOffset);
        currentOffset += vectorByteSizes.get(v);
      }

      return BytesInput.concat(
          BytesInput.from(header.array()), BytesInput.from(offsets.array()), BytesInput.from(encodedVectors));
    }

    @Override
    public void reset() {
      bufferCount = 0;
      totalCount = 0;
      encodedVectors.reset();
      vectorByteSizes.clear();
    }

    @Override
    public void close() {
      encodedVectors.close();
    }

    @Override
    public long getAllocatedSize() {
      return (long) vectorBuffer.length * Long.BYTES + encodedVectors.getCapacity();
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s LongPforValuesWriter %d values, %d bytes allocated", prefix, totalCount, getAllocatedSize());
    }
  }
}
