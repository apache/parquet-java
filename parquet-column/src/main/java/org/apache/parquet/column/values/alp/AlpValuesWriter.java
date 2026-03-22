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
package org.apache.parquet.column.values.alp;

import static org.apache.parquet.column.values.alp.AlpConstants.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;

/**
 * ALP (Adaptive Lossless floating-Point) values writer.
 *
 * <p>ALP encoding converts floating-point values to integers using decimal scaling,
 * then applies Frame of Reference encoding and bit-packing.
 * Values that cannot be losslessly converted are stored as exceptions.
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
 * AlpInfo(4B) + ForInfo(5B/9B) + PackedValues + ExceptionPositions + ExceptionValues
 */
public abstract class AlpValuesWriter extends ValuesWriter {

  protected final int initialCapacity;
  protected final int pageSize;
  protected final ByteBufferAllocator allocator;
  protected final int vectorSize;
  protected final int logVectorSize;

  AlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
    AlpConstants.validateVectorSize(vectorSize);
    this.initialCapacity = initialCapacity;
    this.pageSize = pageSize;
    this.allocator = allocator;
    this.vectorSize = vectorSize;
    this.logVectorSize = Integer.numberOfTrailingZeros(vectorSize);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.ALP;
  }

  /** Float writer. Buffers one vector at a time, encodes and flushes when full. */
  public static class FloatAlpValuesWriter extends AlpValuesWriter {
    private final float[] vectorBuffer;
    private int bufferCount;
    private int totalCount;
    private CapacityByteArrayOutputStream encodedVectors;
    private final List<Integer> vectorByteSizes;

    // Preset caching: collect evenly-spaced sample vectors across the rowgroup,
    // then build presets using estimated compressed size (matching C++ AlpSampler).
    private int vectorsProcessed;
    private int[][] cachedPresets;
    private final List<float[]> rowgroupSamples;
    private final int rowgroupSampleJump;

    // Reusable per-vector buffers
    private final int[] encodedBuffer;
    private final short[] excPosBuffer;
    private final float[] excValBuffer;
    private final byte[] metadataBuf;
    private final byte[] packBuf;
    private final int[] packPadBuf;

    public FloatAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      this(initialCapacity, pageSize, allocator, DEFAULT_VECTOR_SIZE);
    }

    public FloatAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
      super(initialCapacity, pageSize, allocator, vectorSize);
      this.vectorBuffer = new float[vectorSize];
      this.bufferCount = 0;
      this.totalCount = 0;
      this.encodedVectors = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
      this.vectorByteSizes = new ArrayList<>();
      this.vectorsProcessed = 0;
      this.cachedPresets = null;
      this.rowgroupSamples = new ArrayList<>();
      // Space samples evenly: one sample every jump vectors across the rowgroup.
      // Math.max(1, ...) guards against very small rowgroups or large vector sizes.
      this.rowgroupSampleJump = Math.max(1, SAMPLER_ROWGROUP_SIZE / SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP / vectorSize);
      // Pre-allocate reusable buffers
      this.encodedBuffer = new int[vectorSize];
      this.excPosBuffer = new short[vectorSize];
      this.excValBuffer = new float[vectorSize];
      this.metadataBuf = new byte[Math.max(ALP_INFO_SIZE, FLOAT_FOR_INFO_SIZE)];
      this.packBuf = new byte[Integer.SIZE]; // max bit width for int
      this.packPadBuf = new int[8];
    }

    @Override
    public void writeFloat(float v) {
      vectorBuffer[bufferCount++] = v;
      totalCount++;
      if (bufferCount == vectorSize) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }
    }

    private void encodeAndFlushVector(int vectorLen) {
      // Use cached presets after the sampling phase, full search before
      AlpEncoderDecoder.EncodingParams params;
      if (cachedPresets != null) {
        params = AlpEncoderDecoder.findBestFloatParamsWithPresets(vectorBuffer, 0, vectorLen, cachedPresets);
      } else {
        params = AlpEncoderDecoder.findBestFloatParams(vectorBuffer, 0, vectorLen);
        // Collect one sample every rowgroupSampleJump vectors so that samples are
        // evenly distributed across the rowgroup (matching C++ AlpSampler spacing).
        if (vectorsProcessed % rowgroupSampleJump == 0
            && rowgroupSamples.size() < SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP) {
          rowgroupSamples.add(Arrays.copyOf(vectorBuffer, vectorLen));
        }
      }

      vectorsProcessed++;
      if (cachedPresets == null && rowgroupSamples.size() >= SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP) {
        buildPresetCache();
      }

      int excIdx = 0;

      // We need a valid encoded value to fill exception slots (placeholder).
      // Any non-exception value works; it gets overwritten on decode.
      int placeholder = 0;
      for (int i = 0; i < vectorLen; i++) {
        if (!AlpEncoderDecoder.isFloatException(vectorBuffer[i], params.exponent, params.factor)) {
          placeholder = AlpEncoderDecoder.encodeFloat(vectorBuffer[i], params.exponent, params.factor);
          break;
        }
      }

      int minValue = Integer.MAX_VALUE;
      for (int i = 0; i < vectorLen; i++) {
        if (AlpEncoderDecoder.isFloatException(vectorBuffer[i], params.exponent, params.factor)) {
          excPosBuffer[excIdx] = (short) i;
          excValBuffer[excIdx] = vectorBuffer[i];
          excIdx++;
          encodedBuffer[i] = placeholder;
        } else {
          encodedBuffer[i] = AlpEncoderDecoder.encodeFloat(vectorBuffer[i], params.exponent, params.factor);
        }
        if (encodedBuffer[i] < minValue) {
          minValue = encodedBuffer[i];
        }
      }

      // Subtract min so deltas start at 0, reducing bit width.
      // The subtraction may wrap for large ranges but unsigned bits stay correct.
      int maxDelta = 0;
      for (int i = 0; i < vectorLen; i++) {
        encodedBuffer[i] = encodedBuffer[i] - minValue;
        if (Integer.compareUnsigned(encodedBuffer[i], maxDelta) > 0) {
          maxDelta = encodedBuffer[i];
        }
      }

      int bitWidth = AlpEncoderDecoder.bitWidthForInt(maxDelta);

      long startSize = encodedVectors.size();

      // AlpInfo: exponent(1) + factor(1) + numExceptions(2) — little-endian
      metadataBuf[0] = (byte) params.exponent;
      metadataBuf[1] = (byte) params.factor;
      metadataBuf[2] = (byte) (params.numExceptions & 0xFF);
      metadataBuf[3] = (byte) ((params.numExceptions >>> 8) & 0xFF);
      encodedVectors.write(metadataBuf, 0, ALP_INFO_SIZE);

      // ForInfo: frameOfReference(4) + bitWidth(1) — little-endian
      metadataBuf[0] = (byte) (minValue & 0xFF);
      metadataBuf[1] = (byte) ((minValue >>> 8) & 0xFF);
      metadataBuf[2] = (byte) ((minValue >>> 16) & 0xFF);
      metadataBuf[3] = (byte) ((minValue >>> 24) & 0xFF);
      metadataBuf[4] = (byte) bitWidth;
      encodedVectors.write(metadataBuf, 0, FLOAT_FOR_INFO_SIZE);

      if (bitWidth > 0) {
        packIntsWithBytePacker(encodedBuffer, vectorLen, bitWidth);
      }

      // Exception positions then values, written as separate blocks
      if (params.numExceptions > 0) {
        for (int i = 0; i < params.numExceptions; i++) {
          int pos = excPosBuffer[i] & 0xFFFF;
          metadataBuf[0] = (byte) (pos & 0xFF);
          metadataBuf[1] = (byte) ((pos >>> 8) & 0xFF);
          encodedVectors.write(metadataBuf, 0, Short.BYTES);
        }

        for (int i = 0; i < params.numExceptions; i++) {
          int bits = Float.floatToRawIntBits(excValBuffer[i]);
          metadataBuf[0] = (byte) (bits & 0xFF);
          metadataBuf[1] = (byte) ((bits >>> 8) & 0xFF);
          metadataBuf[2] = (byte) ((bits >>> 16) & 0xFF);
          metadataBuf[3] = (byte) ((bits >>> 24) & 0xFF);
          encodedVectors.write(metadataBuf, 0, Float.BYTES);
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

      // Partial last group: pack 8 values (zero-padded), but only write
      // ceil(count * bitWidth / 8) - alreadyWritten bytes per spec.
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

    private void buildPresetCache() {
      // For each sampled vector, find the best (e,f) using estimated compressed size,
      // then rank combos by how many samples they win. Take the top MAX_PRESET_COMBINATIONS.
      Map<Long, Integer> comboCounts = new HashMap<>();
      for (float[] sample : rowgroupSamples) {
        AlpEncoderDecoder.EncodingParams best =
            AlpEncoderDecoder.findBestFloatParams(sample, 0, sample.length);
        long key = ((long) best.exponent << Integer.SIZE) | best.factor;
        comboCounts.merge(key, 1, Integer::sum);
      }
      List<Map.Entry<Long, Integer>> sorted = new ArrayList<>(comboCounts.entrySet());
      sorted.sort(Comparator.<Map.Entry<Long, Integer>, Integer>comparing(Map.Entry::getValue)
          .reversed());
      int numPresets = Math.min(sorted.size(), MAX_PRESET_COMBINATIONS);
      cachedPresets = new int[numPresets][2];
      for (int i = 0; i < numPresets; i++) {
        long key = sorted.get(i).getKey();
        cachedPresets[i][0] = (int) (key >>> Integer.SIZE); // exponent
        cachedPresets[i][1] = (int) key; // factor
      }
      rowgroupSamples.clear();
    }

    @Override
    public long getBufferedSize() {
      // Encoded vectors so far + estimate for buffered values
      return encodedVectors.size() + (long) bufferCount * 3;
    }

    @Override
    public BytesInput getBytes() {
      if (bufferCount > 0) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }

      int numVectors = vectorByteSizes.size();

      // Assemble: [header 7B] [offset array 4B*N] [vector data...]
      ByteBuffer header = ByteBuffer.allocate(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      header.put((byte) ALP_COMPRESSION_MODE);
      header.put((byte) ALP_INTEGER_ENCODING_FOR);
      header.put((byte) logVectorSize);
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
      vectorsProcessed = 0;
      cachedPresets = null;
      rowgroupSamples.clear();
    }

    @Override
    public void close() {
      encodedVectors.close();
    }

    @Override
    public long getAllocatedSize() {
      return (long) vectorBuffer.length * Float.BYTES + encodedVectors.getCapacity();
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s FloatAlpValuesWriter %d values, %d bytes allocated", prefix, totalCount, getAllocatedSize());
    }
  }

  /** Double writer. Same structure as FloatAlpValuesWriter but uses longs. */
  public static class DoubleAlpValuesWriter extends AlpValuesWriter {
    private final double[] vectorBuffer;
    private int bufferCount;
    private int totalCount;
    private CapacityByteArrayOutputStream encodedVectors;
    private final List<Integer> vectorByteSizes;

    // Preset caching: collect evenly-spaced sample vectors across the rowgroup,
    // then build presets using estimated compressed size (matching C++ AlpSampler).
    private int vectorsProcessed;
    private int[][] cachedPresets;
    private final List<double[]> rowgroupSamples;
    private final int rowgroupSampleJump;

    // Reusable per-vector buffers
    private final long[] encodedBuffer;
    private final short[] excPosBuffer;
    private final double[] excValBuffer;
    private final byte[] metadataBuf;
    private final byte[] packBuf;
    private final long[] packPadBuf;

    public DoubleAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      this(initialCapacity, pageSize, allocator, DEFAULT_VECTOR_SIZE);
    }

    public DoubleAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
      super(initialCapacity, pageSize, allocator, vectorSize);
      this.vectorBuffer = new double[vectorSize];
      this.bufferCount = 0;
      this.totalCount = 0;
      this.encodedVectors = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
      this.vectorByteSizes = new ArrayList<>();
      this.vectorsProcessed = 0;
      this.cachedPresets = null;
      this.rowgroupSamples = new ArrayList<>();
      this.rowgroupSampleJump = Math.max(1, SAMPLER_ROWGROUP_SIZE / SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP / vectorSize);
      // Pre-allocate reusable buffers
      this.encodedBuffer = new long[vectorSize];
      this.excPosBuffer = new short[vectorSize];
      this.excValBuffer = new double[vectorSize];
      this.metadataBuf = new byte[Math.max(ALP_INFO_SIZE, DOUBLE_FOR_INFO_SIZE)];
      this.packBuf = new byte[Long.SIZE]; // max bit width for long
      this.packPadBuf = new long[8];
    }

    @Override
    public void writeDouble(double v) {
      vectorBuffer[bufferCount++] = v;
      totalCount++;
      if (bufferCount == vectorSize) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }
    }

    private void encodeAndFlushVector(int vectorLen) {
      AlpEncoderDecoder.EncodingParams params;
      if (cachedPresets != null) {
        params = AlpEncoderDecoder.findBestDoubleParamsWithPresets(vectorBuffer, 0, vectorLen, cachedPresets);
      } else {
        params = AlpEncoderDecoder.findBestDoubleParams(vectorBuffer, 0, vectorLen);
        if (vectorsProcessed % rowgroupSampleJump == 0
            && rowgroupSamples.size() < SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP) {
          rowgroupSamples.add(Arrays.copyOf(vectorBuffer, vectorLen));
        }
      }

      vectorsProcessed++;
      if (cachedPresets == null && rowgroupSamples.size() >= SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP) {
        buildPresetCache();
      }

      int excIdx = 0;
      long placeholder = 0;
      for (int i = 0; i < vectorLen; i++) {
        if (!AlpEncoderDecoder.isDoubleException(vectorBuffer[i], params.exponent, params.factor)) {
          placeholder = AlpEncoderDecoder.encodeDouble(vectorBuffer[i], params.exponent, params.factor);
          break;
        }
      }

      long minValue = Long.MAX_VALUE;
      for (int i = 0; i < vectorLen; i++) {
        if (AlpEncoderDecoder.isDoubleException(vectorBuffer[i], params.exponent, params.factor)) {
          excPosBuffer[excIdx] = (short) i;
          excValBuffer[excIdx] = vectorBuffer[i];
          excIdx++;
          encodedBuffer[i] = placeholder;
        } else {
          encodedBuffer[i] = AlpEncoderDecoder.encodeDouble(vectorBuffer[i], params.exponent, params.factor);
        }
        if (encodedBuffer[i] < minValue) {
          minValue = encodedBuffer[i];
        }
      }

      long maxDelta = 0;
      for (int i = 0; i < vectorLen; i++) {
        encodedBuffer[i] = encodedBuffer[i] - minValue;
        if (Long.compareUnsigned(encodedBuffer[i], maxDelta) > 0) {
          maxDelta = encodedBuffer[i];
        }
      }

      int bitWidth = AlpEncoderDecoder.bitWidthForLong(maxDelta);

      long startSize = encodedVectors.size();

      // AlpInfo: exponent(1) + factor(1) + numExceptions(2) — little-endian
      metadataBuf[0] = (byte) params.exponent;
      metadataBuf[1] = (byte) params.factor;
      metadataBuf[2] = (byte) (params.numExceptions & 0xFF);
      metadataBuf[3] = (byte) ((params.numExceptions >>> 8) & 0xFF);
      encodedVectors.write(metadataBuf, 0, ALP_INFO_SIZE);

      // ForInfo: frameOfReference(8) + bitWidth(1) — little-endian
      metadataBuf[0] = (byte) (minValue & 0xFF);
      metadataBuf[1] = (byte) ((minValue >>> 8) & 0xFF);
      metadataBuf[2] = (byte) ((minValue >>> 16) & 0xFF);
      metadataBuf[3] = (byte) ((minValue >>> 24) & 0xFF);
      metadataBuf[4] = (byte) ((minValue >>> 32) & 0xFF);
      metadataBuf[5] = (byte) ((minValue >>> 40) & 0xFF);
      metadataBuf[6] = (byte) ((minValue >>> 48) & 0xFF);
      metadataBuf[7] = (byte) ((minValue >>> 56) & 0xFF);
      metadataBuf[8] = (byte) bitWidth;
      encodedVectors.write(metadataBuf, 0, DOUBLE_FOR_INFO_SIZE);

      if (bitWidth > 0) {
        packLongsWithBytePacker(encodedBuffer, vectorLen, bitWidth);
      }

      if (params.numExceptions > 0) {
        for (int i = 0; i < params.numExceptions; i++) {
          int pos = excPosBuffer[i] & 0xFFFF;
          metadataBuf[0] = (byte) (pos & 0xFF);
          metadataBuf[1] = (byte) ((pos >>> 8) & 0xFF);
          encodedVectors.write(metadataBuf, 0, Short.BYTES);
        }

        for (int i = 0; i < params.numExceptions; i++) {
          long bits = Double.doubleToRawLongBits(excValBuffer[i]);
          metadataBuf[0] = (byte) (bits & 0xFF);
          metadataBuf[1] = (byte) ((bits >>> 8) & 0xFF);
          metadataBuf[2] = (byte) ((bits >>> 16) & 0xFF);
          metadataBuf[3] = (byte) ((bits >>> 24) & 0xFF);
          metadataBuf[4] = (byte) ((bits >>> 32) & 0xFF);
          metadataBuf[5] = (byte) ((bits >>> 40) & 0xFF);
          metadataBuf[6] = (byte) ((bits >>> 48) & 0xFF);
          metadataBuf[7] = (byte) ((bits >>> 56) & 0xFF);
          encodedVectors.write(metadataBuf, 0, Double.BYTES);
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

    private void buildPresetCache() {
      Map<Long, Integer> comboCounts = new HashMap<>();
      for (double[] sample : rowgroupSamples) {
        AlpEncoderDecoder.EncodingParams best =
            AlpEncoderDecoder.findBestDoubleParams(sample, 0, sample.length);
        long key = ((long) best.exponent << Integer.SIZE) | best.factor;
        comboCounts.merge(key, 1, Integer::sum);
      }
      List<Map.Entry<Long, Integer>> sorted = new ArrayList<>(comboCounts.entrySet());
      sorted.sort(Comparator.<Map.Entry<Long, Integer>, Integer>comparing(Map.Entry::getValue)
          .reversed());
      int numPresets = Math.min(sorted.size(), MAX_PRESET_COMBINATIONS);
      cachedPresets = new int[numPresets][2];
      for (int i = 0; i < numPresets; i++) {
        long key = sorted.get(i).getKey();
        cachedPresets[i][0] = (int) (key >>> Integer.SIZE);
        cachedPresets[i][1] = (int) key;
      }
      rowgroupSamples.clear();
    }

    @Override
    public long getBufferedSize() {
      return encodedVectors.size() + (long) bufferCount * 5;
    }

    @Override
    public BytesInput getBytes() {
      if (bufferCount > 0) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }

      int numVectors = vectorByteSizes.size();

      ByteBuffer header = ByteBuffer.allocate(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      header.put((byte) ALP_COMPRESSION_MODE);
      header.put((byte) ALP_INTEGER_ENCODING_FOR);
      header.put((byte) logVectorSize);
      header.putInt(totalCount);

      if (totalCount == 0) {
        return BytesInput.from(header.array());
      }

      // Offset array lets the reader jump to any vector in O(1)
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
      vectorsProcessed = 0;
      cachedPresets = null;
      rowgroupSamples.clear();
    }

    @Override
    public void close() {
      encodedVectors.close();
    }

    @Override
    public long getAllocatedSize() {
      return (long) vectorBuffer.length * Double.BYTES + encodedVectors.getCapacity();
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s DoubleAlpValuesWriter %d values, %d bytes allocated", prefix, totalCount, getAllocatedSize());
    }
  }
}