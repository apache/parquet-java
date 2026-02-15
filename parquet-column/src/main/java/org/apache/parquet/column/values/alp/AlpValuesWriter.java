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
 * │ 8 bytes │ 4B &times; numVectors │ (interleaved)│ (interleaved)│     │
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

  /**
   * Float-specific ALP values writer with incremental encoding and interleaved layout.
   */
  public static class FloatAlpValuesWriter extends AlpValuesWriter {
    private final float[] vectorBuffer;
    private int bufferCount;
    private int totalCount;
    private CapacityByteArrayOutputStream encodedVectors;
    private final List<Integer> vectorByteSizes;

    // Preset caching
    private int vectorsProcessed;
    private int[][] cachedPresets;
    private final Map<Long, Integer> presetCounts;

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
      this.presetCounts = new HashMap<>();
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
      // Find best encoding parameters
      AlpEncoderDecoder.EncodingParams params;
      if (cachedPresets != null) {
        params = AlpEncoderDecoder.findBestFloatParamsWithPresets(vectorBuffer, 0, vectorLen, cachedPresets);
      } else {
        params = AlpEncoderDecoder.findBestFloatParams(vectorBuffer, 0, vectorLen);
        // Track this combination for preset caching
        long key = ((long) params.exponent << Integer.SIZE) | params.factor;
        presetCounts.merge(key, 1, Integer::sum);
      }

      vectorsProcessed++;
      if (cachedPresets == null && vectorsProcessed >= SAMPLER_SAMPLE_VECTORS) {
        buildPresetCache();
      }

      // Encode values, detect exceptions, find placeholder
      int[] encoded = new int[vectorLen];
      short[] excPositions = new short[params.numExceptions];
      float[] excValues = new float[params.numExceptions];
      int excIdx = 0;
      int placeholder = 0;

      // First pass: find a non-exception value for placeholder
      for (int i = 0; i < vectorLen; i++) {
        if (!AlpEncoderDecoder.isFloatException(vectorBuffer[i], params.exponent, params.factor)) {
          placeholder = AlpEncoderDecoder.encodeFloat(vectorBuffer[i], params.exponent, params.factor);
          break;
        }
      }

      // Second pass: encode all values
      int minValue = Integer.MAX_VALUE;
      for (int i = 0; i < vectorLen; i++) {
        if (AlpEncoderDecoder.isFloatException(vectorBuffer[i], params.exponent, params.factor)) {
          excPositions[excIdx] = (short) i;
          excValues[excIdx] = vectorBuffer[i];
          excIdx++;
          encoded[i] = placeholder;
        } else {
          encoded[i] = AlpEncoderDecoder.encodeFloat(vectorBuffer[i], params.exponent, params.factor);
        }
        if (encoded[i] < minValue) {
          minValue = encoded[i];
        }
      }

      // Apply Frame of Reference (subtract min)
      // Deltas are unsigned: if the range exceeds Integer.MAX_VALUE, the
      // subtraction wraps but the unsigned bits are still correct.
      int maxDelta = 0;
      for (int i = 0; i < vectorLen; i++) {
        encoded[i] = encoded[i] - minValue;
        if (Integer.compareUnsigned(encoded[i], maxDelta) > 0) {
          maxDelta = encoded[i];
        }
      }

      int bitWidth = AlpEncoderDecoder.bitWidthForInt(maxDelta);

      // Record start position to measure vector byte size
      long startSize = encodedVectors.size();

      // Write AlpInfo (4 bytes): exponent(1) + factor(1) + numExceptions(2)
      ByteBuffer alpInfo = ByteBuffer.allocate(ALP_INFO_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      alpInfo.put((byte) params.exponent);
      alpInfo.put((byte) params.factor);
      alpInfo.putShort((short) params.numExceptions);
      encodedVectors.write(alpInfo.array(), 0, ALP_INFO_SIZE);

      // Write ForInfo (5 bytes for float): frameOfReference(4) + bitWidth(1)
      ByteBuffer forInfo = ByteBuffer.allocate(FLOAT_FOR_INFO_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      forInfo.putInt(minValue);
      forInfo.put((byte) bitWidth);
      encodedVectors.write(forInfo.array(), 0, FLOAT_FOR_INFO_SIZE);

      // Write bit-packed values using BytePacker
      if (bitWidth > 0) {
        packIntsWithBytePacker(encoded, vectorLen, bitWidth);
      }

      // Write exception positions (2 bytes each)
      if (params.numExceptions > 0) {
        ByteBuffer excPosBuf =
            ByteBuffer.allocate(params.numExceptions * Short.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < params.numExceptions; i++) {
          excPosBuf.putShort(excPositions[i]);
        }
        encodedVectors.write(excPosBuf.array(), 0, params.numExceptions * Short.BYTES);

        // Write exception values (4 bytes each for float)
        ByteBuffer excValBuf =
            ByteBuffer.allocate(params.numExceptions * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < params.numExceptions; i++) {
          excValBuf.putFloat(excValues[i]);
        }
        encodedVectors.write(excValBuf.array(), 0, params.numExceptions * Float.BYTES);
      }

      vectorByteSizes.add((int) (encodedVectors.size() - startSize));
    }

    private void packIntsWithBytePacker(int[] values, int count, int bitWidth) {
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
      int numFullGroups = count / 8;
      int remaining = count % 8;
      byte[] packed = new byte[bitWidth]; // reuse for each group of 8

      for (int g = 0; g < numFullGroups; g++) {
        packer.pack8Values(values, g * 8, packed, 0);
        encodedVectors.write(packed, 0, bitWidth);
      }

      // Handle partial last group: pad with zeros, write only spec-required bytes
      // Spec: total packed size = ceil(count * bitWidth / 8)
      if (remaining > 0) {
        int[] padded = new int[8];
        System.arraycopy(values, numFullGroups * 8, padded, 0, remaining);
        packer.pack8Values(padded, 0, packed, 0);
        int totalPackedBytes = (count * bitWidth + 7) / 8;
        int alreadyWritten = numFullGroups * bitWidth;
        encodedVectors.write(packed, 0, totalPackedBytes - alreadyWritten);
      }
    }

    private void buildPresetCache() {
      List<Map.Entry<Long, Integer>> sorted = new ArrayList<>(presetCounts.entrySet());
      sorted.sort(Comparator.<Map.Entry<Long, Integer>, Integer>comparing(Map.Entry::getValue)
          .reversed());
      int numPresets = Math.min(sorted.size(), MAX_PRESET_COMBINATIONS);
      cachedPresets = new int[numPresets][2];
      for (int i = 0; i < numPresets; i++) {
        long key = sorted.get(i).getKey();
        cachedPresets[i][0] = (int) (key >>> Integer.SIZE); // exponent
        cachedPresets[i][1] = (int) key; // factor
      }
    }

    @Override
    public long getBufferedSize() {
      // Encoded vectors so far + estimate for buffered values
      return encodedVectors.size() + (long) bufferCount * 3;
    }

    @Override
    public BytesInput getBytes() {
      if (totalCount == 0) {
        return BytesInput.empty();
      }

      // Flush any remaining partial vector
      if (bufferCount > 0) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }

      int numVectors = vectorByteSizes.size();

      // Build header (8 bytes)
      ByteBuffer header = ByteBuffer.allocate(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      header.put((byte) ALP_VERSION);
      header.put((byte) ALP_COMPRESSION_MODE);
      header.put((byte) ALP_INTEGER_ENCODING_FOR);
      header.put((byte) logVectorSize);
      header.putInt(totalCount);

      // Build offset array (4 bytes per vector)
      int offsetArraySize = numVectors * Integer.BYTES;
      ByteBuffer offsets = ByteBuffer.allocate(offsetArraySize).order(ByteOrder.LITTLE_ENDIAN);
      int currentOffset = offsetArraySize; // first vector starts after offset array
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
      presetCounts.clear();
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

  /**
   * Double-specific ALP values writer with incremental encoding and interleaved layout.
   */
  public static class DoubleAlpValuesWriter extends AlpValuesWriter {
    private final double[] vectorBuffer;
    private int bufferCount;
    private int totalCount;
    private CapacityByteArrayOutputStream encodedVectors;
    private final List<Integer> vectorByteSizes;

    // Preset caching
    private int vectorsProcessed;
    private int[][] cachedPresets;
    private final Map<Long, Integer> presetCounts;

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
      this.presetCounts = new HashMap<>();
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
      // Find best encoding parameters
      AlpEncoderDecoder.EncodingParams params;
      if (cachedPresets != null) {
        params = AlpEncoderDecoder.findBestDoubleParamsWithPresets(vectorBuffer, 0, vectorLen, cachedPresets);
      } else {
        params = AlpEncoderDecoder.findBestDoubleParams(vectorBuffer, 0, vectorLen);
        long key = ((long) params.exponent << Integer.SIZE) | params.factor;
        presetCounts.merge(key, 1, Integer::sum);
      }

      vectorsProcessed++;
      if (cachedPresets == null && vectorsProcessed >= SAMPLER_SAMPLE_VECTORS) {
        buildPresetCache();
      }

      // Encode values, detect exceptions, find placeholder
      long[] encoded = new long[vectorLen];
      short[] excPositions = new short[params.numExceptions];
      double[] excValues = new double[params.numExceptions];
      int excIdx = 0;
      long placeholder = 0;

      // First pass: find a non-exception value for placeholder
      for (int i = 0; i < vectorLen; i++) {
        if (!AlpEncoderDecoder.isDoubleException(vectorBuffer[i], params.exponent, params.factor)) {
          placeholder = AlpEncoderDecoder.encodeDouble(vectorBuffer[i], params.exponent, params.factor);
          break;
        }
      }

      // Second pass: encode all values
      long minValue = Long.MAX_VALUE;
      for (int i = 0; i < vectorLen; i++) {
        if (AlpEncoderDecoder.isDoubleException(vectorBuffer[i], params.exponent, params.factor)) {
          excPositions[excIdx] = (short) i;
          excValues[excIdx] = vectorBuffer[i];
          excIdx++;
          encoded[i] = placeholder;
        } else {
          encoded[i] = AlpEncoderDecoder.encodeDouble(vectorBuffer[i], params.exponent, params.factor);
        }
        if (encoded[i] < minValue) {
          minValue = encoded[i];
        }
      }

      // Apply Frame of Reference (subtract min)
      long maxDelta = 0;
      for (int i = 0; i < vectorLen; i++) {
        encoded[i] = encoded[i] - minValue;
        if (Long.compareUnsigned(encoded[i], maxDelta) > 0) {
          maxDelta = encoded[i];
        }
      }

      int bitWidth = AlpEncoderDecoder.bitWidthForLong(maxDelta);

      // Record start position to measure vector byte size
      long startSize = encodedVectors.size();

      // Write AlpInfo (4 bytes): exponent(1) + factor(1) + numExceptions(2)
      ByteBuffer alpInfo = ByteBuffer.allocate(ALP_INFO_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      alpInfo.put((byte) params.exponent);
      alpInfo.put((byte) params.factor);
      alpInfo.putShort((short) params.numExceptions);
      encodedVectors.write(alpInfo.array(), 0, ALP_INFO_SIZE);

      // Write ForInfo (9 bytes for double): frameOfReference(8) + bitWidth(1)
      ByteBuffer forInfo = ByteBuffer.allocate(DOUBLE_FOR_INFO_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      forInfo.putLong(minValue);
      forInfo.put((byte) bitWidth);
      encodedVectors.write(forInfo.array(), 0, DOUBLE_FOR_INFO_SIZE);

      // Write bit-packed values using BytePackerForLong
      if (bitWidth > 0) {
        packLongsWithBytePacker(encoded, vectorLen, bitWidth);
      }

      // Write exception positions (2 bytes each)
      if (params.numExceptions > 0) {
        ByteBuffer excPosBuf =
            ByteBuffer.allocate(params.numExceptions * Short.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < params.numExceptions; i++) {
          excPosBuf.putShort(excPositions[i]);
        }
        encodedVectors.write(excPosBuf.array(), 0, params.numExceptions * Short.BYTES);

        // Write exception values (8 bytes each for double)
        ByteBuffer excValBuf =
            ByteBuffer.allocate(params.numExceptions * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < params.numExceptions; i++) {
          excValBuf.putDouble(excValues[i]);
        }
        encodedVectors.write(excValBuf.array(), 0, params.numExceptions * Double.BYTES);
      }

      vectorByteSizes.add((int) (encodedVectors.size() - startSize));
    }

    private void packLongsWithBytePacker(long[] values, int count, int bitWidth) {
      BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
      int numFullGroups = count / 8;
      int remaining = count % 8;
      byte[] packed = new byte[bitWidth]; // reuse for each group of 8

      for (int g = 0; g < numFullGroups; g++) {
        packer.pack8Values(values, g * 8, packed, 0);
        encodedVectors.write(packed, 0, bitWidth);
      }

      // Handle partial last group: pad with zeros, write only spec-required bytes
      // Spec: total packed size = ceil(count * bitWidth / 8)
      if (remaining > 0) {
        long[] padded = new long[8];
        System.arraycopy(values, numFullGroups * 8, padded, 0, remaining);
        packer.pack8Values(padded, 0, packed, 0);
        int totalPackedBytes = (count * bitWidth + 7) / 8;
        int alreadyWritten = numFullGroups * bitWidth;
        encodedVectors.write(packed, 0, totalPackedBytes - alreadyWritten);
      }
    }

    private void buildPresetCache() {
      List<Map.Entry<Long, Integer>> sorted = new ArrayList<>(presetCounts.entrySet());
      sorted.sort(Comparator.<Map.Entry<Long, Integer>, Integer>comparing(Map.Entry::getValue)
          .reversed());
      int numPresets = Math.min(sorted.size(), MAX_PRESET_COMBINATIONS);
      cachedPresets = new int[numPresets][2];
      for (int i = 0; i < numPresets; i++) {
        long key = sorted.get(i).getKey();
        cachedPresets[i][0] = (int) (key >>> Integer.SIZE);
        cachedPresets[i][1] = (int) key;
      }
    }

    @Override
    public long getBufferedSize() {
      return encodedVectors.size() + (long) bufferCount * 5;
    }

    @Override
    public BytesInput getBytes() {
      if (totalCount == 0) {
        return BytesInput.empty();
      }

      // Flush any remaining partial vector
      if (bufferCount > 0) {
        encodeAndFlushVector(bufferCount);
        bufferCount = 0;
      }

      int numVectors = vectorByteSizes.size();

      // Build header (8 bytes)
      ByteBuffer header = ByteBuffer.allocate(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      header.put((byte) ALP_VERSION);
      header.put((byte) ALP_COMPRESSION_MODE);
      header.put((byte) ALP_INTEGER_ENCODING_FOR);
      header.put((byte) logVectorSize);
      header.putInt(totalCount);

      // Build offset array (4 bytes per vector)
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
      presetCounts.clear();
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
