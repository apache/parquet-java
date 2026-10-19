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

import static org.apache.parquet.column.values.alp.AlpConstants.ALP_COMPRESSION_MODE;
import static org.apache.parquet.column.values.alp.AlpConstants.ALP_HEADER_SIZE;
import static org.apache.parquet.column.values.alp.AlpConstants.ALP_INTEGER_ENCODING_FOR;
import static org.apache.parquet.column.values.alp.AlpConstants.DEFAULT_VECTOR_SIZE;
import static org.apache.parquet.column.values.alp.AlpConstants.PACK_GROUP_SIZE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;

/**
 * ALP (Adaptive Lossless floating-Point) values writer.
 *
 * <p>Writing is incremental: values are buffered in a fixed-size vector buffer,
 * and each full vector is encoded and flushed to the output stream immediately.
 * On {@link #getBytes()}, any remaining partial vector is flushed, and the
 * final page bytes are assembled.
 *
 * <p>Page layout:
 * <pre>
 * ┌─────────┬──────────────────────┬──────────────┬──────────────┬─────┐
 * │ Header  │ Offset Array         │ Vector 0     │ Vector 1     │ ... │
 * │ 7 bytes │ 4B × numVectors      │              │              │     │
 * └─────────┴──────────────────────┴──────────────┴──────────────┴─────┘
 * </pre>
 *
 * <p>Each vector holds AlpInfo(4B) + ForInfo(5B/9B) + PackedValues + ExceptionPositions +
 * ExceptionValues.
 */
public abstract class AlpValuesWriter extends ValuesWriter {

  // The spec's sampling parameters: 8 sampled vectors per row group and up to 5 preset
  // combinations. The row group size comes from the C++ implementation.
  static final int SAMPLER_ROWGROUP_SIZE = 122_880;
  static final int SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP = 8;
  static final int MAX_PRESET_COMBINATIONS = 5;

  protected final int valuesPerVector;
  protected int bufferCount = 0;
  private int totalValueCount = 0;
  private final CapacityByteArrayOutputStream encodedVectors;
  private final List<Integer> vectorByteSizes = new ArrayList<>();
  private final int[] exceptionPositions;
  private final ByteBuffer littleEndian = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);

  private final int sampleJump;
  private int vectorsProcessed = 0;
  private final List<int[]> sampledParams = new ArrayList<>();
  private int[][] presets;

  AlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
    AlpConstants.validateVectorSize(vectorSize);
    this.valuesPerVector = vectorSize;
    this.encodedVectors = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
    this.exceptionPositions = new int[vectorSize];
    this.sampleJump = Math.max(1, SAMPLER_ROWGROUP_SIZE / SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP / vectorSize);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.ALP;
  }

  /** Counts the value just stored at {@link #bufferCount} and flushes the vector once it is full. */
  protected final void addedValue() {
    bufferCount++;
    totalValueCount++;
    if (bufferCount == valuesPerVector) {
      encodeAndFlushVector();
    }
  }

  private void encodeAndFlushVector() {
    int vectorSize = bufferCount;
    AlpCodec.EncodingParams params = chooseParams(vectorSize);
    int numExceptions = encodeVector(vectorSize, params.exponent, params.factor, exceptionPositions);
    long frameOfReference = subtractFrameOfReference(vectorSize);
    int bitWidth = deltaBitWidth(vectorSize);

    long startSize = encodedVectors.size();
    writeLittleEndian(params.exponent, 1);
    writeLittleEndian(params.factor, 1);
    writeLittleEndian(numExceptions, Short.BYTES);
    writeLittleEndian(frameOfReference, valueSize());
    writeLittleEndian(bitWidth, 1);
    if (bitWidth > 0) {
      writePackedValues(vectorSize, bitWidth, encodedVectors);
    }
    for (int i = 0; i < numExceptions; i++) {
      writeLittleEndian(exceptionPositions[i], Short.BYTES);
    }
    for (int i = 0; i < numExceptions; i++) {
      writeLittleEndian(exceptionValueBits(i), valueSize());
    }
    vectorByteSizes.add((int) (encodedVectors.size() - startSize));
    bufferCount = 0;
  }

  /**
   * Searches every (exponent, factor) pair until enough vectors have been sampled, then only the
   * presets built from those samples.
   */
  private AlpCodec.EncodingParams chooseParams(int vectorSize) {
    AlpCodec.EncodingParams params;
    if (presets == null) {
      params = findBestParams(vectorSize, allPairs());
      if (vectorsProcessed % sampleJump == 0 && sampledParams.size() < SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP) {
        sampledParams.add(new int[] {params.exponent, params.factor});
      }
      if (sampledParams.size() >= SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP) {
        presets = buildPresets(sampledParams, MAX_PRESET_COMBINATIONS);
        sampledParams.clear();
      }
    } else {
      params = findBestParams(vectorSize, presets);
    }
    vectorsProcessed++;
    return params;
  }

  /** The (exponent, factor) pairs that won the most samples, most frequent first. */
  static int[][] buildPresets(List<int[]> sampledParams, int maxPresets) {
    Map<Long, Integer> counts = new HashMap<>();
    for (int[] params : sampledParams) {
      counts.merge(((long) params[0] << Integer.SIZE) | params[1], 1, Integer::sum);
    }
    // Ties keep HashMap order. Changing it changes which preset comes first, and so the bytes.
    List<Map.Entry<Long, Integer>> byCount = new ArrayList<>(counts.entrySet());
    byCount.sort(Map.Entry.<Long, Integer>comparingByValue().reversed());
    int[][] presets = new int[Math.min(byCount.size(), maxPresets)][];
    for (int i = 0; i < presets.length; i++) {
      long key = byCount.get(i).getKey();
      presets[i] = new int[] {(int) (key >>> Integer.SIZE), (int) key};
    }
    return presets;
  }

  private void writeLittleEndian(long value, int byteCount) {
    littleEndian.clear();
    littleEndian.putLong(value);
    encodedVectors.write(littleEndian.array(), 0, byteCount);
  }

  @Override
  public long getBufferedSize() {
    return encodedVectors.size() + (long) bufferCount * estimatedBytesPerBufferedValue();
  }

  @Override
  public BytesInput getBytes() {
    if (bufferCount > 0) {
      encodeAndFlushVector();
    }

    ByteBuffer header = ByteBuffer.allocate(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    header.put((byte) ALP_COMPRESSION_MODE);
    header.put((byte) ALP_INTEGER_ENCODING_FOR);
    header.put((byte) Integer.numberOfTrailingZeros(valuesPerVector));
    header.putInt(totalValueCount);
    if (totalValueCount == 0) {
      return BytesInput.from(header.array());
    }

    int numVectors = vectorByteSizes.size();
    ByteBuffer offsets = ByteBuffer.allocate(numVectors * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    int offset = numVectors * Integer.BYTES;
    for (int vectorByteSize : vectorByteSizes) {
      offsets.putInt(offset);
      offset += vectorByteSize;
    }
    return BytesInput.concat(
        BytesInput.from(header.array()), BytesInput.from(offsets.array()), BytesInput.from(encodedVectors));
  }

  @Override
  public void reset() {
    bufferCount = 0;
    totalValueCount = 0;
    encodedVectors.reset();
    vectorByteSizes.clear();
    // The sampling state is kept: a sampled row group spans many pages.
  }

  /** Visible for testing: whether enough vectors have been sampled to build the presets. */
  boolean hasPresetCache() {
    return presets != null;
  }

  @Override
  public void close() {
    encodedVectors.close();
  }

  @Override
  public long getAllocatedSize() {
    return (long) valuesPerVector * valueSize() + encodedVectors.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format(
        "%s %s %d values, %d bytes allocated",
        prefix, getClass().getSimpleName(), totalValueCount, getAllocatedSize());
  }

  /** Size in bytes of one value of this type, and of its frame of reference. */
  protected abstract int valueSize();

  /** A rough size for a value not yet encoded; the exact size is known once its vector is. */
  protected abstract int estimatedBytesPerBufferedValue();

  /** Every valid (exponent, factor) pair for this type, searched until the presets are built. */
  protected abstract int[][] allPairs();

  protected abstract AlpCodec.EncodingParams findBestParams(int vectorSize, int[][] pairs);

  /**
   * Encodes the buffered values, recording exceptions in {@code exceptionPositions} and the
   * type's exception values, and returns how many there are.
   */
  protected abstract int encodeVector(int vectorSize, int exponent, int factor, int[] exceptionPositions);

  protected abstract long subtractFrameOfReference(int vectorSize);

  protected abstract int deltaBitWidth(int vectorSize);

  protected abstract void writePackedValues(int vectorSize, int bitWidth, CapacityByteArrayOutputStream out);

  /** The raw bits of exception {@code index}, as recorded by {@link #encodeVector}. */
  protected abstract long exceptionValueBits(int index);

  /** Float writer. Buffers one vector at a time, encodes and flushes when full. */
  public static class FloatAlpValuesWriter extends AlpValuesWriter {
    private final float[] vectorBuffer;
    private final int[] encoded;
    private final float[] exceptionValues;
    private final byte[] packed = new byte[Integer.SIZE];
    private final int[] padded = new int[PACK_GROUP_SIZE];
    private final AlpCodec.EncodeResult encodeResult = new AlpCodec.EncodeResult();

    public FloatAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      this(initialCapacity, pageSize, allocator, DEFAULT_VECTOR_SIZE);
    }

    public FloatAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
      super(initialCapacity, pageSize, allocator, vectorSize);
      this.vectorBuffer = new float[vectorSize];
      this.encoded = new int[vectorSize];
      this.exceptionValues = new float[vectorSize];
    }

    @Override
    public void writeFloat(float v) {
      vectorBuffer[bufferCount] = v;
      addedValue();
    }

    @Override
    protected int valueSize() {
      return Float.BYTES;
    }

    @Override
    protected int estimatedBytesPerBufferedValue() {
      return 3;
    }

    @Override
    protected int[][] allPairs() {
      return AlpCodec.ALL_VALID_FLOAT_PAIRS;
    }

    @Override
    protected AlpCodec.EncodingParams findBestParams(int vectorSize, int[][] pairs) {
      return AlpCodec.findBestFloatParams(vectorBuffer, 0, vectorSize, pairs);
    }

    @Override
    protected int encodeVector(int vectorSize, int exponent, int factor, int[] exceptionPositions) {
      return encode(
          vectorBuffer,
          vectorSize,
          exponent,
          factor,
          encoded,
          exceptionPositions,
          exceptionValues,
          encodeResult);
    }

    @Override
    protected long subtractFrameOfReference(int vectorSize) {
      return subtractMin(encoded, vectorSize);
    }

    @Override
    protected int deltaBitWidth(int vectorSize) {
      return bitWidth(encoded, vectorSize);
    }

    @Override
    protected void writePackedValues(int vectorSize, int bitWidth, CapacityByteArrayOutputStream out) {
      pack(encoded, vectorSize, bitWidth, packed, padded, out);
    }

    @Override
    protected long exceptionValueBits(int index) {
      return Float.floatToRawIntBits(exceptionValues[index]);
    }

    /**
     * Encodes {@code count} values into {@code encoded}, recording each exception's position and
     * value, and returns how many there are.
     */
    static int encode(
        float[] values,
        int count,
        int exponent,
        int factor,
        int[] encoded,
        int[] exceptionPositions,
        float[] exceptionValues,
        AlpCodec.EncodeResult scratch) {
      int numExceptions = 0;
      int placeholder = 0;
      boolean havePlaceholder = false;
      for (int i = 0; i < count; i++) {
        AlpCodec.tryEncodeFloat(values[i], exponent, factor, scratch);
        if (scratch.isException) {
          exceptionPositions[numExceptions] = i;
          exceptionValues[numExceptions] = values[i];
          numExceptions++;
        } else {
          encoded[i] = (int) scratch.encoded;
          if (!havePlaceholder) {
            placeholder = encoded[i];
            havePlaceholder = true;
          }
        }
      }
      // Exception slots take an encoding already in the vector, so they don't widen the deltas.
      for (int k = 0; k < numExceptions; k++) {
        encoded[exceptionPositions[k]] = placeholder;
      }
      return numExceptions;
    }

    /** Bits needed for the largest delta, read as unsigned. */
    static int bitWidth(int[] deltas, int count) {
      int maxDelta = 0;
      for (int i = 0; i < count; i++) {
        if (Integer.compareUnsigned(deltas[i], maxDelta) > 0) {
          maxDelta = deltas[i];
        }
      }
      return BytesUtils.getWidthFromMaxInt(maxDelta);
    }

    /** Subtracts the minimum from every value, wrapping as unsigned, and returns the minimum. */
    static int subtractMin(int[] values, int count) {
      int min = Integer.MAX_VALUE;
      for (int i = 0; i < count; i++) {
        min = Math.min(min, values[i]);
      }
      for (int i = 0; i < count; i++) {
        values[i] -= min;
      }
      return min;
    }

    /**
     * Packs {@code count} values in groups of 8, writing only the bytes the last, partial group
     * needs.
     */
    static void pack(
        int[] values, int count, int bitWidth, byte[] packed, int[] padded, CapacityByteArrayOutputStream out) {
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
      int fullGroups = count / PACK_GROUP_SIZE;
      for (int g = 0; g < fullGroups; g++) {
        packer.pack8Values(values, g * PACK_GROUP_SIZE, packed, 0);
        out.write(packed, 0, bitWidth);
      }
      int remaining = count % PACK_GROUP_SIZE;
      if (remaining > 0) {
        System.arraycopy(values, fullGroups * PACK_GROUP_SIZE, padded, 0, remaining);
        Arrays.fill(padded, remaining, PACK_GROUP_SIZE, 0);
        packer.pack8Values(padded, 0, packed, 0);
        out.write(packed, 0, BytesUtils.paddedByteCountFromBits(count * bitWidth) - fullGroups * bitWidth);
      }
    }
  }

  /** Double writer. Buffers one vector at a time, encodes and flushes when full. */
  public static class DoubleAlpValuesWriter extends AlpValuesWriter {
    private final double[] vectorBuffer;
    private final long[] encoded;
    private final double[] exceptionValues;
    private final byte[] packed = new byte[Long.SIZE];
    private final long[] padded = new long[PACK_GROUP_SIZE];
    private final AlpCodec.EncodeResult encodeResult = new AlpCodec.EncodeResult();

    public DoubleAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      this(initialCapacity, pageSize, allocator, DEFAULT_VECTOR_SIZE);
    }

    public DoubleAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
      super(initialCapacity, pageSize, allocator, vectorSize);
      this.vectorBuffer = new double[vectorSize];
      this.encoded = new long[vectorSize];
      this.exceptionValues = new double[vectorSize];
    }

    @Override
    public void writeDouble(double v) {
      vectorBuffer[bufferCount] = v;
      addedValue();
    }

    @Override
    protected int valueSize() {
      return Double.BYTES;
    }

    @Override
    protected int estimatedBytesPerBufferedValue() {
      return 5;
    }

    @Override
    protected int[][] allPairs() {
      return AlpCodec.ALL_VALID_DOUBLE_PAIRS;
    }

    @Override
    protected AlpCodec.EncodingParams findBestParams(int vectorSize, int[][] pairs) {
      return AlpCodec.findBestDoubleParams(vectorBuffer, 0, vectorSize, pairs);
    }

    @Override
    protected int encodeVector(int vectorSize, int exponent, int factor, int[] exceptionPositions) {
      return encode(
          vectorBuffer,
          vectorSize,
          exponent,
          factor,
          encoded,
          exceptionPositions,
          exceptionValues,
          encodeResult);
    }

    @Override
    protected long subtractFrameOfReference(int vectorSize) {
      return subtractMin(encoded, vectorSize);
    }

    @Override
    protected int deltaBitWidth(int vectorSize) {
      return bitWidth(encoded, vectorSize);
    }

    @Override
    protected void writePackedValues(int vectorSize, int bitWidth, CapacityByteArrayOutputStream out) {
      pack(encoded, vectorSize, bitWidth, packed, padded, out);
    }

    @Override
    protected long exceptionValueBits(int index) {
      return Double.doubleToRawLongBits(exceptionValues[index]);
    }

    /**
     * Encodes {@code count} values into {@code encoded}, recording each exception's position and
     * value, and returns how many there are.
     */
    static int encode(
        double[] values,
        int count,
        int exponent,
        int factor,
        long[] encoded,
        int[] exceptionPositions,
        double[] exceptionValues,
        AlpCodec.EncodeResult scratch) {
      int numExceptions = 0;
      long placeholder = 0;
      boolean havePlaceholder = false;
      for (int i = 0; i < count; i++) {
        AlpCodec.tryEncodeDouble(values[i], exponent, factor, scratch);
        if (scratch.isException) {
          exceptionPositions[numExceptions] = i;
          exceptionValues[numExceptions] = values[i];
          numExceptions++;
        } else {
          encoded[i] = scratch.encoded;
          if (!havePlaceholder) {
            placeholder = encoded[i];
            havePlaceholder = true;
          }
        }
      }
      // Exception slots take an encoding already in the vector, so they don't widen the deltas.
      for (int k = 0; k < numExceptions; k++) {
        encoded[exceptionPositions[k]] = placeholder;
      }
      return numExceptions;
    }

    /** Bits needed for the largest delta, read as unsigned. */
    static int bitWidth(long[] deltas, int count) {
      long maxDelta = 0;
      for (int i = 0; i < count; i++) {
        if (Long.compareUnsigned(deltas[i], maxDelta) > 0) {
          maxDelta = deltas[i];
        }
      }
      return BytesUtils.getWidthFromMaxLong(maxDelta);
    }

    /** Subtracts the minimum from every value, wrapping as unsigned, and returns the minimum. */
    static long subtractMin(long[] values, int count) {
      long min = Long.MAX_VALUE;
      for (int i = 0; i < count; i++) {
        min = Math.min(min, values[i]);
      }
      for (int i = 0; i < count; i++) {
        values[i] -= min;
      }
      return min;
    }

    /**
     * Packs {@code count} values in groups of 8, writing only the bytes the last, partial group
     * needs.
     */
    static void pack(
        long[] values,
        int count,
        int bitWidth,
        byte[] packed,
        long[] padded,
        CapacityByteArrayOutputStream out) {
      BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
      int fullGroups = count / PACK_GROUP_SIZE;
      for (int g = 0; g < fullGroups; g++) {
        packer.pack8Values(values, g * PACK_GROUP_SIZE, packed, 0);
        out.write(packed, 0, bitWidth);
      }
      int remaining = count % PACK_GROUP_SIZE;
      if (remaining > 0) {
        System.arraycopy(values, fullGroups * PACK_GROUP_SIZE, padded, 0, remaining);
        Arrays.fill(padded, remaining, PACK_GROUP_SIZE, 0L);
        packer.pack8Values(padded, 0, packed, 0);
        out.write(packed, 0, BytesUtils.paddedByteCountFromBits(count * bitWidth) - fullGroups * bitWidth);
      }
    }
  }
}
