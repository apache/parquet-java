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
import java.util.Arrays;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;

/**
 * ALP compression and decompression for single vectors of floating-point values.
 *
 * <p>Compression selects an exponent and factor, converts values to integers, applies
 * frame-of-reference encoding, and bit-packs the resulting deltas.
 */
final class AlpCompression {

  private AlpCompression() {}

  /** Preset containing candidate (exponent, factor) combinations from sampling. */
  static final class AlpEncodingPreset {
    final int[][] combinations;

    AlpEncodingPreset(int[][] combinations) {
      this.combinations = combinations;
    }
  }

  /**
   * A compressed ALP vector for float data.
   *
   * <p>Wire format (little-endian):
   * [ALP metadata (4 bytes)][FOR metadata (5 bytes)][packed values][exception positions][exception values]
   */
  static final class FloatCompressedVector {
    int exponent;
    int factor;
    int numExceptions;
    int frameOfReference;
    int bitWidth;
    int numElements;
    byte[] packedValues;
    short[] exceptionPositions;
    float[] exceptionValues;

    int storedSize() {
      return ALP_INFO_SIZE + FLOAT_FOR_INFO_SIZE + dataStoredSize();
    }

    int dataStoredSize() {
      return AlpEncoderDecoder.bitPackedSize(numElements, bitWidth)
          + numExceptions * (POSITION_SIZE + Float.BYTES);
    }

    void store(byte[] output, int offset) {
      ByteBuffer buf = ByteBuffer.wrap(output, offset, storedSize()).order(ByteOrder.LITTLE_ENDIAN);
      buf.put((byte) exponent);
      buf.put((byte) factor);
      buf.putShort((short) numExceptions);
      buf.putInt(frameOfReference);
      buf.put((byte) bitWidth);
      storeDataTo(buf);
    }

    void storeDataOnly(byte[] output, int offset) {
      ByteBuffer buf = ByteBuffer.wrap(output, offset, dataStoredSize()).order(ByteOrder.LITTLE_ENDIAN);
      storeDataTo(buf);
    }

    private void storeDataTo(ByteBuffer buf) {
      int packedSize = AlpEncoderDecoder.bitPackedSize(numElements, bitWidth);
      buf.put(packedValues, 0, packedSize);
      for (int i = 0; i < numExceptions; i++) {
        buf.putShort(exceptionPositions[i]);
      }
      for (int i = 0; i < numExceptions; i++) {
        buf.putFloat(exceptionValues[i]);
      }
    }

    static FloatCompressedVector load(byte[] input, int offset, int numElements) {
      ByteBuffer buf =
          ByteBuffer.wrap(input, offset, input.length - offset).order(ByteOrder.LITTLE_ENDIAN);
      FloatCompressedVector v = new FloatCompressedVector();
      v.numElements = numElements;
      v.exponent = buf.get() & 0xFF;
      v.factor = buf.get() & 0xFF;
      v.numExceptions = buf.getShort() & 0xFFFF;
      v.frameOfReference = buf.getInt();
      v.bitWidth = buf.get() & 0xFF;
      int packedSize = AlpEncoderDecoder.bitPackedSize(numElements, v.bitWidth);
      v.packedValues = new byte[packedSize];
      buf.get(v.packedValues);
      v.exceptionPositions = new short[v.numExceptions];
      for (int i = 0; i < v.numExceptions; i++) {
        v.exceptionPositions[i] = buf.getShort();
      }
      v.exceptionValues = new float[v.numExceptions];
      for (int i = 0; i < v.numExceptions; i++) {
        v.exceptionValues[i] = buf.getFloat();
      }
      return v;
    }
  }

  /**
   * A compressed ALP vector for double data.
   *
   * <p>Wire format (little-endian):
   * [ALP metadata (4 bytes)][FOR metadata (9 bytes)][packed values][exception positions][exception values]
   */
  static final class DoubleCompressedVector {
    int exponent;
    int factor;
    int numExceptions;
    long frameOfReference;
    int bitWidth;
    int numElements;
    byte[] packedValues;
    short[] exceptionPositions;
    double[] exceptionValues;

    int storedSize() {
      return ALP_INFO_SIZE + DOUBLE_FOR_INFO_SIZE + dataStoredSize();
    }

    int dataStoredSize() {
      return AlpEncoderDecoder.bitPackedSize(numElements, bitWidth)
          + numExceptions * (POSITION_SIZE + Double.BYTES);
    }

    void store(byte[] output, int offset) {
      ByteBuffer buf = ByteBuffer.wrap(output, offset, storedSize()).order(ByteOrder.LITTLE_ENDIAN);
      buf.put((byte) exponent);
      buf.put((byte) factor);
      buf.putShort((short) numExceptions);
      buf.putLong(frameOfReference);
      buf.put((byte) bitWidth);
      storeDataTo(buf);
    }

    void storeDataOnly(byte[] output, int offset) {
      ByteBuffer buf = ByteBuffer.wrap(output, offset, dataStoredSize()).order(ByteOrder.LITTLE_ENDIAN);
      storeDataTo(buf);
    }

    private void storeDataTo(ByteBuffer buf) {
      int packedSize = AlpEncoderDecoder.bitPackedSize(numElements, bitWidth);
      buf.put(packedValues, 0, packedSize);
      for (int i = 0; i < numExceptions; i++) {
        buf.putShort(exceptionPositions[i]);
      }
      for (int i = 0; i < numExceptions; i++) {
        buf.putDouble(exceptionValues[i]);
      }
    }

    static DoubleCompressedVector load(byte[] input, int offset, int numElements) {
      ByteBuffer buf =
          ByteBuffer.wrap(input, offset, input.length - offset).order(ByteOrder.LITTLE_ENDIAN);
      DoubleCompressedVector v = new DoubleCompressedVector();
      v.numElements = numElements;
      v.exponent = buf.get() & 0xFF;
      v.factor = buf.get() & 0xFF;
      v.numExceptions = buf.getShort() & 0xFFFF;
      v.frameOfReference = buf.getLong();
      v.bitWidth = buf.get() & 0xFF;
      int packedSize = AlpEncoderDecoder.bitPackedSize(numElements, v.bitWidth);
      v.packedValues = new byte[packedSize];
      buf.get(v.packedValues);
      v.exceptionPositions = new short[v.numExceptions];
      for (int i = 0; i < v.numExceptions; i++) {
        v.exceptionPositions[i] = buf.getShort();
      }
      v.exceptionValues = new double[v.numExceptions];
      for (int i = 0; i < v.numExceptions; i++) {
        v.exceptionValues[i] = buf.getDouble();
      }
      return v;
    }
  }

  static FloatCompressedVector compressFloatVector(float[] input, int count, AlpEncodingPreset preset) {
    if (count == 0) {
      FloatCompressedVector r = new FloatCompressedVector();
      r.packedValues = new byte[0];
      r.exceptionPositions = new short[0];
      r.exceptionValues = new float[0];
      return r;
    }

    AlpEncoderDecoder.EncodingParams params =
        AlpEncoderDecoder.findBestFloatParamsWithPresets(input, 0, count, preset.combinations);
    int exponent = params.exponent;
    int factor = params.factor;

    int[] encoded = new int[count];
    for (int i = 0; i < count; i++) {
      encoded[i] = AlpEncoderDecoder.encodeFloat(input[i], exponent, factor);
    }

    int numExceptions = 0;
    short[] exceptionPositions = new short[count];
    for (int i = 0; i < count; i++) {
      float decoded = AlpEncoderDecoder.decodeFloat(encoded[i], exponent, factor);
      if (Float.floatToRawIntBits(decoded) != Float.floatToRawIntBits(input[i])) {
        exceptionPositions[numExceptions++] = (short) i;
      }
    }

    int placeholder = 0;
    int exceptionIndex = 0;
    for (int i = 0; i < count; i++) {
      if (exceptionIndex < numExceptions && (exceptionPositions[exceptionIndex] & 0xFFFF) == i) {
        exceptionIndex++;
      } else {
        placeholder = encoded[i];
        break;
      }
    }

    float[] exceptionValues = new float[numExceptions];
    for (int i = 0; i < numExceptions; i++) {
      int position = exceptionPositions[i] & 0xFFFF;
      exceptionValues[i] = input[position];
      encoded[position] = placeholder;
    }

    int min = encoded[0];
    int max = encoded[0];
    for (int i = 1; i < count; i++) {
      if (encoded[i] < min) min = encoded[i];
      if (encoded[i] > max) max = encoded[i];
    }
    for (int i = 0; i < count; i++) {
      encoded[i] -= min;
    }
    int maxDelta = max - min;

    int bitWidth = AlpEncoderDecoder.bitWidthForInt(maxDelta);
    int packedSize = AlpEncoderDecoder.bitPackedSize(count, bitWidth);
    byte[] packedValues = new byte[packedSize];
    if (bitWidth > 0) {
      packInts(encoded, count, bitWidth, packedValues);
    }

    FloatCompressedVector result = new FloatCompressedVector();
    result.exponent = exponent;
    result.factor = factor;
    result.numExceptions = numExceptions;
    result.frameOfReference = min;
    result.bitWidth = bitWidth;
    result.numElements = count;
    result.packedValues = packedValues;
    result.exceptionPositions = Arrays.copyOf(exceptionPositions, numExceptions);
    result.exceptionValues = exceptionValues;
    return result;
  }

  static void decompressFloatVector(FloatCompressedVector v, float[] output) {
    int[] encoded = new int[v.numElements];
    if (v.bitWidth > 0) {
      unpackInts(v.packedValues, v.numElements, v.bitWidth, encoded);
    }

    for (int i = 0; i < v.numElements; i++) {
      int encodedValue = encoded[i] + v.frameOfReference;
      output[i] = AlpEncoderDecoder.decodeFloat(encodedValue, v.exponent, v.factor);
    }

    for (int i = 0; i < v.numExceptions; i++) {
      output[v.exceptionPositions[i] & 0xFFFF] = v.exceptionValues[i];
    }
  }

  static DoubleCompressedVector compressDoubleVector(double[] input, int count, AlpEncodingPreset preset) {
    if (count == 0) {
      DoubleCompressedVector r = new DoubleCompressedVector();
      r.packedValues = new byte[0];
      r.exceptionPositions = new short[0];
      r.exceptionValues = new double[0];
      return r;
    }

    AlpEncoderDecoder.EncodingParams params =
        AlpEncoderDecoder.findBestDoubleParamsWithPresets(input, 0, count, preset.combinations);
    int exponent = params.exponent;
    int factor = params.factor;

    long[] encoded = new long[count];
    for (int i = 0; i < count; i++) {
      encoded[i] = AlpEncoderDecoder.encodeDouble(input[i], exponent, factor);
    }

    int numExceptions = 0;
    short[] exceptionPositions = new short[count];
    for (int i = 0; i < count; i++) {
      double decoded = AlpEncoderDecoder.decodeDouble(encoded[i], exponent, factor);
      if (Double.doubleToRawLongBits(decoded) != Double.doubleToRawLongBits(input[i])) {
        exceptionPositions[numExceptions++] = (short) i;
      }
    }

    long placeholder = 0;
    int exceptionIndex = 0;
    for (int i = 0; i < count; i++) {
      if (exceptionIndex < numExceptions && (exceptionPositions[exceptionIndex] & 0xFFFF) == i) {
        exceptionIndex++;
      } else {
        placeholder = encoded[i];
        break;
      }
    }

    double[] exceptionValues = new double[numExceptions];
    for (int i = 0; i < numExceptions; i++) {
      int position = exceptionPositions[i] & 0xFFFF;
      exceptionValues[i] = input[position];
      encoded[position] = placeholder;
    }

    long min = encoded[0];
    long max = encoded[0];
    for (int i = 1; i < count; i++) {
      if (encoded[i] < min) min = encoded[i];
      if (encoded[i] > max) max = encoded[i];
    }
    for (int i = 0; i < count; i++) {
      encoded[i] -= min;
    }
    long maxDelta = max - min;

    int bitWidth = AlpEncoderDecoder.bitWidthForLong(maxDelta);
    int packedSize = AlpEncoderDecoder.bitPackedSize(count, bitWidth);
    byte[] packedValues = new byte[packedSize];
    if (bitWidth > 0) {
      packLongs(encoded, count, bitWidth, packedValues);
    }

    DoubleCompressedVector result = new DoubleCompressedVector();
    result.exponent = exponent;
    result.factor = factor;
    result.numExceptions = numExceptions;
    result.frameOfReference = min;
    result.bitWidth = bitWidth;
    result.numElements = count;
    result.packedValues = packedValues;
    result.exceptionPositions = Arrays.copyOf(exceptionPositions, numExceptions);
    result.exceptionValues = exceptionValues;
    return result;
  }

  static void decompressDoubleVector(DoubleCompressedVector v, double[] output) {
    long[] encoded = new long[v.numElements];
    decompressDoubleVector(v, output, encoded);
  }

  static void decompressDoubleVector(DoubleCompressedVector v, double[] output, long[] encodedBuffer) {
    if (v.bitWidth > 0) {
      unpackLongs(v.packedValues, v.numElements, v.bitWidth, encodedBuffer);
    } else {
      for (int i = 0; i < v.numElements; i++) {
        encodedBuffer[i] = 0;
      }
    }

    // Separate multiplications preserve the intermediate rounding required by the wire format.
    final long frameOfReference = v.frameOfReference;
    final double factorMul = DOUBLE_POW10[v.factor];
    final double expMul = DOUBLE_POW10_NEGATIVE[v.exponent];
    final int numElements = v.numElements;
    for (int i = 0; i < numElements; i++) {
      output[i] = (double) (encodedBuffer[i] + frameOfReference) * factorMul * expMul;
    }

    for (int i = 0; i < v.numExceptions; i++) {
      output[v.exceptionPositions[i] & 0xFFFF] = v.exceptionValues[i];
    }
  }

  @SuppressWarnings("deprecation")
  static void packInts(int[] values, int count, int bitWidth, byte[] output) {
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int fullGroups = count / 8;
    for (int g = 0; g < fullGroups; g++) {
      packer.pack8Values(values, g * 8, output, g * bitWidth);
    }
    int remaining = count % 8;
    if (remaining > 0) {
      int[] padded = new int[8];
      System.arraycopy(values, fullGroups * 8, padded, 0, remaining);
      byte[] tmp = new byte[bitWidth];
      packer.pack8Values(padded, 0, tmp, 0);
      int tailBytes = AlpEncoderDecoder.bitPackedSize(count, bitWidth) - fullGroups * bitWidth;
      System.arraycopy(tmp, 0, output, fullGroups * bitWidth, tailBytes);
    }
  }

  @SuppressWarnings("deprecation")
  static void unpackInts(byte[] packed, int count, int bitWidth, int[] output) {
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int fullGroups = count / 8;
    for (int g = 0; g < fullGroups; g++) {
      packer.unpack8Values(packed, g * bitWidth, output, g * 8);
    }
    int remaining = count % 8;
    if (remaining > 0) {
      byte[] tmp = new byte[bitWidth];
      int available = packed.length - fullGroups * bitWidth;
      System.arraycopy(packed, fullGroups * bitWidth, tmp, 0, Math.min(available, bitWidth));
      int[] padded = new int[8];
      packer.unpack8Values(tmp, 0, padded, 0);
      System.arraycopy(padded, 0, output, fullGroups * 8, remaining);
    }
  }

  @SuppressWarnings("deprecation")
  static void packLongs(long[] values, int count, int bitWidth, byte[] output) {
    BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
    int fullGroups = count / 8;
    for (int g = 0; g < fullGroups; g++) {
      packer.pack8Values(values, g * 8, output, g * bitWidth);
    }
    int remaining = count % 8;
    if (remaining > 0) {
      long[] padded = new long[8];
      System.arraycopy(values, fullGroups * 8, padded, 0, remaining);
      byte[] tmp = new byte[bitWidth];
      packer.pack8Values(padded, 0, tmp, 0);
      int tailBytes = AlpEncoderDecoder.bitPackedSize(count, bitWidth) - fullGroups * bitWidth;
      System.arraycopy(tmp, 0, output, fullGroups * bitWidth, tailBytes);
    }
  }

  static void unpackLongs(byte[] packed, int count, int bitWidth, long[] output) {
    BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);

    // Decode 32 values per iteration to amortize unpacker calls.
    int fullGroups32 = count / 32;
    for (int g = 0; g < fullGroups32; g++) {
      packer.unpack32Values(packed, g * bitWidth * 4, output, g * 32);
    }

    int processed = fullGroups32 * 32;
    int byteOffset = fullGroups32 * bitWidth * 4;
    int remaining8 = (count - processed) / 8;
    for (int g = 0; g < remaining8; g++) {
      packer.unpack8Values(packed, byteOffset + g * bitWidth, output, processed + g * 8);
    }

    int tailStart = processed + remaining8 * 8;
    int tailCount = count - tailStart;
    if (tailCount > 0) {
      int tailByteOffset = byteOffset + remaining8 * bitWidth;
      byte[] tmp = new byte[bitWidth];
      int available = packed.length - tailByteOffset;
      System.arraycopy(packed, tailByteOffset, tmp, 0, Math.min(available, bitWidth));
      long[] padded = new long[8];
      packer.unpack8Values(tmp, 0, padded, 0);
      System.arraycopy(padded, 0, output, tailStart, tailCount);
    }
  }
}
