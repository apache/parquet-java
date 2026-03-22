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

/**
 * Core ALP (Adaptive Lossless floating-Point) encoding and decoding logic.
 *
 * <p>ALP works by converting floating-point values to integers using decimal scaling,
 * then applying Frame of Reference encoding and bit-packing.
 * Values that cannot be losslessly converted are stored as exceptions.
 *
 * <p>Encoding formula: encoded = fastRound(value * POW10[e] * POW10_NEGATIVE[f])
 * <p>Decoding formula: value = encoded * POW10[f] * POW10_NEGATIVE[e]
 *
 * <p>The order of operations is critical for IEEE 754 correctness. Both formulas must
 * be evaluated as single expressions — storing the intermediate multiplication result
 * in a variable before the second multiply changes IEEE 754 rounding and produces extra
 * exceptions. Uses multiply-by-reciprocal (via POW10_NEGATIVE) for C++ wire compatibility.
 *
 * <p>Exception conditions:
 * <ul>
 *   <li>NaN values</li>
 *   <li>Infinity values</li>
 *   <li>Negative zero (-0.0)</li>
 *   <li>Out of integer range</li>
 *   <li>Round-trip failure (decode(encode(v)) != v)</li>
 * </ul>
 */
final class AlpEncoderDecoder {

  private static final double ENCODING_UPPER_LIMIT = 9223372036854774784.0;
  private static final double ENCODING_LOWER_LIMIT = -9223372036854774784.0;
  private static final float FLOAT_ENCODING_UPPER_LIMIT = 2147483520.0f;
  private static final float FLOAT_ENCODING_LOWER_LIMIT = -2147483520.0f;

  private AlpEncoderDecoder() {
    // Utility class
  }

  /** NaN, Inf, and -0.0 can never be encoded regardless of exponent/factor. */
  static boolean isFloatException(float value) {
    if (Float.isNaN(value)) {
      return true;
    }
    if (Float.isInfinite(value)) {
      return true;
    }
    return Float.floatToRawIntBits(value) == FLOAT_NEGATIVE_ZERO_BITS;
  }

  /** Check round-trip: encode then decode, and see if we get the same bits back. */
  static boolean isFloatException(float value, int exponent, int factor) {
    if (isFloatException(value)) {
      return true;
    }
    // Check before rounding: overflow or non-finite after scaling
    float scaled = value * FLOAT_POW10[exponent] * FLOAT_POW10_NEGATIVE[factor];
    if (!Float.isFinite(scaled) || scaled > FLOAT_ENCODING_UPPER_LIMIT || scaled < FLOAT_ENCODING_LOWER_LIMIT) {
      return true;
    }
    int encoded = encodeFloat(value, exponent, factor);
    float decoded = decodeFloat(encoded, exponent, factor);
    return Float.floatToRawIntBits(value) != Float.floatToRawIntBits(decoded);
  }

  /** Round float to nearest integer using magic-number trick with sign branching. */
  static int fastRoundFloat(float value) {
    if (value >= 0) {
      return (int) ((value + MAGIC_FLOAT) - MAGIC_FLOAT);
    } else {
      return (int) ((value - MAGIC_FLOAT) + MAGIC_FLOAT);
    }
  }

  /** Encode: fastRound(value * POW10[e] * POW10_NEGATIVE[f]) — single expression. */
  static int encodeFloat(float value, int exponent, int factor) {
    return fastRoundFloat(value * FLOAT_POW10[exponent] * FLOAT_POW10_NEGATIVE[factor]);
  }

  /** Decode: encoded * POW10[f] * POW10_NEGATIVE[e] — single expression. */
  static float decodeFloat(int encoded, int exponent, int factor) {
    return encoded * FLOAT_POW10[factor] * FLOAT_POW10_NEGATIVE[exponent];
  }

  static boolean isDoubleException(double value) {
    if (Double.isNaN(value)) {
      return true;
    }
    if (Double.isInfinite(value)) {
      return true;
    }
    return Double.doubleToRawLongBits(value) == DOUBLE_NEGATIVE_ZERO_BITS;
  }

  static boolean isDoubleException(double value, int exponent, int factor) {
    if (isDoubleException(value)) {
      return true;
    }
    // Check before rounding: overflow or non-finite after scaling
    double scaled = value * DOUBLE_POW10[exponent] * DOUBLE_POW10_NEGATIVE[factor];
    if (!Double.isFinite(scaled) || scaled > ENCODING_UPPER_LIMIT || scaled < ENCODING_LOWER_LIMIT) {
      return true;
    }
    long encoded = encodeDouble(value, exponent, factor);
    double decoded = decodeDouble(encoded, exponent, factor);
    return Double.doubleToRawLongBits(value) != Double.doubleToRawLongBits(decoded);
  }

  /** Round double to nearest integer using magic-number trick with sign branching. */
  static long fastRoundDouble(double value) {
    if (value >= 0) {
      return (long) ((value + MAGIC_DOUBLE) - MAGIC_DOUBLE);
    } else {
      return (long) ((value - MAGIC_DOUBLE) + MAGIC_DOUBLE);
    }
  }

  /** Encode: fastRound(value * POW10[e] * POW10_NEGATIVE[f]) — single expression. */
  static long encodeDouble(double value, int exponent, int factor) {
    return fastRoundDouble(value * DOUBLE_POW10[exponent] * DOUBLE_POW10_NEGATIVE[factor]);
  }

  /** Decode: encoded * POW10[f] * POW10_NEGATIVE[e] — single expression. */
  static double decodeDouble(long encoded, int exponent, int factor) {
    return encoded * DOUBLE_POW10[factor] * DOUBLE_POW10_NEGATIVE[exponent];
  }

  /** Number of bits needed to represent maxDelta as an unsigned value. */
  static int bitWidthForInt(int maxDelta) {
    if (maxDelta == 0) {
      return 0;
    }
    return Integer.SIZE - Integer.numberOfLeadingZeros(maxDelta);
  }

  static int bitWidthForLong(long maxDelta) {
    if (maxDelta == 0) {
      return 0;
    }
    return Long.SIZE - Long.numberOfLeadingZeros(maxDelta);
  }

  public static class EncodingParams {
    public final int exponent;
    public final int factor;
    public final int numExceptions;

    EncodingParams(int exponent, int factor, int numExceptions) {
      this.exponent = exponent;
      this.factor = factor;
      this.numExceptions = numExceptions;
    }
  }

  /**
   * Try all (exponent, factor) combos and pick the one with the smallest estimated compressed size.
   *
   * <p>Estimated size (in bits) = {@code length * bitWidth + exceptions * (Float.SIZE + Short.SIZE)},
   * where bitWidth is the number of bits needed to represent the unsigned range of non-exception
   * encoded values after frame-of-reference subtraction. This matches the C++ ALP cost model and
   * produces better compression ratios than minimizing exception count alone.
   */
  static EncodingParams findBestFloatParams(float[] values, int offset, int length) {
    int bestExponent = 0;
    int bestFactor = 0;
    int bestExceptions = length;
    long bestEstimatedSize = Long.MAX_VALUE;

    for (int e = 0; e <= FLOAT_MAX_EXPONENT; e++) {
      for (int f = 0; f <= e; f++) {
        int exceptions = 0;
        int minEncoded = Integer.MAX_VALUE;
        int maxEncoded = Integer.MIN_VALUE;
        for (int i = 0; i < length; i++) {
          float value = values[offset + i];
          if (isFloatException(value, e, f)) {
            exceptions++;
          } else {
            int encoded = encodeFloat(value, e, f);
            if (encoded < minEncoded) minEncoded = encoded;
            if (encoded > maxEncoded) maxEncoded = encoded;
          }
        }
        int nonExceptions = length - exceptions;
        if (nonExceptions == 0) continue;
        long delta = (nonExceptions < 2) ? 0 :
            Integer.toUnsignedLong(maxEncoded) - Integer.toUnsignedLong(minEncoded);
        int bitsPerValue = (delta == 0) ? 0 : (64 - Long.numberOfLeadingZeros(delta));
        long estimatedSize = (long) length * bitsPerValue
            + (long) exceptions * (Float.SIZE + Short.SIZE);
        if (estimatedSize < bestEstimatedSize
            || (estimatedSize == bestEstimatedSize
                && (e > bestExponent || (e == bestExponent && f > bestFactor)))) {
          bestEstimatedSize = estimatedSize;
          bestExponent = e;
          bestFactor = f;
          bestExceptions = exceptions;
          if (bestExceptions == 0 && bitsPerValue == 0) {
            return new EncodingParams(bestExponent, bestFactor, 0);
          }
        }
      }
    }
    return new EncodingParams(bestExponent, bestFactor, bestExceptions);
  }

  /** Same as findBestFloatParams but only tries the cached preset combos. */
  static EncodingParams findBestFloatParamsWithPresets(float[] values, int offset, int length, int[][] presets) {
    int bestExponent = presets[0][0];
    int bestFactor = presets[0][1];
    int bestExceptions = length;
    long bestEstimatedSize = Long.MAX_VALUE;

    for (int[] preset : presets) {
      int e = preset[0];
      int f = preset[1];
      int exceptions = 0;
      int minEncoded = Integer.MAX_VALUE;
      int maxEncoded = Integer.MIN_VALUE;
      for (int i = 0; i < length; i++) {
        float value = values[offset + i];
        if (isFloatException(value, e, f)) {
          exceptions++;
        } else {
          int encoded = encodeFloat(value, e, f);
          if (encoded < minEncoded) minEncoded = encoded;
          if (encoded > maxEncoded) maxEncoded = encoded;
        }
      }
      int nonExceptions = length - exceptions;
      if (nonExceptions == 0) continue;
      long delta = (nonExceptions < 2) ? 0 :
          Integer.toUnsignedLong(maxEncoded) - Integer.toUnsignedLong(minEncoded);
      int bitsPerValue = (delta == 0) ? 0 : (64 - Long.numberOfLeadingZeros(delta));
      long estimatedSize = (long) length * bitsPerValue
          + (long) exceptions * (Float.SIZE + Short.SIZE);
      if (estimatedSize < bestEstimatedSize
          || (estimatedSize == bestEstimatedSize
              && (e > bestExponent || (e == bestExponent && f > bestFactor)))) {
        bestEstimatedSize = estimatedSize;
        bestExponent = e;
        bestFactor = f;
        bestExceptions = exceptions;
        if (bestExceptions == 0 && bitsPerValue == 0) {
          return new EncodingParams(bestExponent, bestFactor, 0);
        }
      }
    }
    return new EncodingParams(bestExponent, bestFactor, bestExceptions);
  }

  /** Try all (exponent, factor) combos and pick the one with the smallest estimated compressed size. */
  static EncodingParams findBestDoubleParams(double[] values, int offset, int length) {
    int bestExponent = 0;
    int bestFactor = 0;
    int bestExceptions = length;
    long bestEstimatedSize = Long.MAX_VALUE;

    for (int e = 0; e <= DOUBLE_MAX_EXPONENT; e++) {
      for (int f = 0; f <= e; f++) {
        int exceptions = 0;
        long minEncoded = Long.MAX_VALUE;
        long maxEncoded = Long.MIN_VALUE;
        for (int i = 0; i < length; i++) {
          double value = values[offset + i];
          if (isDoubleException(value, e, f)) {
            exceptions++;
          } else {
            long encoded = encodeDouble(value, e, f);
            if (encoded < minEncoded) minEncoded = encoded;
            if (encoded > maxEncoded) maxEncoded = encoded;
          }
        }
        int nonExceptions = length - exceptions;
        if (nonExceptions == 0) continue;
        // delta as signed subtraction; Long.numberOfLeadingZeros handles the unsigned bit width
        // correctly even when the subtraction overflows (large range → penalized with 64 bits).
        long delta = (nonExceptions < 2) ? 0 : (maxEncoded - minEncoded);
        int bitsPerValue = (delta == 0) ? 0 : (64 - Long.numberOfLeadingZeros(delta));
        long estimatedSize = (long) length * bitsPerValue
            + (long) exceptions * (Double.SIZE + Short.SIZE);
        if (estimatedSize < bestEstimatedSize
            || (estimatedSize == bestEstimatedSize
                && (e > bestExponent || (e == bestExponent && f > bestFactor)))) {
          bestEstimatedSize = estimatedSize;
          bestExponent = e;
          bestFactor = f;
          bestExceptions = exceptions;
          if (bestExceptions == 0 && bitsPerValue == 0) {
            return new EncodingParams(bestExponent, bestFactor, 0);
          }
        }
      }
    }
    return new EncodingParams(bestExponent, bestFactor, bestExceptions);
  }

  /** Same as findBestDoubleParams but only tries the cached preset combos. */
  static EncodingParams findBestDoubleParamsWithPresets(double[] values, int offset, int length, int[][] presets) {
    int bestExponent = presets[0][0];
    int bestFactor = presets[0][1];
    int bestExceptions = length;
    long bestEstimatedSize = Long.MAX_VALUE;

    for (int[] preset : presets) {
      int e = preset[0];
      int f = preset[1];
      int exceptions = 0;
      long minEncoded = Long.MAX_VALUE;
      long maxEncoded = Long.MIN_VALUE;
      for (int i = 0; i < length; i++) {
        double value = values[offset + i];
        if (isDoubleException(value, e, f)) {
          exceptions++;
        } else {
          long encoded = encodeDouble(value, e, f);
          if (encoded < minEncoded) minEncoded = encoded;
          if (encoded > maxEncoded) maxEncoded = encoded;
        }
      }
      int nonExceptions = length - exceptions;
      if (nonExceptions == 0) continue;
      long delta = (nonExceptions < 2) ? 0 : (maxEncoded - minEncoded);
      int bitsPerValue = (delta == 0) ? 0 : (64 - Long.numberOfLeadingZeros(delta));
      long estimatedSize = (long) length * bitsPerValue
          + (long) exceptions * (Double.SIZE + Short.SIZE);
      if (estimatedSize < bestEstimatedSize
          || (estimatedSize == bestEstimatedSize
              && (e > bestExponent || (e == bestExponent && f > bestFactor)))) {
        bestEstimatedSize = estimatedSize;
        bestExponent = e;
        bestFactor = f;
        bestExceptions = exceptions;
        if (bestExceptions == 0 && bitsPerValue == 0) {
          return new EncodingParams(bestExponent, bestFactor, 0);
        }
      }
    }
    return new EncodingParams(bestExponent, bestFactor, bestExceptions);
  }
}
