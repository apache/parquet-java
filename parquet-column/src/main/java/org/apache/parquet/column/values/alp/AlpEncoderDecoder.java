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
 * <p>Encoding formula: encoded = round(value * 10^exponent * 10^(-factor))
 * <p>Decoding formula: value = encoded * 10^factor * 10^(-exponent)
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

  private AlpEncoderDecoder() {
    // Utility class
  }

  // ========== Float exception detection ==========

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
    float scaled = value * FLOAT_POW10[exponent] * FLOAT_POW10_NEGATIVE[factor];
    if (scaled > FLOAT_ENCODING_UPPER_LIMIT || scaled < FLOAT_ENCODING_LOWER_LIMIT) {
      return true;
    }
    int encoded = encodeFloat(value, exponent, factor);
    float decoded = decodeFloat(encoded, exponent, factor);
    return Float.floatToRawIntBits(value) != Float.floatToRawIntBits(decoded);
  }

  // ========== Float encode/decode ==========
  // Two-step multiplication matching C++ to produce identical floating-point rounding.
  // C++ encode: value * 10^exponent * 10^(-factor)
  // C++ decode: (float)encoded * 10^factor * 10^(-exponent)

  /** Encode: round(value * 10^exponent * 10^(-factor)) */
  static int encodeFloat(float value, int exponent, int factor) {
    return fastRoundFloat(value * FLOAT_POW10[exponent] * FLOAT_POW10_NEGATIVE[factor]);
  }

  /** Decode: encoded * 10^factor * 10^(-exponent) */
  static float decodeFloat(int encoded, int exponent, int factor) {
    return (float) encoded * FLOAT_POW10[factor] * FLOAT_POW10_NEGATIVE[exponent];
  }

  // Uses the 2^22+2^23 magic-number trick to round without branching on the FPU.
  static int fastRoundFloat(float value) {
    if (value >= 0) {
      return (int) ((value + MAGIC_FLOAT) - MAGIC_FLOAT);
    } else {
      return (int) ((value - MAGIC_FLOAT) + MAGIC_FLOAT);
    }
  }

  // ========== Double exception detection ==========

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
    double scaled = value * DOUBLE_POW10[exponent] * DOUBLE_POW10_NEGATIVE[factor];
    if (scaled > DOUBLE_ENCODING_UPPER_LIMIT || scaled < DOUBLE_ENCODING_LOWER_LIMIT) {
      return true;
    }
    long encoded = encodeDouble(value, exponent, factor);
    double decoded = decodeDouble(encoded, exponent, factor);
    return Double.doubleToRawLongBits(value) != Double.doubleToRawLongBits(decoded);
  }

  // ========== Double encode/decode ==========
  // Two-step multiplication matching C++ to produce identical floating-point rounding.

  /** Encode: round(value * 10^exponent * 10^(-factor)) */
  static long encodeDouble(double value, int exponent, int factor) {
    return fastRoundDouble(value * DOUBLE_POW10[exponent] * DOUBLE_POW10_NEGATIVE[factor]);
  }

  /** Decode: encoded * 10^factor * 10^(-exponent) */
  static double decodeDouble(long encoded, int exponent, int factor) {
    return (double) encoded * DOUBLE_POW10[factor] * DOUBLE_POW10_NEGATIVE[exponent];
  }

  // Same trick but with 2^51+2^52 for double precision.
  static long fastRoundDouble(double value) {
    if (value >= 0) {
      return (long) ((value + MAGIC_DOUBLE) - MAGIC_DOUBLE);
    } else {
      return (long) ((value - MAGIC_DOUBLE) + MAGIC_DOUBLE);
    }
  }

  // ========== Bit width ==========

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

  /** Packed data size in bytes: ceil(numElements * bitWidth / 8). */
  static int bitPackedSize(int numElements, int bitWidth) {
    return (numElements * bitWidth + 7) / 8;
  }

  // ========== Encoding params ==========

  static class EncodingParams {
    final int exponent;
    final int factor;
    final int numExceptions;

    EncodingParams(int exponent, int factor, int numExceptions) {
      this.exponent = exponent;
      this.factor = factor;
      this.numExceptions = numExceptions;
    }
  }

  /** Try all (exponent, factor) combos and pick the one with fewest exceptions. */
  static EncodingParams findBestFloatParams(float[] values, int offset, int length) {
    int bestExponent = 0;
    int bestFactor = 0;
    int bestExceptions = length;

    for (int e = 0; e <= FLOAT_MAX_EXPONENT; e++) {
      for (int f = 0; f <= e; f++) {
        int exceptions = 0;
        for (int i = 0; i < length; i++) {
          if (isFloatException(values[offset + i], e, f)) {
            exceptions++;
          }
        }
        if (exceptions < bestExceptions) {
          bestExponent = e;
          bestFactor = f;
          bestExceptions = exceptions;
          if (bestExceptions == 0) {
            return new EncodingParams(bestExponent, bestFactor, bestExceptions);
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

    for (int[] preset : presets) {
      int e = preset[0];
      int f = preset[1];
      int exceptions = 0;
      for (int i = 0; i < length; i++) {
        if (isFloatException(values[offset + i], e, f)) {
          exceptions++;
        }
      }
      if (exceptions < bestExceptions) {
        bestExponent = e;
        bestFactor = f;
        bestExceptions = exceptions;
        if (bestExceptions == 0) {
          return new EncodingParams(bestExponent, bestFactor, bestExceptions);
        }
      }
    }
    return new EncodingParams(bestExponent, bestFactor, bestExceptions);
  }

  static EncodingParams findBestDoubleParams(double[] values, int offset, int length) {
    int bestExponent = 0;
    int bestFactor = 0;
    int bestExceptions = length;

    for (int e = 0; e <= DOUBLE_MAX_EXPONENT; e++) {
      for (int f = 0; f <= e; f++) {
        int exceptions = 0;
        for (int i = 0; i < length; i++) {
          if (isDoubleException(values[offset + i], e, f)) {
            exceptions++;
          }
        }
        if (exceptions < bestExceptions) {
          bestExponent = e;
          bestFactor = f;
          bestExceptions = exceptions;
          if (bestExceptions == 0) {
            return new EncodingParams(bestExponent, bestFactor, bestExceptions);
          }
        }
      }
    }
    return new EncodingParams(bestExponent, bestFactor, bestExceptions);
  }

  static EncodingParams findBestDoubleParamsWithPresets(double[] values, int offset, int length, int[][] presets) {
    int bestExponent = presets[0][0];
    int bestFactor = presets[0][1];
    int bestExceptions = length;

    for (int[] preset : presets) {
      int e = preset[0];
      int f = preset[1];
      int exceptions = 0;
      for (int i = 0; i < length; i++) {
        if (isDoubleException(values[offset + i], e, f)) {
          exceptions++;
        }
      }
      if (exceptions < bestExceptions) {
        bestExponent = e;
        bestFactor = f;
        bestExceptions = exceptions;
        if (bestExceptions == 0) {
          return new EncodingParams(bestExponent, bestFactor, bestExceptions);
        }
      }
    }
    return new EncodingParams(bestExponent, bestFactor, bestExceptions);
  }
}
