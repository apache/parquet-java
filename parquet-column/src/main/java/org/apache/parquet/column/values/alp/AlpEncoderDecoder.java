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
 * <p>Encoding formula: encoded = round(value * 10^(exponent - factor))
 * <p>Decoding formula: value = encoded / 10^(exponent - factor)
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

  // ========== Multiplier Computation ==========

  /**
   * Compute the float multiplier for the given exponent and factor.
   *
   * @param exponent the decimal exponent (0-10)
   * @param factor   the decimal factor (0 &lt;= factor &lt;= exponent)
   * @return the multiplier: 10^exponent / 10^factor
   */
  static float getFloatMultiplier(int exponent, int factor) {
    float multiplier = FLOAT_POW10[exponent];
    if (factor > 0) {
      multiplier /= FLOAT_POW10[factor];
    }
    return multiplier;
  }

  /**
   * Compute the double multiplier for the given exponent and factor.
   *
   * @param exponent the decimal exponent (0-18)
   * @param factor   the decimal factor (0 &lt;= factor &lt;= exponent)
   * @return the multiplier: 10^exponent / 10^factor
   */
  static double getDoubleMultiplier(int exponent, int factor) {
    double multiplier = DOUBLE_POW10[exponent];
    if (factor > 0) {
      multiplier /= DOUBLE_POW10[factor];
    }
    return multiplier;
  }

  // ========== Float Encoding/Decoding ==========

  /**
   * Check if a float value is an unconditional exception (cannot be encoded regardless of params).
   *
   * @param value the float value to check
   * @return true if the value is NaN, Infinite, or negative zero
   */
  static boolean isFloatException(float value) {
    if (Float.isNaN(value)) {
      return true;
    }
    if (Float.isInfinite(value)) {
      return true;
    }
    return Float.floatToRawIntBits(value) == FLOAT_NEGATIVE_ZERO_BITS;
  }

  /**
   * Check if a float value will be an exception for the given exponent/factor.
   * Uses encodeFloat/decodeFloat internally to verify round-trip.
   *
   * @param value    the float value
   * @param exponent the decimal exponent (0-10)
   * @param factor   the decimal factor (0 &lt;= factor &lt;= exponent)
   * @return true if the value is an exception for this encoding
   */
  static boolean isFloatException(float value, int exponent, int factor) {
    if (isFloatException(value)) {
      return true;
    }
    float multiplier = getFloatMultiplier(exponent, factor);
    float scaled = value * multiplier;
    if (scaled > Integer.MAX_VALUE || scaled < Integer.MIN_VALUE) {
      return true;
    }
    int encoded = encodeFloat(value, exponent, factor);
    float decoded = decodeFloat(encoded, exponent, factor);
    return Float.floatToRawIntBits(value) != Float.floatToRawIntBits(decoded);
  }

  /**
   * Encode a float value to an integer using the specified exponent and factor.
   *
   * <p>Formula: encoded = round(value * 10^(exponent - factor))
   *
   * @param value    the float value to encode
   * @param exponent the decimal exponent (0-10)
   * @param factor   the decimal factor (0 &lt;= factor &lt;= exponent)
   * @return the encoded integer value
   */
  static int encodeFloat(float value, int exponent, int factor) {
    return fastRoundFloat(value * getFloatMultiplier(exponent, factor));
  }

  /**
   * Decode an integer back to a float using the specified exponent and factor.
   *
   * <p>Formula: value = encoded / 10^(exponent - factor)
   *
   * @param encoded  the encoded integer value
   * @param exponent the decimal exponent (0-10)
   * @param factor   the decimal factor (0 &lt;= factor &lt;= exponent)
   * @return the decoded float value
   */
  static float decodeFloat(int encoded, int exponent, int factor) {
    return encoded / getFloatMultiplier(exponent, factor);
  }

  /**
   * Fast rounding for float values using the magic number technique.
   *
   * @param value the float value to round
   * @return the rounded integer value
   */
  static int fastRoundFloat(float value) {
    if (value >= 0) {
      return (int) ((value + MAGIC_FLOAT) - MAGIC_FLOAT);
    } else {
      return (int) ((value - MAGIC_FLOAT) + MAGIC_FLOAT);
    }
  }

  // ========== Double Encoding/Decoding ==========

  /**
   * Check if a double value is an unconditional exception (cannot be encoded regardless of params).
   *
   * @param value the double value to check
   * @return true if the value is NaN, Infinite, or negative zero
   */
  static boolean isDoubleException(double value) {
    if (Double.isNaN(value)) {
      return true;
    }
    if (Double.isInfinite(value)) {
      return true;
    }
    return Double.doubleToRawLongBits(value) == DOUBLE_NEGATIVE_ZERO_BITS;
  }

  /**
   * Check if a double value will be an exception for the given exponent/factor.
   * Uses encodeDouble/decodeDouble internally to verify round-trip.
   *
   * @param value    the double value
   * @param exponent the decimal exponent (0-18)
   * @param factor   the decimal factor (0 &lt;= factor &lt;= exponent)
   * @return true if the value is an exception for this encoding
   */
  static boolean isDoubleException(double value, int exponent, int factor) {
    if (isDoubleException(value)) {
      return true;
    }
    double multiplier = getDoubleMultiplier(exponent, factor);
    double scaled = value * multiplier;
    if (scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
      return true;
    }
    long encoded = encodeDouble(value, exponent, factor);
    double decoded = decodeDouble(encoded, exponent, factor);
    return Double.doubleToRawLongBits(value) != Double.doubleToRawLongBits(decoded);
  }

  /**
   * Encode a double value to a long using the specified exponent and factor.
   *
   * <p>Formula: encoded = round(value * 10^(exponent - factor))
   *
   * @param value    the double value to encode
   * @param exponent the decimal exponent (0-18)
   * @param factor   the decimal factor (0 &lt;= factor &lt;= exponent)
   * @return the encoded long value
   */
  static long encodeDouble(double value, int exponent, int factor) {
    return fastRoundDouble(value * getDoubleMultiplier(exponent, factor));
  }

  /**
   * Decode a long back to a double using the specified exponent and factor.
   *
   * <p>Formula: value = encoded / 10^(exponent - factor)
   *
   * @param encoded  the encoded long value
   * @param exponent the decimal exponent (0-18)
   * @param factor   the decimal factor (0 &lt;= factor &lt;= exponent)
   * @return the decoded double value
   */
  static double decodeDouble(long encoded, int exponent, int factor) {
    return encoded / getDoubleMultiplier(exponent, factor);
  }

  /**
   * Fast rounding for double values using the magic number technique.
   *
   * @param value the double value to round
   * @return the rounded long value
   */
  static long fastRoundDouble(double value) {
    if (value >= 0) {
      return (long) ((value + MAGIC_DOUBLE) - MAGIC_DOUBLE);
    } else {
      return (long) ((value - MAGIC_DOUBLE) + MAGIC_DOUBLE);
    }
  }

  // ========== Bit Width Calculation ==========

  /**
   * Calculate the bit width needed to store unsigned int values up to maxDelta.
   *
   * @param maxDelta the maximum delta value (unsigned)
   * @return the number of bits needed (0 to {@link Integer#SIZE})
   */
  static int bitWidthForInt(int maxDelta) {
    if (maxDelta == 0) {
      return 0;
    }
    return Integer.SIZE - Integer.numberOfLeadingZeros(maxDelta);
  }

  /**
   * Calculate the bit width needed to store unsigned long values up to maxDelta.
   *
   * @param maxDelta the maximum delta value (unsigned)
   * @return the number of bits needed (0 to {@link Long#SIZE})
   */
  static int bitWidthForLong(long maxDelta) {
    if (maxDelta == 0) {
      return 0;
    }
    return Long.SIZE - Long.numberOfLeadingZeros(maxDelta);
  }

  // ========== Best Exponent/Factor Selection ==========

  /**
   * Result of finding the best exponent/factor combination.
   */
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
   * Find the best exponent/factor combination for encoding float values.
   *
   * <p>Tries all valid (exponent, factor) combinations and selects the one
   * that minimizes the number of exceptions.
   *
   * @param values the float values to analyze
   * @param offset the starting index in the array
   * @param length the number of values to analyze
   * @return the best encoding parameters
   */
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

  /**
   * Find the best exponent/factor from a preset list of combinations for float values.
   *
   * @param values  the float values to analyze
   * @param offset  the starting index in the array
   * @param length  the number of values to analyze
   * @param presets array of [exponent, factor] pairs to try
   * @return the best encoding parameters from the presets
   */
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

  /**
   * Find the best exponent/factor combination for encoding double values.
   *
   * <p>Tries all valid (exponent, factor) combinations and selects the one
   * that minimizes the number of exceptions.
   *
   * @param values the double values to analyze
   * @param offset the starting index in the array
   * @param length the number of values to analyze
   * @return the best encoding parameters
   */
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

  /**
   * Find the best exponent/factor from a preset list of combinations for double values.
   *
   * @param values  the double values to analyze
   * @param offset  the starting index in the array
   * @param length  the number of values to analyze
   * @param presets array of [exponent, factor] pairs to try
   * @return the best encoding parameters from the presets
   */
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
