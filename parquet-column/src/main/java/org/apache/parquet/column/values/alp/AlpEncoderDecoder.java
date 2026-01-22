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
 * then applying Frame of Reference (FOR) encoding and bit-packing.
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
public final class AlpEncoderDecoder {

  private AlpEncoderDecoder() {
    // Utility class
  }

  // ========== Float Encoding/Decoding ==========

  /**
   * Check if a float value is an exception (cannot be losslessly encoded).
   *
   * @param value the float value to check
   * @return true if the value is an exception
   */
  public static boolean isFloatException(float value) {
    // NaN check
    if (Float.isNaN(value)) {
      return true;
    }
    // Infinity check
    if (Float.isInfinite(value)) {
      return true;
    }
    // Negative zero check
    if (Float.floatToRawIntBits(value) == FLOAT_NEGATIVE_ZERO_BITS) {
      return true;
    }
    return false;
  }

  /**
   * Check if a float value will be an exception for the given exponent/factor.
   *
   * @param value    the float value
   * @param exponent the decimal exponent (0-10)
   * @param factor   the decimal factor (0 <= factor <= exponent)
   * @return true if the value is an exception for this encoding
   */
  public static boolean isFloatException(float value, int exponent, int factor) {
    if (isFloatException(value)) {
      return true;
    }

    // Try encoding and check for round-trip failure
    float multiplier = FLOAT_POW10[exponent];
    if (factor > 0) {
      multiplier /= FLOAT_POW10[factor];
    }

    float scaled = value * multiplier;

    // Check for overflow
    if (scaled > Integer.MAX_VALUE || scaled < Integer.MIN_VALUE) {
      return true;
    }

    // Fast round
    int encoded = fastRoundFloat(scaled);

    // Check round-trip
    float decoded = encoded / multiplier;
    return Float.floatToRawIntBits(value) != Float.floatToRawIntBits(decoded);
  }

  /**
   * Encode a float value to an integer using the specified exponent and factor.
   *
   * <p>Formula: encoded = round(value * 10^(exponent - factor))
   *
   * @param value    the float value to encode
   * @param exponent the decimal exponent (0-10)
   * @param factor   the decimal factor (0 <= factor <= exponent)
   * @return the encoded integer value
   */
  public static int encodeFloat(float value, int exponent, int factor) {
    float multiplier = FLOAT_POW10[exponent];
    if (factor > 0) {
      multiplier /= FLOAT_POW10[factor];
    }
    return fastRoundFloat(value * multiplier);
  }

  /**
   * Decode an integer back to a float using the specified exponent and factor.
   *
   * <p>Formula: value = encoded / 10^(exponent - factor)
   *
   * @param encoded  the encoded integer value
   * @param exponent the decimal exponent (0-10)
   * @param factor   the decimal factor (0 <= factor <= exponent)
   * @return the decoded float value
   */
  public static float decodeFloat(int encoded, int exponent, int factor) {
    float multiplier = FLOAT_POW10[exponent];
    if (factor > 0) {
      multiplier /= FLOAT_POW10[factor];
    }
    return encoded / multiplier;
  }

  /**
   * Fast rounding for float values using the magic number technique.
   *
   * @param value the float value to round
   * @return the rounded integer value
   */
  public static int fastRoundFloat(float value) {
    if (value >= 0) {
      return (int) ((value + MAGIC_FLOAT) - MAGIC_FLOAT);
    } else {
      return (int) ((value - MAGIC_FLOAT) + MAGIC_FLOAT);
    }
  }

  // ========== Double Encoding/Decoding ==========

  /**
   * Check if a double value is an exception (cannot be losslessly encoded).
   *
   * @param value the double value to check
   * @return true if the value is an exception
   */
  public static boolean isDoubleException(double value) {
    // NaN check
    if (Double.isNaN(value)) {
      return true;
    }
    // Infinity check
    if (Double.isInfinite(value)) {
      return true;
    }
    // Negative zero check
    if (Double.doubleToRawLongBits(value) == DOUBLE_NEGATIVE_ZERO_BITS) {
      return true;
    }
    return false;
  }

  /**
   * Check if a double value will be an exception for the given exponent/factor.
   *
   * @param value    the double value
   * @param exponent the decimal exponent (0-18)
   * @param factor   the decimal factor (0 <= factor <= exponent)
   * @return true if the value is an exception for this encoding
   */
  public static boolean isDoubleException(double value, int exponent, int factor) {
    if (isDoubleException(value)) {
      return true;
    }

    // Try encoding and check for round-trip failure
    double multiplier = DOUBLE_POW10[exponent];
    if (factor > 0) {
      multiplier /= DOUBLE_POW10[factor];
    }

    double scaled = value * multiplier;

    // Check for overflow
    if (scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
      return true;
    }

    // Fast round
    long encoded = fastRoundDouble(scaled);

    // Check round-trip
    double decoded = encoded / multiplier;
    return Double.doubleToRawLongBits(value) != Double.doubleToRawLongBits(decoded);
  }

  /**
   * Encode a double value to a long using the specified exponent and factor.
   *
   * <p>Formula: encoded = round(value * 10^(exponent - factor))
   *
   * @param value    the double value to encode
   * @param exponent the decimal exponent (0-18)
   * @param factor   the decimal factor (0 <= factor <= exponent)
   * @return the encoded long value
   */
  public static long encodeDouble(double value, int exponent, int factor) {
    double multiplier = DOUBLE_POW10[exponent];
    if (factor > 0) {
      multiplier /= DOUBLE_POW10[factor];
    }
    return fastRoundDouble(value * multiplier);
  }

  /**
   * Decode a long back to a double using the specified exponent and factor.
   *
   * <p>Formula: value = encoded / 10^(exponent - factor)
   *
   * @param encoded  the encoded long value
   * @param exponent the decimal exponent (0-18)
   * @param factor   the decimal factor (0 <= factor <= exponent)
   * @return the decoded double value
   */
  public static double decodeDouble(long encoded, int exponent, int factor) {
    double multiplier = DOUBLE_POW10[exponent];
    if (factor > 0) {
      multiplier /= DOUBLE_POW10[factor];
    }
    return encoded / multiplier;
  }

  /**
   * Fast rounding for double values using the magic number technique.
   *
   * @param value the double value to round
   * @return the rounded long value
   */
  public static long fastRoundDouble(double value) {
    if (value >= 0) {
      return (long) ((value + MAGIC_DOUBLE) - MAGIC_DOUBLE);
    } else {
      return (long) ((value - MAGIC_DOUBLE) + MAGIC_DOUBLE);
    }
  }

  // ========== Bit Width Calculation ==========

  /**
   * Calculate the bit width needed to store unsigned values up to maxDelta.
   *
   * @param maxDelta the maximum delta value (unsigned)
   * @return the number of bits needed (0-32 for int, 0-64 for long)
   */
  public static int bitWidth(int maxDelta) {
    if (maxDelta == 0) {
      return 0;
    }
    return 32 - Integer.numberOfLeadingZeros(maxDelta);
  }

  /**
   * Calculate the bit width needed to store unsigned values up to maxDelta.
   *
   * @param maxDelta the maximum delta value (unsigned)
   * @return the number of bits needed (0-64)
   */
  public static int bitWidth(long maxDelta) {
    if (maxDelta == 0) {
      return 0;
    }
    return 64 - Long.numberOfLeadingZeros(maxDelta);
  }

  // ========== Best Exponent/Factor Selection ==========

  /**
   * Result of finding the best exponent/factor combination.
   */
  public static class EncodingParams {
    public final int exponent;
    public final int factor;
    public final int numExceptions;

    public EncodingParams(int exponent, int factor, int numExceptions) {
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
  public static EncodingParams findBestFloatParams(float[] values, int offset, int length) {
    int bestExponent = 0;
    int bestFactor = 0;
    int bestExceptions = length; // Start with worst case

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
          // Early exit if we found perfect encoding
          if (bestExceptions == 0) {
            return new EncodingParams(bestExponent, bestFactor, bestExceptions);
          }
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
  public static EncodingParams findBestDoubleParams(double[] values, int offset, int length) {
    int bestExponent = 0;
    int bestFactor = 0;
    int bestExceptions = length; // Start with worst case

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
          // Early exit if we found perfect encoding
          if (bestExceptions == 0) {
            return new EncodingParams(bestExponent, bestFactor, bestExceptions);
          }
        }
      }
    }
    return new EncodingParams(bestExponent, bestFactor, bestExceptions);
  }
}
