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

  static float getFloatMultiplier(int exponent, int factor) {
    float multiplier = FLOAT_POW10[exponent];
    if (factor > 0) {
      multiplier /= FLOAT_POW10[factor];
    }
    return multiplier;
  }

  static double getDoubleMultiplier(int exponent, int factor) {
    double multiplier = DOUBLE_POW10[exponent];
    if (factor > 0) {
      multiplier /= DOUBLE_POW10[factor];
    }
    return multiplier;
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
    float multiplier = getFloatMultiplier(exponent, factor);
    float scaled = value * multiplier;
    if (scaled > Integer.MAX_VALUE || scaled < Integer.MIN_VALUE) {
      return true;
    }
    int encoded = encodeFloat(value, exponent, factor);
    float decoded = decodeFloat(encoded, exponent, factor);
    return Float.floatToRawIntBits(value) != Float.floatToRawIntBits(decoded);
  }

  /** Encode: round(value * 10^exponent / 10^factor) */
  static int encodeFloat(float value, int exponent, int factor) {
    return fastRoundFloat(value * getFloatMultiplier(exponent, factor));
  }

  /** Decode: encoded / 10^exponent * 10^factor */
  static float decodeFloat(int encoded, int exponent, int factor) {
    return encoded / getFloatMultiplier(exponent, factor);
  }

  // Uses the 2^22+2^23 magic-number trick to round without branching on the FPU.
  static int fastRoundFloat(float value) {
    if (value >= 0) {
      return (int) ((value + MAGIC_FLOAT) - MAGIC_FLOAT);
    } else {
      return (int) ((value - MAGIC_FLOAT) + MAGIC_FLOAT);
    }
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
    double multiplier = getDoubleMultiplier(exponent, factor);
    double scaled = value * multiplier;
    if (scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
      return true;
    }
    long encoded = encodeDouble(value, exponent, factor);
    double decoded = decodeDouble(encoded, exponent, factor);
    return Double.doubleToRawLongBits(value) != Double.doubleToRawLongBits(decoded);
  }

  static long encodeDouble(double value, int exponent, int factor) {
    return fastRoundDouble(value * getDoubleMultiplier(exponent, factor));
  }

  static double decodeDouble(long encoded, int exponent, int factor) {
    return encoded / getDoubleMultiplier(exponent, factor);
  }

  // Same trick but with 2^51+2^52 for double precision.
  static long fastRoundDouble(double value) {
    if (value >= 0) {
      return (long) ((value + MAGIC_DOUBLE) - MAGIC_DOUBLE);
    } else {
      return (long) ((value - MAGIC_DOUBLE) + MAGIC_DOUBLE);
    }
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
