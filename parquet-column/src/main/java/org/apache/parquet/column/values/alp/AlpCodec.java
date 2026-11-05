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

import static org.apache.parquet.column.values.alp.AlpConstants.DOUBLE_MAX_EXPONENT;
import static org.apache.parquet.column.values.alp.AlpConstants.FLOAT_MAX_EXPONENT;

import org.apache.parquet.bytes.BytesUtils;

/**
 * Encoding multiplies by 10^e and then by the reciprocal of 10^f; decoding multiplies by 10^f and then
 * by the reciprocal of 10^e. Reordering or dividing rounds differently, which changes which values
 * become exceptions, so the bytes would no longer match other implementations.
 */
final class AlpCodec {

  static final float MAGIC_FLOAT = 12_582_912.0f; // 2^22 + 2^23
  static final double MAGIC_DOUBLE = 6_755_399_441_055_744.0; // 2^51 + 2^52

  static final float[] FLOAT_POW10 = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f};

  static final double[] DOUBLE_POW10 = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
  };

  static final float[] FLOAT_POW10_NEGATIVE = {
    1e0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f
  };

  static final double[] DOUBLE_POW10_NEGATIVE = {
    1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16,
    1e-17, 1e-18
  };

  static final int FLOAT_NEGATIVE_ZERO_BITS = 0x80000000;
  static final long DOUBLE_NEGATIVE_ZERO_BITS = 0x8000000000000000L;

  // Beyond these a value could still round-trip through the cast to long or int, so the limits, which
  // match other implementations, are what make it an exception.
  private static final double DOUBLE_ENCODING_UPPER_LIMIT = 9223372036854774784.0;
  private static final double DOUBLE_ENCODING_LOWER_LIMIT = -9223372036854774784.0;
  private static final float FLOAT_ENCODING_UPPER_LIMIT = 2147483520.0f;
  private static final float FLOAT_ENCODING_LOWER_LIMIT = -2147483520.0f;

  private AlpCodec() {
    // Utility class
  }

  /** NaN, Inf, and -0.0 can never be encoded regardless of exponent/factor. */
  static boolean isIntrinsicFloatException(float value) {
    if (Float.isNaN(value)) {
      return true;
    }
    if (Float.isInfinite(value)) {
      return true;
    }
    return Float.floatToRawIntBits(value) == FLOAT_NEGATIVE_ZERO_BITS;
  }

  /** Must use the same expression, in the same order, as {@link #encodeFloat}. */
  static void tryEncodeFloat(float value, int exponent, int factor, EncodeResult out) {
    if (isIntrinsicFloatException(value)) {
      out.isException = true;
      return;
    }
    // Check before rounding: overflow or non-finite after scaling
    float scaled = value * FLOAT_POW10[exponent] * FLOAT_POW10_NEGATIVE[factor];
    if (!Float.isFinite(scaled) || scaled > FLOAT_ENCODING_UPPER_LIMIT || scaled < FLOAT_ENCODING_LOWER_LIMIT) {
      out.isException = true;
      return;
    }
    int encoded = fastRoundFloat(scaled);
    float decoded = decodeFloat(encoded, exponent, factor);
    if (Float.floatToRawIntBits(value) != Float.floatToRawIntBits(decoded)) {
      out.isException = true;
      return;
    }
    out.encoded = encoded;
    out.isException = false;
  }

  /** Round float to nearest integer using magic-number trick with sign branching. */
  static int fastRoundFloat(float value) {
    if (value >= 0) {
      return (int) ((value + MAGIC_FLOAT) - MAGIC_FLOAT);
    } else {
      return (int) ((value - MAGIC_FLOAT) + MAGIC_FLOAT);
    }
  }

  static int encodeFloat(float value, int exponent, int factor) {
    return fastRoundFloat(value * FLOAT_POW10[exponent] * FLOAT_POW10_NEGATIVE[factor]);
  }

  static float decodeFloat(int encoded, int exponent, int factor) {
    return encoded * FLOAT_POW10[factor] * FLOAT_POW10_NEGATIVE[exponent];
  }

  /** NaN, Inf, and -0.0 can never be encoded regardless of exponent/factor. */
  static boolean isIntrinsicDoubleException(double value) {
    if (Double.isNaN(value)) {
      return true;
    }
    if (Double.isInfinite(value)) {
      return true;
    }
    return Double.doubleToRawLongBits(value) == DOUBLE_NEGATIVE_ZERO_BITS;
  }

  /** Must use the same expression, in the same order, as {@link #encodeDouble}. */
  static void tryEncodeDouble(double value, int exponent, int factor, EncodeResult out) {
    if (isIntrinsicDoubleException(value)) {
      out.isException = true;
      return;
    }
    // Check before rounding: overflow or non-finite after scaling
    double scaled = value * DOUBLE_POW10[exponent] * DOUBLE_POW10_NEGATIVE[factor];
    if (!Double.isFinite(scaled) || scaled > DOUBLE_ENCODING_UPPER_LIMIT || scaled < DOUBLE_ENCODING_LOWER_LIMIT) {
      out.isException = true;
      return;
    }
    long encoded = fastRoundDouble(scaled);
    double decoded = decodeDouble(encoded, exponent, factor);
    if (Double.doubleToRawLongBits(value) != Double.doubleToRawLongBits(decoded)) {
      out.isException = true;
      return;
    }
    out.encoded = encoded;
    out.isException = false;
  }

  /** Round double to nearest integer using magic-number trick with sign branching. */
  static long fastRoundDouble(double value) {
    if (value >= 0) {
      return (long) ((value + MAGIC_DOUBLE) - MAGIC_DOUBLE);
    } else {
      return (long) ((value - MAGIC_DOUBLE) + MAGIC_DOUBLE);
    }
  }

  static long encodeDouble(double value, int exponent, int factor) {
    return fastRoundDouble(value * DOUBLE_POW10[exponent] * DOUBLE_POW10_NEGATIVE[factor]);
  }

  static double decodeDouble(long encoded, int exponent, int factor) {
    return encoded * DOUBLE_POW10[factor] * DOUBLE_POW10_NEGATIVE[exponent];
  }

  /**
   * Outcome of a single {@link #tryEncodeFloat} or {@link #tryEncodeDouble} call. Mutable and
   * meant to be reused by the caller so the per-value hot path allocates nothing; an Optional or a
   * freshly allocated result here would box on every value. Not thread safe, so each writer keeps
   * its own.
   */
  static final class EncodeResult {
    /** The encoded value, meaningful only when {@link #isException} is false. */
    long encoded;

    boolean isException;
  }

  static final class EncodingParams {
    final int exponent;
    final int factor;
    final int numExceptions;

    EncodingParams(int exponent, int factor, int numExceptions) {
      this.exponent = exponent;
      this.factor = factor;
      this.numExceptions = numExceptions;
    }
  }

  // Index positions within an (exponent, factor) pair array.
  private static final int E = 0;
  private static final int F = 1;

  // Every valid (exponent, factor) pair, e ascending then f ascending. The order decides the early
  // exit and the fallback when every value is an exception.
  static final int[][] ALL_VALID_FLOAT_PAIRS = buildAllPairs(FLOAT_MAX_EXPONENT);
  static final int[][] ALL_VALID_DOUBLE_PAIRS = buildAllPairs(DOUBLE_MAX_EXPONENT);

  private static int[][] buildAllPairs(int maxExponent) {
    int count = (maxExponent + 1) * (maxExponent + 2) / 2;
    int[][] pairs = new int[count][];
    int idx = 0;
    for (int e = 0; e <= maxExponent; e++) {
      for (int f = 0; f <= e; f++) {
        pairs[idx++] = new int[] {e, f};
      }
    }
    return pairs;
  }

  /**
   * Scores each (exponent, factor) pair by estimated compressed size and returns the best.
   *
   * <p>Estimated size (in bits) = {@code length * bitWidth + exceptions * (Float.SIZE + Short.SIZE)},
   * where bitWidth is the number of bits needed to represent the signed range (max - min) of
   * non-exception encoded values after frame-of-reference subtraction, matching the writer's FOR
   * packing. This produces better compression ratios than minimizing exception count alone. Ties in
   * size are broken toward the higher exponent, then the higher factor. The first pair (in {@code
   * pairs} order) that encodes every value into a single FOR value with zero exceptions wins
   * outright. When no pair yields any non-exception values, {@code pairs[0]} is returned.
   */
  static EncodingParams findBestFloatParams(float[] values, int offset, int length, int[][] pairs) {
    int bestExponent = pairs[0][E];
    int bestFactor = pairs[0][F];
    int bestExceptions = length;
    long bestEstimatedSize = Long.MAX_VALUE;

    // One reusable result for the whole search: every value below is encoded exactly once.
    EncodeResult result = new EncodeResult();
    for (int[] pair : pairs) {
      int e = pair[E];
      int f = pair[F];
      int exceptions = 0;
      int minEncoded = Integer.MAX_VALUE;
      int maxEncoded = Integer.MIN_VALUE;
      for (int i = 0; i < length; i++) {
        tryEncodeFloat(values[offset + i], e, f, result);
        if (result.isException) {
          exceptions++;
        } else {
          int encoded = (int) result.encoded;
          if (encoded < minEncoded) minEncoded = encoded;
          if (encoded > maxEncoded) maxEncoded = encoded;
        }
      }
      int nonExceptions = length - exceptions;
      if (nonExceptions == 0) continue;
      // Signed subtraction gives the true FOR span; getWidthFromMaxLong yields the correct
      // unsigned bit width, and an overflowing large range is penalized with 64 bits. This matches
      // the writer's FOR packing.
      long delta = (nonExceptions < 2) ? 0 : ((long) maxEncoded - (long) minEncoded);
      int bitsPerValue = BytesUtils.getWidthFromMaxLong(delta);
      long estimatedSize = (long) length * bitsPerValue + (long) exceptions * (Float.SIZE + Short.SIZE);
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

  /** Double counterpart to {@link #findBestFloatParams}; see it for the scoring and tie-break rules. */
  static EncodingParams findBestDoubleParams(double[] values, int offset, int length, int[][] pairs) {
    int bestExponent = pairs[0][E];
    int bestFactor = pairs[0][F];
    int bestExceptions = length;
    long bestEstimatedSize = Long.MAX_VALUE;

    // One reusable result for the whole search: every value below is encoded exactly once.
    EncodeResult result = new EncodeResult();
    for (int[] pair : pairs) {
      int e = pair[E];
      int f = pair[F];
      int exceptions = 0;
      long minEncoded = Long.MAX_VALUE;
      long maxEncoded = Long.MIN_VALUE;
      for (int i = 0; i < length; i++) {
        tryEncodeDouble(values[offset + i], e, f, result);
        if (result.isException) {
          exceptions++;
        } else {
          long encoded = result.encoded;
          if (encoded < minEncoded) minEncoded = encoded;
          if (encoded > maxEncoded) maxEncoded = encoded;
        }
      }
      int nonExceptions = length - exceptions;
      if (nonExceptions == 0) continue;
      long delta = (nonExceptions < 2) ? 0 : (maxEncoded - minEncoded);
      int bitsPerValue = BytesUtils.getWidthFromMaxLong(delta);
      long estimatedSize = (long) length * bitsPerValue + (long) exceptions * (Double.SIZE + Short.SIZE);
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
