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

import org.apache.parquet.Preconditions;

/**
 * Constants for the ALP (Adaptive Lossless floating-Point) encoding.
 *
 * <p>ALP encoding converts floating-point values to integers using decimal scaling,
 * then applies Frame of Reference encoding and bit-packing.
 * Values that cannot be losslessly converted are stored as exceptions.
 *
 * <p>Based on the paper: "ALP: Adaptive Lossless floating-Point Compression" (SIGMOD 2024)
 *
 * @see <a href="https://dl.acm.org/doi/10.1145/3626717">ALP Paper</a>
 */
public final class AlpConstants {

  private AlpConstants() {
    // Utility class
  }

  // ========== Page header (7 bytes, matching C++ AlpHeader) ==========
  // [compression_mode(1)][integer_encoding(1)][log_vector_size(1)][num_elements(4)]
  public static final int HEADER_SIZE = 7;
  public static final int COMPRESSION_MODE_ALP = 0;
  public static final int INTEGER_ENCODING_FOR = 0;

  // ========== Vector sizing ==========
  public static final int DEFAULT_VECTOR_SIZE = 1024;
  public static final int DEFAULT_VECTOR_SIZE_LOG = 10;

  // Capped at 15 (vectorSize=32768) because num_exceptions is uint16,
  // so vectorSize must not exceed 65535 to avoid overflow when all values are exceptions.
  static final int MAX_LOG_VECTOR_SIZE = 15;
  static final int MIN_LOG_VECTOR_SIZE = 3;

  // ========== Sampler constants (matching C++ AlpConstants) ==========
  static final int SAMPLER_VECTOR_SIZE = 4096;
  static final int SAMPLER_ROWGROUP_SIZE = 122880;
  static final int SAMPLER_SAMPLES_PER_VECTOR = 256;
  static final int SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP = 8;
  static final int MAX_COMBINATIONS = 5;
  static final int SAMPLING_EARLY_EXIT_THRESHOLD = 4;

  // ========== Float-specific ==========
  static final int FLOAT_MAX_EXPONENT = 10;
  static final float MAGIC_FLOAT = 12_582_912.0f; // 2^22 + 2^23
  static final float FLOAT_ENCODING_UPPER_LIMIT = 2147483520.0f;
  static final float FLOAT_ENCODING_LOWER_LIMIT = -2147483520.0f;
  static final int FLOAT_NEGATIVE_ZERO_BITS = 0x80000000;

  static final float[] FLOAT_POW10 = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f};

  // ========== Double-specific ==========
  static final int DOUBLE_MAX_EXPONENT = 18;
  static final double MAGIC_DOUBLE = 6_755_399_441_055_744.0; // 2^51 + 2^52
  static final double DOUBLE_ENCODING_UPPER_LIMIT = 9223372036854774784.0;
  static final double DOUBLE_ENCODING_LOWER_LIMIT = -9223372036854774784.0;
  static final long DOUBLE_NEGATIVE_ZERO_BITS = 0x8000000000000000L;

  static final double[] DOUBLE_POW10 = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
  };

  // ========== Per-vector metadata sizes ==========
  public static final int ALP_INFO_SIZE = 4; // exponent(1) + factor(1) + num_exceptions(2)
  public static final int FLOAT_FOR_INFO_SIZE = 5; // frame_of_reference(4) + bit_width(1)
  public static final int DOUBLE_FOR_INFO_SIZE = 9; // frame_of_reference(8) + bit_width(1)

  // ========== Offset and position types ==========
  // OffsetType = int (4 bytes), PositionType = short (2 bytes) — matching C++ uint32_t and uint16_t
  public static final int OFFSET_SIZE = Integer.BYTES;
  public static final int POSITION_SIZE = Short.BYTES;

  /** Returns 10^power as a long, for power in [0, 18]. */
  static long integerPow10(int power) {
    Preconditions.checkArgument(power >= 0 && power <= 18, "power must be in [0, 18], got: %s", power);
    return INTEGER_POW10[power];
  }

  private static final long[] INTEGER_POW10 = {
    1L,
    10L,
    100L,
    1_000L,
    10_000L,
    100_000L,
    1_000_000L,
    10_000_000L,
    100_000_000L,
    1_000_000_000L,
    10_000_000_000L,
    100_000_000_000L,
    1_000_000_000_000L,
    10_000_000_000_000L,
    100_000_000_000_000L,
    1_000_000_000_000_000L,
    10_000_000_000_000_000L,
    100_000_000_000_000_000L,
    1_000_000_000_000_000_000L,
  };

  /** Validates vector size: must be a power of 2 in [2^MIN_LOG .. 2^MAX_LOG]. */
  public static int validateVectorSize(int vectorSize) {
    Preconditions.checkArgument(
        vectorSize > 0 && (vectorSize & (vectorSize - 1)) == 0,
        "Vector size must be a power of 2, got: %s",
        vectorSize);
    int logSize = Integer.numberOfTrailingZeros(vectorSize);
    Preconditions.checkArgument(
        logSize >= MIN_LOG_VECTOR_SIZE && logSize <= MAX_LOG_VECTOR_SIZE,
        "Vector size log2 must be between %s and %s, got: %s (vectorSize=%s)",
        MIN_LOG_VECTOR_SIZE,
        MAX_LOG_VECTOR_SIZE,
        logSize,
        vectorSize);
    return vectorSize;
  }
}
