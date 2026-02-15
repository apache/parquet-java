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

  // ========== Page Header Constants ==========

  /** Current ALP format version */
  public static final int ALP_VERSION = 1;

  /** ALP compression mode identifier (0 = ALP) */
  public static final int ALP_COMPRESSION_MODE = 0;

  /** Frame of Reference encoding for integers (0 = Frame of Reference) */
  public static final int ALP_INTEGER_ENCODING_FOR = 0;

  /** Size of the ALP page header in bytes */
  public static final int ALP_HEADER_SIZE = 8;

  // ========== Vector Size Constants ==========

  /** Default number of elements per compressed vector (2^10 = 1024) */
  public static final int DEFAULT_VECTOR_SIZE = 1024;

  /** Log2 of the default vector size */
  public static final int DEFAULT_VECTOR_SIZE_LOG = 10;

  /** Maximum allowed log2 of vector size */
  static final int MAX_LOG_VECTOR_SIZE = 16;

  /** Minimum allowed log2 of vector size */
  static final int MIN_LOG_VECTOR_SIZE = 3;

  // ========== Exponent/Factor Limits ==========

  /** Maximum exponent for float encoding (10^10 ~ 10 billion) */
  static final int FLOAT_MAX_EXPONENT = 10;

  /** Maximum exponent for double encoding (10^18 ~ 1 quintillion) */
  static final int DOUBLE_MAX_EXPONENT = 18;

  // ========== Sampling Constants ==========

  /** Number of sample vectors used for preset caching */
  static final int SAMPLER_SAMPLE_VECTORS = 8;

  /** Maximum (exponent, factor) combinations to keep in preset */
  static final int MAX_PRESET_COMBINATIONS = 5;

  // ========== Fast Rounding Magic Numbers ==========

  /**
   * Magic number for fast float rounding using the floating-point trick.
   * Formula: 2^22 + 2^23 = 12,582,912
   */
  static final float MAGIC_FLOAT = 12_582_912.0f;

  /**
   * Magic number for fast double rounding using the floating-point trick.
   * Formula: 2^51 + 2^52 = 6,755,399,441,055,744
   */
  static final double MAGIC_DOUBLE = 6_755_399_441_055_744.0;

  // ========== Metadata Sizes ==========

  /** Size of AlpInfo structure in bytes (exponent:1 + factor:1 + num_exceptions:2) */
  public static final int ALP_INFO_SIZE = 4;

  /** Size of ForInfo structure for float (frame_of_reference:4 + bit_width:1) */
  public static final int FLOAT_FOR_INFO_SIZE = 5;

  /** Size of ForInfo structure for double (frame_of_reference:8 + bit_width:1) */
  public static final int DOUBLE_FOR_INFO_SIZE = 9;

  // ========== Precomputed Powers of 10 ==========

  /** Precomputed powers of 10 for float encoding (10^0 to 10^10) */
  static final float[] FLOAT_POW10 = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f};

  /** Precomputed powers of 10 for double encoding (10^0 to 10^18) */
  static final double[] DOUBLE_POW10 = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
  };

  // ========== Bit Masks for Negative Zero Detection ==========

  /** Bit pattern for negative zero in float */
  static final int FLOAT_NEGATIVE_ZERO_BITS = 0x80000000;

  /** Bit pattern for negative zero in double */
  static final long DOUBLE_NEGATIVE_ZERO_BITS = 0x8000000000000000L;

  // ========== Validation ==========

  /**
   * Validate that a vector size is a power of 2 and within the allowed range.
   *
   * @param vectorSize the vector size to validate
   * @return the validated vector size
   * @throws IllegalArgumentException if the vector size is invalid
   */
  static int validateVectorSize(int vectorSize) {
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
