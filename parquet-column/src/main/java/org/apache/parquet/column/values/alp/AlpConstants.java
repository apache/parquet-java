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

/**
 * Constants for the ALP (Adaptive Lossless floating-Point) encoding.
 *
 * <p>ALP encoding converts floating-point values to integers using decimal scaling,
 * then applies Frame of Reference (FOR) encoding and bit-packing.
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

  /** FOR encoding for integers (0 = FOR) */
  public static final int ALP_INTEGER_ENCODING_FOR = 0;

  /** Size of the ALP page header in bytes */
  public static final int ALP_HEADER_SIZE = 8;

  // ========== Vector Constants ==========

  /** Default number of elements per compressed vector (2^10 = 1024) */
  public static final int ALP_VECTOR_SIZE = 1024;

  /** Log2 of the default vector size */
  public static final int ALP_VECTOR_SIZE_LOG = 10;

  // ========== Exponent/Factor Limits ==========

  /** Maximum exponent for float encoding (10^10 ~ 10 billion) */
  public static final int FLOAT_MAX_EXPONENT = 10;

  /** Maximum exponent for double encoding (10^18 ~ 1 quintillion) */
  public static final int DOUBLE_MAX_EXPONENT = 18;

  /** Number of (exponent, factor) combinations for float: sum(1..11) = 66 */
  public static final int FLOAT_COMBINATIONS = 66;

  /** Number of (exponent, factor) combinations for double: sum(1..19) = 190 */
  public static final int DOUBLE_COMBINATIONS = 190;

  // ========== Sampling Constants ==========

  /** Number of values sampled per vector */
  public static final int SAMPLER_SAMPLES_PER_VECTOR = 256;

  /** Number of sample vectors per rowgroup */
  public static final int SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP = 8;

  /** Maximum (exponent, factor) combinations to keep in preset */
  public static final int MAX_COMBINATIONS = 5;

  /** Stop sampling if this many consecutive combinations produce worse results */
  public static final int EARLY_EXIT_THRESHOLD = 4;

  // ========== Fast Rounding Magic Numbers ==========

  /**
   * Magic number for fast float rounding using the floating-point trick.
   * Formula: 2^22 + 2^23 = 12,582,912
   */
  public static final float MAGIC_FLOAT = 12_582_912.0f;

  /**
   * Magic number for fast double rounding using the floating-point trick.
   * Formula: 2^51 + 2^52 = 6,755,399,441,055,744
   */
  public static final double MAGIC_DOUBLE = 6_755_399_441_055_744.0;

  // ========== Metadata Sizes ==========

  /** Size of AlpInfo structure in bytes (exponent:1 + factor:1 + num_exceptions:2) */
  public static final int ALP_INFO_SIZE = 4;

  /** Size of ForInfo structure for float (frame_of_reference:4 + bit_width:1) */
  public static final int FLOAT_FOR_INFO_SIZE = 5;

  /** Size of ForInfo structure for double (frame_of_reference:8 + bit_width:1) */
  public static final int DOUBLE_FOR_INFO_SIZE = 9;

  // ========== Precomputed Powers of 10 ==========

  /** Precomputed powers of 10 for float encoding (10^0 to 10^10) */
  public static final float[] FLOAT_POW10 = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f};

  /** Precomputed powers of 10 for double encoding (10^0 to 10^18) */
  public static final double[] DOUBLE_POW10 = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
  };

  /** Precomputed negative powers of 10 for decoding (10^0 to 10^-18) */
  public static final double[] DOUBLE_POW10_NEG = {
    1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16,
    1e-17, 1e-18
  };

  // ========== Bit Masks for Negative Zero Detection ==========

  /** Bit pattern for negative zero in float */
  public static final int FLOAT_NEGATIVE_ZERO_BITS = 0x80000000;

  /** Bit pattern for negative zero in double */
  public static final long DOUBLE_NEGATIVE_ZERO_BITS = 0x8000000000000000L;
}
