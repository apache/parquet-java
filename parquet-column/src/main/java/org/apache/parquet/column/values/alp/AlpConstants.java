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
final class AlpConstants {

  private AlpConstants() {
    // Utility class
  }

  // Page header fields
  static final int ALP_COMPRESSION_MODE = 0;
  static final int ALP_INTEGER_ENCODING_FOR = 0;
  static final int ALP_HEADER_SIZE = 7;

  static final int DEFAULT_VECTOR_SIZE = 1024;
  static final int DEFAULT_VECTOR_SIZE_LOG = 10;

  // BytePacker packs/unpacks 8 values at a time (pack8Values/unpack8Values).
  static final int PACK_GROUP_SIZE = 8;

  // Capped at 15 (vectorSize=32768) because num_exceptions is uint16,
  // so vectorSize must not exceed 65535 to avoid overflow when all values are exceptions.
  static final int MAX_LOG_VECTOR_SIZE = 15;
  static final int MIN_LOG_VECTOR_SIZE = 3;

  static final int FLOAT_MAX_EXPONENT = 10;
  static final int DOUBLE_MAX_EXPONENT = 18;

  // Per-vector metadata sizes in bytes
  static final int ALP_INFO_SIZE = 4; // exponent(1) + factor(1) + num_exceptions(2)
  static final int FLOAT_FOR_INFO_SIZE = 5; // frame_of_reference(4) + bit_width(1)
  static final int DOUBLE_FOR_INFO_SIZE = 9; // frame_of_reference(8) + bit_width(1)

  /** Validates vector size: must be a power of 2 in [2^MIN_LOG .. 2^MAX_LOG]. */
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
