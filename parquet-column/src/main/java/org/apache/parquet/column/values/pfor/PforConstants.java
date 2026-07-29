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
package org.apache.parquet.column.values.pfor;

import org.apache.parquet.Preconditions;

/**
 * Constants for the PFOR (Patched Frame of Reference) encoding.
 *
 * <p>PFOR encoding compresses integer columns (INT32/INT64) by:
 * <ol>
 *   <li>Subtracting the minimum value (Frame of Reference)</li>
 *   <li>Choosing an optimal bit width via a cost model</li>
 *   <li>Bit-packing the deltas at the chosen width</li>
 *   <li>Storing outlier values (exceptions) separately with their positions</li>
 * </ol>
 */
public final class PforConstants {

  private PforConstants() {
    // Utility class
  }

  // Page header fields (7 bytes total)
  public static final int PFOR_PACKING_MODE_FOR = 0;
  public static final int PFOR_HEADER_SIZE = 7;

  public static final int DEFAULT_VECTOR_SIZE = 1024;
  public static final int DEFAULT_VECTOR_SIZE_LOG = 10;

  // Capped at 15 (vectorSize=32768) because num_exceptions is uint16,
  // so vectorSize must not exceed 65535 to avoid overflow when all values are exceptions.
  static final int MAX_LOG_VECTOR_SIZE = 15;
  static final int MIN_LOG_VECTOR_SIZE = 3;

  // Maximum exceptions per vector (uint16)
  public static final int MAX_EXCEPTIONS = 65535;

  // Per-vector metadata sizes in bytes
  // INT32: frame_of_reference(4) + bit_width(1) + num_exceptions(2) = 7
  public static final int INT32_VECTOR_INFO_SIZE = 7;
  // INT64: frame_of_reference(8) + bit_width(1) + num_exceptions(2) = 11
  public static final int INT64_VECTOR_INFO_SIZE = 11;

  // Value byte widths
  public static final int INT32_VALUE_BYTE_WIDTH = 4;
  public static final int INT64_VALUE_BYTE_WIDTH = 8;

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
