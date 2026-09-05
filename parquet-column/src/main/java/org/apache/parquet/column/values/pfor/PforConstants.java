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
 *   <li>Subtracting a frame of reference: any lower bound on the vector, chosen so the
 *       residuals pack narrowly, and not necessarily the minimum</li>
 *   <li>Choosing an optimal bit width via a cost model</li>
 *   <li>Bit-packing the residuals at the chosen width</li>
 *   <li>Storing outlier values (exceptions) separately with their positions</li>
 * </ol>
 *
 * <p>A writer may first replace the values of a vector with the differences
 * between successive values -- the delta mode -- and run all of the above on
 * those instead. The choice is recorded per vector in bit 7 of the bit width
 * byte, and such a vector carries its own first value so that it still decodes
 * without the vector before it.
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

  // The bit width occupies bits 0..6 of its byte and must be masked off before it
  // is used or range-checked; bit 7 says the vector holds differences.
  //
  // The width takes seven bits rather than six because its range is 0..64
  // inclusive, and 64 does not fit in six: masking with six bits would read an
  // INT64 vector whose residuals need the full 64 bits as width 0, which has no
  // packed bytes and no exceptions, so the misreading looks like a constant
  // vector and neither a size mismatch nor an error reveals it.
  public static final int BIT_WIDTH_MASK = 0x7F;
  public static final int DELTA_FLAG = 0x80;

  // A delta vector stores its first value between its info block and its packed
  // residuals, so its header is that much longer.
  public static int vectorInfoSize(int valueByteWidth, boolean delta) {
    int base = valueByteWidth == INT32_VALUE_BYTE_WIDTH ? INT32_VECTOR_INFO_SIZE : INT64_VECTOR_INFO_SIZE;
    return delta ? base + valueByteWidth : base;
  }

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
