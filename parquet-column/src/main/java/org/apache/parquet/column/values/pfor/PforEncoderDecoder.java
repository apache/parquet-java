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

/**
 * Core PFOR encoding/decoding logic with histogram-based cost model.
 *
 * <p>The cost model selects the optimal bit width by evaluating:
 * <pre>
 * total_cost(b) = num_elements * b + num_exceptions(b) * (16 + value_bits)
 * </pre>
 * where {@code value_bits} is 32 for INT32 and 64 for INT64, and
 * {@code num_exceptions(b)} is the count of deltas requiring more than {@code b} bits.
 */
public final class PforEncoderDecoder {

  private PforEncoderDecoder() {
    // Utility class
  }

  /** Result of the optimal bit width search. */
  public static final class BitWidthResult {
    public final int bitWidth;
    public final int numExceptions;

    BitWidthResult(int bitWidth, int numExceptions) {
      this.bitWidth = bitWidth;
      this.numExceptions = numExceptions;
    }
  }

  /**
   * Find the optimal bit width for packing INT32 unsigned deltas.
   *
   * <p>Builds a histogram of bits required per delta, then evaluates each
   * candidate bit width from 0 to 32 using the cost model.
   *
   * @param deltas unsigned deltas (values[] - min), treated as unsigned int
   * @param numElements number of elements
   * @return the optimal bit width and resulting number of exceptions
   */
  public static BitWidthResult findOptimalBitWidthForInt(int[] deltas, int numElements) {
    // Histogram: bitsHist[b] = count of deltas that need exactly b bits
    int[] bitsHist = new int[33]; // 0..32
    for (int i = 0; i < numElements; i++) {
      bitsHist[bitWidthForInt(deltas[i])]++;
    }

    // Exception cost per exception: position(16 bits) + value(32 bits) = 48 bits
    final long exceptionBitsPerValue = 16 + 32;

    long bestCost = Long.MAX_VALUE;
    int bestBitWidth = 0;
    int bestExceptions = 0;

    // exceptionsAbove[b] = number of deltas requiring > b bits
    int exceptionsAbove = numElements; // at b=0, all nonzero deltas might be exceptions
    // Actually: deltas requiring > 0 bits = all deltas with bitsRequired > 0
    // We need to track cumulative: exceptionsAbove starts at numElements - bitsHist[0]
    // But let's compute it properly by starting from b=0.
    // At b=0, only deltas requiring 0 bits (i.e., delta==0) are NOT exceptions.
    // Correction: at candidate bit_width = b, values needing bitsRequired > b are exceptions.
    // bitsRequired(0) = 0, so delta==0 needs 0 bits. At b=0, exceptions = values with bitsRequired > 0.
    exceptionsAbove = numElements - bitsHist[0];

    for (int b = 0; b <= 32; b++) {
      long packingCost = (long) numElements * b;
      long exceptionCost = (long) exceptionsAbove * exceptionBitsPerValue;
      long totalCost = packingCost + exceptionCost;

      if (totalCost < bestCost) {
        bestCost = totalCost;
        bestBitWidth = b;
        bestExceptions = exceptionsAbove;
      }

      // Move to next candidate: values requiring exactly (b+1) bits are no longer exceptions
      if (b < 32) {
        exceptionsAbove -= bitsHist[b + 1];
      }
    }

    return new BitWidthResult(bestBitWidth, bestExceptions);
  }

  /**
   * Find the optimal bit width for packing INT64 unsigned deltas.
   *
   * @param deltas unsigned deltas (values[] - min), treated as unsigned long
   * @param numElements number of elements
   * @return the optimal bit width and resulting number of exceptions
   */
  public static BitWidthResult findOptimalBitWidthForLong(long[] deltas, int numElements) {
    // Histogram: bitsHist[b] = count of deltas that need exactly b bits
    int[] bitsHist = new int[65]; // 0..64
    for (int i = 0; i < numElements; i++) {
      bitsHist[bitWidthForLong(deltas[i])]++;
    }

    // Exception cost per exception: position(16 bits) + value(64 bits) = 80 bits
    final long exceptionBitsPerValue = 16 + 64;

    long bestCost = Long.MAX_VALUE;
    int bestBitWidth = 0;
    int bestExceptions = 0;

    int exceptionsAbove = numElements - bitsHist[0];

    for (int b = 0; b <= 64; b++) {
      long packingCost = (long) numElements * b;
      long exceptionCost = (long) exceptionsAbove * exceptionBitsPerValue;
      long totalCost = packingCost + exceptionCost;

      if (totalCost < bestCost) {
        bestCost = totalCost;
        bestBitWidth = b;
        bestExceptions = exceptionsAbove;
      }

      if (b < 64) {
        exceptionsAbove -= bitsHist[b + 1];
      }
    }

    return new BitWidthResult(bestBitWidth, bestExceptions);
  }

  /**
   * Returns the number of bits required to represent an unsigned int value.
   * Returns 0 for value == 0.
   */
  public static int bitWidthForInt(int value) {
    if (value == 0) return 0;
    return 32 - Integer.numberOfLeadingZeros(value);
  }

  /**
   * Returns the number of bits required to represent an unsigned long value.
   * Returns 0 for value == 0.
   */
  public static int bitWidthForLong(long value) {
    if (value == 0) return 0;
    return 64 - Long.numberOfLeadingZeros(value);
  }
}
