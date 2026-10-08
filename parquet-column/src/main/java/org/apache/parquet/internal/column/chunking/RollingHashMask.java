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
package org.apache.parquet.internal.column.chunking;

import org.apache.parquet.Preconditions;

/**
 * Derives the rolling hash mask that content defined chunking tests boundary candidates against.
 *
 * <p>Ported from Arrow C++ {@code parquet::internal::CalculateMask} and arrow-rs
 * {@code CdcChunker::calculate_mask}; the arithmetic has to stay bit-identical to theirs.
 */
public final class RollingHashMask {

  /** A boundary needs this many rolling hash matches, one per gear hash table. */
  private static final int NUM_GEARHASH_TABLES = 8;

  private RollingHashMask() {}

  /**
   * Derives the mask, validating the envelope.
   *
   * <p>A mask with the top {@code n} bits set matches a uniform hash with probability 1/2^n. The
   * target is the average chunk size minus the skipped {@code minChunkSize}, divided by the
   * {@value #NUM_GEARHASH_TABLES} matches a boundary needs.
   *
   * @param minChunkSize the minimum chunk size in bytes
   * @param maxChunkSize the maximum chunk size in bytes
   * @param normLevel the normalization level
   * @return the rolling hash mask
   * @throws IllegalArgumentException if the arguments cannot produce a usable mask
   */
  public static long calculate(long minChunkSize, long maxChunkSize, int normLevel) {
    Preconditions.checkArgument(
        minChunkSize >= 0, "Invalid content defined chunking minimum chunk size (negative): %s", minChunkSize);
    Preconditions.checkArgument(
        maxChunkSize > minChunkSize,
        "Invalid content defined chunking size range: maximum chunk size (%s) must be greater than minimum chunk size (%s)",
        maxChunkSize,
        minChunkSize);

    // Halve before adding, so that the sum cannot overflow; the references do overflow there.
    long avgChunkSize = minChunkSize / 2 + maxChunkSize / 2 + (minChunkSize % 2 + maxChunkSize % 2) / 2;
    long targetSize = (avgChunkSize - minChunkSize) / NUM_GEARHASH_TABLES;
    int targetBits = Long.SIZE - Long.numberOfLeadingZeros(targetSize);
    int maskBits = targetBits == 0 ? 0 : targetBits - 1;
    int effectiveBits = maskBits - normLevel;

    // Java takes a long's shift count modulo 64, so an out-of-range width must be rejected here.
    Preconditions.checkArgument(
        effectiveBits >= 1 && effectiveBits <= 63,
        "The content defined chunking mask must be between 1 and 63 bits but was %s"
            + " (minimum chunk size %s, maximum chunk size %s, normalization level %s)",
        effectiveBits,
        minChunkSize,
        maxChunkSize,
        normLevel);
    return -1L << (Long.SIZE - effectiveBits);
  }
}
