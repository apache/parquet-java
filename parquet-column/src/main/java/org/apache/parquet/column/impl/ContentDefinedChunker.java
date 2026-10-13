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
package org.apache.parquet.column.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

/**
 * Decides the content defined data page boundaries of one leaf column for a whole file: each
 * triplet is rolled into the hash, and {@link #needNewChunk()} says whether a chunk ends before the
 * next one.
 *
 * <p>Ported from Arrow C++ {@code parquet/chunker_internal} and arrow-rs {@code column/chunker/cdc.rs},
 * whose boundaries it must reproduce. Not thread safe.
 */
public final class ContentDefinedChunker {

  private final long minChunkSize;
  private final long maxChunkSize;
  private final long mask;
  private final int maxDef;
  private final int maxRep;

  private long rollingHash;
  private boolean hasMatched;
  private int nthRun;
  private long chunkSize;

  /**
   * EXPERIMENTAL: The chunkers of a file, one per leaf column of {@code schema} in order, which
   * every row group's column write store of the file continues.
   *
   * @param schema the schema of the file
   * @param options the content defined chunking options
   * @return the chunkers
   */
  public static List<ContentDefinedChunker> forColumns(MessageType schema, CdcOptions options) {
    return schema.getColumns().stream()
        .map(path -> new ContentDefinedChunker(options, path))
        .collect(Collectors.toList());
  }

  ContentDefinedChunker(CdcOptions options, ColumnDescriptor path) {
    this.minChunkSize = options.getMinChunkSize();
    this.maxChunkSize = options.getMaxChunkSize();
    this.mask = calculateMask(minChunkSize, maxChunkSize, options.getNormLevel());
    this.maxDef = path.getMaxDefinitionLevel();
    this.maxRep = path.getMaxRepetitionLevel();
  }

  /**
   * Derives the rolling hash mask, validating the envelope. Ported from Arrow C++
   * {@code CalculateMask} and arrow-rs {@code calculate_mask}; the arithmetic has to stay
   * bit-identical to theirs.
   *
   * <p>A mask with the top {@code n} bits set matches a uniform hash with probability 1/2^n. The
   * target is the average chunk size minus the skipped {@code minChunkSize}, divided by the
   * eight matches a boundary needs.
   *
   * @param minChunkSize the minimum chunk size in bytes
   * @param maxChunkSize the maximum chunk size in bytes
   * @param normLevel the normalization level
   * @return the rolling hash mask
   * @throws IllegalArgumentException if the arguments cannot produce a usable mask
   */
  public static long calculateMask(long minChunkSize, long maxChunkSize, int normLevel) {
    Preconditions.checkArgument(
        minChunkSize >= 0, "Invalid content defined chunking minimum chunk size (negative): %s", minChunkSize);
    Preconditions.checkArgument(
        maxChunkSize > minChunkSize,
        "Invalid content defined chunking size range: maximum chunk size (%s) must be greater than minimum chunk size (%s)",
        maxChunkSize,
        minChunkSize);

    // Halve before adding, so that the sum cannot overflow; the references do overflow there.
    long avgChunkSize = minChunkSize / 2 + maxChunkSize / 2 + (minChunkSize % 2 + maxChunkSize % 2) / 2;
    long targetSize = (avgChunkSize - minChunkSize) / GearHashTable.TABLE.length;
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

  void roll(int value, int repetitionLevel, int definitionLevel) {
    rollTriplet(value, 4, repetitionLevel, definitionLevel);
  }

  void roll(long value, int repetitionLevel, int definitionLevel) {
    rollTriplet(value, 8, repetitionLevel, definitionLevel);
  }

  void roll(float value, int repetitionLevel, int definitionLevel) {
    // Raw bits: floatToIntBits canonicalizes NaN payloads and would diverge from the references.
    rollTriplet(Float.floatToRawIntBits(value), 4, repetitionLevel, definitionLevel);
  }

  void roll(double value, int repetitionLevel, int definitionLevel) {
    rollTriplet(Double.doubleToRawLongBits(value), 8, repetitionLevel, definitionLevel);
  }

  void roll(boolean value, int repetitionLevel, int definitionLevel) {
    // One byte per boolean, not one bit, as the references hash it.
    rollTriplet(value ? 1 : 0, 1, repetitionLevel, definitionLevel);
  }

  private void rollTriplet(long bits, int width, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    rollFixedWidth(bits, width);
  }

  void roll(Binary value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    rollBinary(value);
  }

  /**
   * Rolls the levels only, even when {@code definitionLevel == maxDef}: an omitted top-level
   * required field arrives here as {@code (0, 0)} and has no value to hash.
   */
  void rollNull(int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
  }

  /**
   * Definition level first, and each level as two bytes: the references hash the levels as int16_t
   * in this order.
   */
  private void rollLevels(int repetitionLevel, int definitionLevel) {
    if (maxDef > 0) {
      rollFixedWidth(definitionLevel, 2);
    }
    if (maxRep > 0) {
      rollFixedWidth(repetitionLevel, 2);
    }
  }

  /** The value's bytes without a length prefix, as the references hash them. */
  private void rollBinary(Binary value) {
    chunkSize += value.length();
    if (chunkSize < minChunkSize) {
      return;
    }
    // A view of the value's bytes, wherever they lie, not a copy.
    ByteBuffer bytes = value.toByteBuffer();
    long hash = rollingHash;
    boolean matched = hasMatched;
    long[] table = GearHashTable.TABLE[nthRun];
    for (int i = bytes.position(); i < bytes.limit(); ++i) {
      hash = (hash << 1) + table[bytes.get(i) & 0xFF];
      matched |= (hash & mask) == 0;
    }
    rollingHash = hash;
    hasMatched = matched;
  }

  /**
   * The {@code width} low-order bytes of {@code bits}, little-endian. Like the references, this
   * checks the skip window once per value rather than per byte.
   */
  private void rollFixedWidth(long bits, int width) {
    chunkSize += width;
    if (chunkSize < minChunkSize) {
      return;
    }
    long hash = rollingHash;
    boolean matched = hasMatched;
    long[] table = GearHashTable.TABLE[nthRun];
    for (int i = 0; i < width; ++i) {
      hash = (hash << 1) + table[(int) ((bits >>> (8 * i)) & 0xFF)];
      matched |= (hash & mask) == 0;
    }
    rollingHash = hash;
    hasMatched = matched;
  }

  /**
   * A chunk ends after eight matches, each against the next gear hash table, which approximates a
   * normal chunk size distribution; or at {@code maxChunkSize}. As in the references, neither
   * resets the rolling hash, and the maximum size cut leaves the run counter alone.
   */
  boolean needNewChunk() {
    if (hasMatched) {
      hasMatched = false;
      if (++nthRun >= GearHashTable.TABLE.length) {
        nthRun = 0;
        chunkSize = 0;
        return true;
      }
    }
    if (chunkSize >= maxChunkSize) {
      chunkSize = 0;
      return true;
    }
    return false;
  }
}
