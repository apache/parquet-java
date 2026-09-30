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
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.internal.column.chunking.RollingHashMask;
import org.apache.parquet.io.api.Binary;

/**
 * Decides content defined data page boundaries for one column.
 *
 * <p>The column writer offers every {@code (definitionLevel, repetitionLevel, value)} triplet, and
 * each {@code offer} says whether a page should end <i>before</i> it. Because boundaries follow
 * from the data alone, an edit early in a column stops shifting every page after it and a
 * content addressable store can deduplicate the unchanged tail.
 *
 * <p>Ported from Arrow C++ {@code parquet/chunker_internal} and arrow-rs
 * {@code column/chunker/cdc.rs}. This is an interop contract, not merely an algorithm: boundaries
 * deduplicate across the three implementations only while all of them compute the same ones, which
 * {@code TestCdcChunkerGoldenBoundaries} pins against Arrow C++'s own output.
 *
 * <p>Not thread safe; one instance belongs to one column writer.
 */
final class CdcChunker {

  private final long minChunkSize;
  private final long maxChunkSize;
  private final long mask;
  private final int maxDef;
  private final int maxRep;

  private long rollingHash;
  private boolean hasMatched;
  private int nthRun;
  private long chunkSize;

  CdcChunker(CdcOptions options, ColumnDescriptor path) {
    this.minChunkSize = options.getMinChunkSize();
    this.maxChunkSize = options.getMaxChunkSize();
    this.mask = RollingHashMask.calculate(minChunkSize, maxChunkSize, options.getNormLevel());
    this.maxDef = path.getMaxDefinitionLevel();
    this.maxRep = path.getMaxRepetitionLevel();
  }

  boolean offer(int value, int repetitionLevel, int definitionLevel) {
    return offerFixedWidth(value, 4, repetitionLevel, definitionLevel);
  }

  boolean offer(long value, int repetitionLevel, int definitionLevel) {
    return offerFixedWidth(value, 8, repetitionLevel, definitionLevel);
  }

  boolean offer(float value, int repetitionLevel, int definitionLevel) {
    // Raw bits: floatToIntBits canonicalizes NaN payloads and would diverge from the references.
    return offerFixedWidth(Float.floatToRawIntBits(value), 4, repetitionLevel, definitionLevel);
  }

  boolean offer(double value, int repetitionLevel, int definitionLevel) {
    return offerFixedWidth(Double.doubleToRawLongBits(value), 8, repetitionLevel, definitionLevel);
  }

  boolean offer(boolean value, int repetitionLevel, int definitionLevel) {
    // One byte per boolean, not one bit; the references count it that way too.
    return offerFixedWidth(value ? 1 : 0, 1, repetitionLevel, definitionLevel);
  }

  private boolean offerFixedWidth(long bits, int width, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    if (definitionLevel == maxDef) {
      rollFixedWidth(bits, width);
    }
    return endsPageHere(repetitionLevel);
  }

  boolean offer(Binary value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    if (definitionLevel == maxDef) {
      rollBinary(value);
    }
    return endsPageHere(repetitionLevel);
  }

  /**
   * Offers a null, which contributes its levels and nothing else.
   *
   * <p>No value is rolled even when {@code definitionLevel == maxDef}: an omitted top-level
   * required field arrives here with both at zero, and there is no value to hash.
   */
  boolean offerNull(int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    return endsPageHere(repetitionLevel);
  }

  /**
   * Feeds the levels, definition first -- the order the references hash them in, which is the
   * opposite of the order the column writer emits them. Swapping the two moves every boundary.
   *
   * <p>Each level is hashed as two little-endian bytes, because the references hash an int16_t. The
   * hashed width changes every boundary downstream, so this narrows regardless of how the levels
   * are stored here; Parquet caps levels at 32767 anyway.
   */
  private void rollLevels(int repetitionLevel, int definitionLevel) {
    if (maxDef > 0) {
      rollFixedWidth(definitionLevel, 2);
    }
    if (maxRep > 0) {
      rollFixedWidth(repetitionLevel, 2);
    }
  }

  /**
   * Whether a page ends before the triplet just rolled.
   *
   * <p>{@link #needNewChunk()} mutates the run state, so it is deliberately not reached away from a
   * record start: a mid-record match carries forward to the next one rather than being consumed.
   */
  private boolean endsPageHere(int repetitionLevel) {
    return (maxRep == 0 || repetitionLevel == 0) && needNewChunk();
  }

  /** The element bytes only, with no length prefix. */
  private void rollBinary(Binary value) {
    // Off length() first, so a value inside the skip window is never read.
    chunkSize += value.length();
    if (chunkSize < minChunkSize) {
      return;
    }
    // toByteBuffer() is a view in every Binary, so this reads a direct buffer in place.
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
   * The {@code width} low-order bytes of {@code bits}, least significant first, matching the
   * little-endian order the references hash a value's storage bytes in.
   *
   * <p>The skip window is checked once per value, not per byte; a per-byte check would diverge at
   * whichever value straddles {@code minChunkSize}.
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
   * Advances the run state, and decides whether a new chunk starts.
   *
   * <p>Two conditions. A single gear hash gives geometrically distributed chunk sizes; requiring
   * eight consecutive matches, each against a different table, approximates a normal distribution
   * by the central limit theorem. And a hard cut at {@code maxChunkSize} bounds the tail.
   *
   * <p>Neither resets the rolling hash -- only the size counter -- and the hard cut deliberately
   * leaves the run counter alone, so the run sequence continues into the next chunk. The references
   * do both on purpose.
   *
   * @return {@code true} if a new chunk starts at the triplet just rolled
   */
  private boolean needNewChunk() {
    if (hasMatched) {
      hasMatched = false;
      if (++nthRun >= RollingHashMask.NUM_GEARHASH_TABLES) {
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
