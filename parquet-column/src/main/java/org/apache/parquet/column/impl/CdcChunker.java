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
 * each {@code offer} says whether a page should end <i>before</i> it.
 *
 * <p>Ported from Arrow C++ {@code parquet/chunker_internal} and arrow-rs
 * {@code column/chunker/cdc.rs}. Pages only deduplicate across implementations while all of them
 * place the same boundaries, which {@code TestCdcChunkerGoldenBoundaries} pins against Arrow C++.
 *
 * <p>Not thread safe. One instance serves one leaf column for a whole file: it is never reset, at a
 * page or a row group boundary, but carried to the next row group's store.
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
    // One byte per boolean, not one bit, as the references hash it.
    return offerFixedWidth(value ? 1 : 0, 1, repetitionLevel, definitionLevel);
  }

  private boolean offerFixedWidth(long bits, int width, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    rollFixedWidth(bits, width);
    return endsPageHere(repetitionLevel);
  }

  boolean offer(Binary value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    rollBinary(value);
    return endsPageHere(repetitionLevel);
  }

  /**
   * Rolls the levels only, even when {@code definitionLevel == maxDef}: an omitted top-level
   * required field arrives here as {@code (0, 0)} and has no value to hash.
   */
  boolean offerNull(int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    return endsPageHere(repetitionLevel);
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

  /**
   * Asks {@link #needNewChunk()} only at a record start, because asking consumes a match; a match
   * inside a record carries over to the next record start.
   */
  private boolean endsPageHere(int repetitionLevel) {
    return repetitionLevel == 0 && needNewChunk();
  }

  /** The value's bytes without a length prefix, as the references hash them. */
  private void rollBinary(Binary value) {
    chunkSize += value.length();
    if (chunkSize < minChunkSize) {
      return;
    }
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
  private boolean needNewChunk() {
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
