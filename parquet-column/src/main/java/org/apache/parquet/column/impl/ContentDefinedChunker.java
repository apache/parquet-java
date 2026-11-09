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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

/**
 * Decides the content defined chunk boundaries of one leaf column for a whole file. The column writer
 * asks {@code startsNewChunk} with each triplet before writing it; the chunker rolls the triplet into
 * its hash and answers whether a chunk starts there, and the writer then ends its page. The page limits
 * end pages as they do without content defined chunking.
 *
 * <p>Ported from Arrow C++ {@code parquet/chunker_internal} and arrow-rs {@code column/chunker/cdc.rs},
 * whose boundaries it must reproduce. Not thread safe.
 */
public final class ContentDefinedChunker {

  private final long minChunkSize;
  private final long maxChunkSize;
  private final long rollingHashMask;
  private final int maxDef;
  private final int maxRep;

  private long rollingHash;
  private boolean hasMatched;
  private int nthRun;
  private long chunkSize;

  /**
   * EXPERIMENTAL: The content defined chunkers of a file, one per leaf column of {@code schema} in
   * order, which every row group's column write store of the file continues.
   *
   * @param schema the schema of the file
   * @param props the writer properties, with content defined chunking enabled
   * @return the chunkers
   */
  public static List<ContentDefinedChunker> forColumns(MessageType schema, ParquetProperties props) {
    return schema.getColumns().stream()
        .map(path -> new ContentDefinedChunker(path, props))
        .collect(Collectors.toList());
  }

  ContentDefinedChunker(ColumnDescriptor path, ParquetProperties props) {
    CdcOptions options = props.getContentDefinedChunkingOptions();
    this.minChunkSize = options.getMinChunkSize();
    this.maxChunkSize = options.getMaxChunkSize();
    this.rollingHashMask = calculateMask(options);
    this.maxDef = path.getMaxDefinitionLevel();
    this.maxRep = path.getMaxRepetitionLevel();
  }

  /**
   * Derives the rolling hash mask, rejecting options that cannot produce a usable one. Ported from
   * Arrow C++ {@code CalculateMask} and arrow-rs {@code calculate_mask}; the arithmetic has to stay
   * bit-identical to theirs. {@link CdcOptions} has already checked the size range.
   *
   * <p>A mask with the top {@code n} bits set matches a uniform hash with probability 1/2^n. The
   * target is the average chunk size minus the skipped {@code minChunkSize}, divided by the
   * eight matches a boundary needs.
   *
   * @param options the chunking options
   * @return the rolling hash mask
   * @throws IllegalArgumentException if the options cannot produce a usable mask
   */
  static long calculateMask(CdcOptions options) {
    long minChunkSize = options.getMinChunkSize();
    long maxChunkSize = options.getMaxChunkSize();
    int normLevel = options.getNormLevel();
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

  /**
   * Rolls a triplet into the hash and answers whether it starts a new chunk, which only the first
   * triplet of a record can; see {@link #needNewChunk()}.
   *
   * @param value the value of the triplet
   * @param repetitionLevel the repetition level of the triplet
   * @param definitionLevel the definition level of the triplet
   * @return whether a chunk ends before the triplet
   */
  boolean startsNewChunk(int value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value, 4);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(long value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value, 8);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(float value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    // Raw bits: floatToIntBits canonicalizes NaN payloads and would diverge from the references.
    roll(Float.floatToRawIntBits(value), 4);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(double value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(Double.doubleToRawLongBits(value), 8);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(boolean value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    // One byte per boolean, not one bit, as the references hash it.
    roll(value ? 1 : 0, 1);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(Binary value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value);
    return repetitionLevel == 0 && needNewChunk();
  }

  /**
   * Rolls the triplet of a null, which has no value to hash even when {@code definitionLevel ==
   * maxDef}, as an omitted top-level required field does, and answers whether a chunk ends before it.
   */
  boolean startsNewChunk(int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    return repetitionLevel == 0 && needNewChunk();
  }

  /**
   * The levels of a triplet as two bytes each, definition level first, as the references hash them,
   * and only those the column has; Arrow C++ does this inline in {@code Calculate}.
   */
  private void rollLevels(int repetitionLevel, int definitionLevel) {
    if (maxDef > 0) {
      roll(definitionLevel, 2);
    }
    if (maxRep > 0) {
      roll(repetitionLevel, 2);
    }
  }

  /** Arrow C++ {@code Roll<kByteWidth>}: the {@code byteWidth} low-order bytes, little-endian. */
  private void roll(long bits, int byteWidth) {
    chunkSize += byteWidth;
    if (chunkSize < minChunkSize) {
      return;
    }
    long hash = rollingHash;
    boolean matched = hasMatched;
    long[] table = GearHashTable.TABLE[nthRun];
    for (int i = 0; i < byteWidth; ++i) {
      hash = (hash << 1) + table[(int) ((bits >>> (8 * i)) & 0xFF)];
      matched |= (hash & rollingHashMask) == 0;
    }
    rollingHash = hash;
    hasMatched = matched;
  }

  /** Arrow C++ {@code Roll(const uint8_t*, length)}: the value's bytes without a length prefix. */
  private void roll(Binary value) {
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
      matched |= (hash & rollingHashMask) == 0;
    }
    rollingHash = hash;
    hasMatched = matched;
  }

  /**
   * Arrow C++ {@code NeedNewChunk} and arrow-rs {@code need_new_chunk}: a chunk ends after eight
   * matches, each against the next gear hash table, which approximates a normal chunk size
   * distribution, or at {@code maxChunkSize}. Neither resets the rolling hash, and the maximum size cut
   * leaves the run counter alone. Asked at record starts only, as they ask it, so a match or the maximum
   * size reached inside a record is kept until the next one.
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
