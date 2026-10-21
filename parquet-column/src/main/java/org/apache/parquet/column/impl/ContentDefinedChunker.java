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
 * Decides the content defined data page boundaries of one leaf column for a whole file. The column
 * writer asks {@code needNewPage} before writing each triplet, which this chunker answers by rolling
 * the triplet into its hash; a no-op chunker, {@link #noop()}, stands in while content defined
 * chunking is disabled.
 *
 * <p>Ported from Arrow C++ {@code parquet/chunker_internal} and arrow-rs {@code column/chunker/cdc.rs},
 * whose boundaries it must reproduce. Not thread safe.
 */
public class ContentDefinedChunker {

  private static final ContentDefinedChunker NOOP = new NoopChunker();

  private final long minChunkSize;
  private final long maxChunkSize;
  private final long rollingHashMask;
  private final int maxDef;
  private final int maxRep;
  private final long pageSizeThreshold;
  private final int pageValueCountThreshold;
  private final int pageRowCountLimit;

  private long rollingHash;
  private boolean hasMatched;
  private int nthRun;
  private long chunkSize;

  /**
   * EXPERIMENTAL: The chunkers of a file, one per leaf column of {@code schema} in order, which
   * every row group's column write store of the file continues.
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

  /** The chunker of a column without content defined chunking, which never ends a page. */
  static ContentDefinedChunker noop() {
    return NOOP;
  }

  ContentDefinedChunker(ColumnDescriptor path, ParquetProperties props) {
    CdcOptions options = props.getContentDefinedChunkingOptions();
    this.minChunkSize = options.getMinChunkSize();
    this.maxChunkSize = options.getMaxChunkSize();
    this.rollingHashMask = calculateMask(minChunkSize, maxChunkSize, options.getNormLevel());
    this.maxDef = path.getMaxDefinitionLevel();
    this.maxRep = path.getMaxRepetitionLevel();
    this.pageSizeThreshold = props.getPageSizeThreshold();
    this.pageValueCountThreshold = props.getPageValueCountThreshold();
    this.pageRowCountLimit = props.getPageRowCountLimit();
  }

  /** Only for {@link NoopChunker}, which uses none of the state. */
  private ContentDefinedChunker() {
    this.minChunkSize = 0;
    this.maxChunkSize = 0;
    this.rollingHashMask = 0;
    this.maxDef = 0;
    this.maxRep = 0;
    this.pageSizeThreshold = 0;
    this.pageValueCountThreshold = 0;
    this.pageRowCountLimit = 0;
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
    rollLevels(repetitionLevel, definitionLevel);
    roll(value, 4);
  }

  void roll(long value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value, 8);
  }

  void roll(float value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    // Raw bits: floatToIntBits canonicalizes NaN payloads and would diverge from the references.
    roll(Float.floatToRawIntBits(value), 4);
  }

  void roll(double value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(Double.doubleToRawLongBits(value), 8);
  }

  void roll(boolean value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    // One byte per boolean, not one bit, as the references hash it.
    roll(value ? 1 : 0, 1);
  }

  void roll(Binary value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value);
  }

  /**
   * Rolls the triplet of a null, which has no value to hash even when {@code definitionLevel ==
   * maxDef}, as an omitted top-level required field does.
   */
  void rollNull(int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
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

  /**
   * Whether the column writer should end its page before this triplet, which it asks before writing
   * each one; {@code needNewPage(repetitionLevel, definitionLevel, writer)} asks for a null.
   */
  boolean needNewPage(int value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
    roll(value, repetitionLevel, definitionLevel);
    return pageEndsBefore(repetitionLevel, writer);
  }

  boolean needNewPage(long value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
    roll(value, repetitionLevel, definitionLevel);
    return pageEndsBefore(repetitionLevel, writer);
  }

  boolean needNewPage(float value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
    roll(value, repetitionLevel, definitionLevel);
    return pageEndsBefore(repetitionLevel, writer);
  }

  boolean needNewPage(double value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
    roll(value, repetitionLevel, definitionLevel);
    return pageEndsBefore(repetitionLevel, writer);
  }

  boolean needNewPage(boolean value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
    roll(value, repetitionLevel, definitionLevel);
    return pageEndsBefore(repetitionLevel, writer);
  }

  boolean needNewPage(Binary value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
    roll(value, repetitionLevel, definitionLevel);
    return pageEndsBefore(repetitionLevel, writer);
  }

  boolean needNewPage(int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
    rollNull(repetitionLevel, definitionLevel);
    return pageEndsBefore(repetitionLevel, writer);
  }

  /**
   * At a record start, if a chunk ends there or a page limit is reached, counted from the page start
   * so that it cuts in the same places after an edit. Asking consumes a match, so a match inside a
   * record carries over to the next record start. A boundary on the first value of a page has no page
   * to end.
   */
  private boolean pageEndsBefore(int repetitionLevel, ColumnWriterBase writer) {
    if (repetitionLevel != 0) {
      return false;
    }
    boolean chunkBoundary = needNewChunk();
    return writer.getValueCount() > 0
        && (chunkBoundary
            || writer.getPageRowCount() >= pageRowCountLimit
            || writer.getValueCount() >= pageValueCountThreshold
            || writer.getCurrentPageBufferedSize() >= pageSizeThreshold);
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
   * Arrow C++ {@code NeedNewChunk}: a chunk ends after eight matches, each against the next gear
   * hash table, which approximates a normal chunk size distribution; or at {@code maxChunkSize}.
   * Neither resets the rolling hash, and the maximum size cut leaves the run counter alone.
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

  /** The chunker of a column without content defined chunking: it rolls nothing and ends no page. */
  private static final class NoopChunker extends ContentDefinedChunker {
    @Override
    boolean needNewPage(int value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
      return false;
    }

    @Override
    boolean needNewPage(long value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
      return false;
    }

    @Override
    boolean needNewPage(float value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
      return false;
    }

    @Override
    boolean needNewPage(double value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
      return false;
    }

    @Override
    boolean needNewPage(boolean value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
      return false;
    }

    @Override
    boolean needNewPage(Binary value, int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
      return false;
    }

    @Override
    boolean needNewPage(int repetitionLevel, int definitionLevel, ColumnWriterBase writer) {
      return false;
    }
  }
}
