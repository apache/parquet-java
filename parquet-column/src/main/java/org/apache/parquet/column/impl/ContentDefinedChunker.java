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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ContentDefinedChunkingOptions;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * EXPERIMENTAL: Decides the content defined chunk boundaries of one leaf column for a whole file. The column writer
 * asks {@code startsNewChunk} with each triplet before writing it; the chunker rolls the triplet into
 * its hash and answers whether a chunk starts there, and the writer then ends its page. The page limits
 * end pages as they do without content defined chunking.
 *
 * <p>Ported from Arrow C++ {@code parquet/chunker_internal} and arrow-rs {@code column/chunker/cdc.rs},
 * whose boundaries it must reproduce for the same values. They hash a value as Arrow holds it in
 * memory, which for most columns is as Parquet stores it, but for an INT(8) or INT(16) in 1 or 2 bytes
 * rather than the 4 of an INT32; see {@code byteWidth}. Where Arrow holds a value otherwise, the
 * boundaries differ from Arrow's: a DECIMAL, which Arrow holds as a decimal128 or decimal256, and the
 * legacy INT96, which it holds as a timestamp in nanoseconds. So do those of an Arrow writer that
 * converts the values it is given before storing them, such as date64, time32[s] and timestamp[s],
 * and uint32 at Parquet format 1.0, or that is given a dictionary array, whose indices it hashes.
 *
 * <p>As in Arrow, the file writer owns the chunkers of a file, made by {@link #forColumns}, and lends
 * each row group's column writers theirs, so that the chunking continues across row groups. The class
 * is public only for the file writers of other modules; Arrow keeps it internal. Not thread safe.
 */
public final class ContentDefinedChunker {

  private final long minChunkSize;
  private final long maxChunkSize;
  private final long rollingHashMask;
  private final int maxDef;
  private final int maxRep;
  private final int byteWidth;

  private long rollingHash;
  private boolean hasMatched;
  private int nthRun;
  private long chunkSize;

  /** Rolls the bytes written to it into the hash. */
  private final OutputStream byteRoller = new OutputStream() {
    @Override
    public void write(int b) {
      write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
      long hash = rollingHash;
      boolean matched = hasMatched;
      long[] table = GearHashTable.TABLE[nthRun];
      for (int i = offset; i < offset + length; ++i) {
        hash = (hash << 1) + table[bytes[i] & 0xFF];
        matched |= (hash & rollingHashMask) == 0;
      }
      rollingHash = hash;
      hasMatched = matched;
    }
  };

  /**
   * EXPERIMENTAL: The content defined chunkers of a file, one per leaf column of {@code schema} in
   * order, which every row group's column write store of the file continues.
   *
   * @param schema the schema of the file
   * @param options the content defined chunking options of the file
   * @return the chunkers
   */
  public static List<ContentDefinedChunker> forColumns(MessageType schema, ContentDefinedChunkingOptions options) {
    return schema.getColumns().stream()
        .map(path -> new ContentDefinedChunker(path, options))
        .collect(Collectors.toList());
  }

  ContentDefinedChunker(ColumnDescriptor path, ContentDefinedChunkingOptions options) {
    this.minChunkSize = options.getMinChunkSize();
    this.maxChunkSize = options.getMaxChunkSize();
    this.rollingHashMask = calculateMask(options);
    this.maxDef = path.getMaxDefinitionLevel();
    this.maxRep = path.getMaxRepetitionLevel();
    this.byteWidth = byteWidth(path.getPrimitiveType());
  }

  /**
   * The width in bytes Arrow holds a value of an integer column in, as Arrow reads the column by
   * default ({@code GetArrowType} in Arrow C++ {@code parquet/arrow/schema_internal.cc}): an INT32 or
   * INT64 at its own width, but an INT(8) or INT(16) as an int8 or int16. Zero for other columns.
   *
   * @param type the type of the column
   * @return the width Arrow holds a value in, or zero
   */
  private static int byteWidth(PrimitiveType type) {
    if (type.getLogicalTypeAnnotation() instanceof IntLogicalTypeAnnotation integer) {
      return integer.getBitWidth() / Byte.SIZE;
    }
    return switch (type.getPrimitiveTypeName()) {
      case INT32 -> Integer.BYTES;
      case INT64 -> Long.BYTES;
      default -> 0;
    };
  }

  /**
   * EXPERIMENTAL: Derives the rolling hash mask, rejecting options that cannot produce a usable one,
   * which {@link ContentDefinedChunkingOptions} does when it is built. Ported from Arrow C++
   * {@code CalculateMask} and arrow-rs {@code calculate_mask}; the arithmetic has to stay
   * bit-identical to theirs.
   *
   * <p>A mask with the top {@code n} bits set matches a uniform hash with probability 1/2^n. The
   * target is the average chunk size minus the skipped {@code minChunkSize}, divided by the
   * eight matches a boundary needs.
   *
   * @param options the chunking options
   * @return the rolling hash mask
   * @throws IllegalArgumentException if the options cannot produce a usable mask
   */
  public static long calculateMask(ContentDefinedChunkingOptions options) {
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
    roll(value, byteWidth);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(long value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value, byteWidth);
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

  /** Arrow C++ {@code Roll<kByteWidth>}: the {@code width} low-order bytes, little-endian. */
  private void roll(long bits, int width) {
    chunkSize += width;
    if (chunkSize < minChunkSize) {
      return;
    }
    long hash = rollingHash;
    boolean matched = hasMatched;
    long[] table = GearHashTable.TABLE[nthRun];
    for (int i = 0; i < width; ++i, bits >>>= 8) {
      hash = (hash << 1) + table[(int) bits & 0xFF];
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
    try {
      // The value hands over its bytes as it does to the values writers, with no view or copy of a
      // heap value allocated, even where several kinds of Binary reach this call.
      value.writeTo(byteRoller);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not hash bytes", e);
    }
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
