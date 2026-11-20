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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * EXPERIMENTAL: Decides the content defined chunk boundaries of one leaf column for a whole file: the
 * column writer ends its page before a triplet {@code startsNewChunk} answers {@code true} for.
 *
 * <p>Ported from Arrow C++ {@code parquet/chunker_internal} and arrow-rs {@code column/chunker/cdc.rs},
 * with the same boundaries for the same values, except where Arrow holds them in another form, such as
 * a DECIMAL, an INT96 or a dictionary array.
 *
 * <p>The chunkers {@link #forColumns} makes continue across the row groups of a file. Public for the
 * file writers of other modules only. Not thread safe.
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
   * EXPERIMENTAL: The chunkers of a file, one per leaf column of {@code schema}, for every row group.
   *
   * @param schema the schema of the file
   * @param props the properties of the file
   * @return the chunkers
   */
  public static List<ContentDefinedChunker> forColumns(MessageType schema, ParquetProperties props) {
    return schema.getColumns().stream()
        .map(path -> new ContentDefinedChunker(path, props))
        .collect(Collectors.toList());
  }

  ContentDefinedChunker(ColumnDescriptor path, ParquetProperties props) {
    this.minChunkSize = props.getContentDefinedChunkingMinSize();
    this.maxChunkSize = props.getContentDefinedChunkingMaxSize();
    this.rollingHashMask = calculateMask(minChunkSize, maxChunkSize, props.getContentDefinedChunkingNormLevel());
    this.maxDef = path.getMaxDefinitionLevel();
    this.maxRep = path.getMaxRepetitionLevel();
    this.byteWidth = byteWidth(path.getPrimitiveType());
  }

  /**
   * The bytes Arrow holds an integer in: 1 for an INT(8), 2 for an INT(16), 4 for other INT32s and 8 for
   * INT64s. Zero for other columns.
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
   * EXPERIMENTAL: The rolling hash mask, bit for bit as Arrow C++ {@code CalculateMask} derives it: the
   * top 15 bits for the default 256 KiB to 1 MiB.
   *
   * @param minChunkSize the minimum chunk size in bytes
   * @param maxChunkSize the maximum chunk size in bytes
   * @param normLevel the normalization level
   * @return the rolling hash mask
   * @throws IllegalArgumentException if the minimum is negative, the maximum not above it, or the
   *     mask would not have 1 to 63 bits
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

  /** Rolls a triplet into the hash and answers whether a chunk starts with it, as only a record's first can. */
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

  /** The same for a triplet without a value: a null, or an omitted required field at {@code maxDef}. */
  boolean startsNewChunk(int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    return repetitionLevel == 0 && needNewChunk();
  }

  /** The definition level, then the repetition level, as two bytes each, if the column has them. */
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
      // Hands over a heap value's bytes without copying them, as to the values writers
      value.writeTo(byteRoller);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not hash bytes", e);
    }
  }

  /**
   * Arrow C++ {@code NeedNewChunk}: a chunk ends after a match against each of the eight gear hash
   * tables in turn, or at {@code maxChunkSize}, which leaves the run counter alone.
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
