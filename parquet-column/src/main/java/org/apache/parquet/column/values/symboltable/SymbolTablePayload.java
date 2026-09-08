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
package org.apache.parquet.column.values.symboltable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * A data page body holding a code stream and the boundaries between its values.
 *
 * <p>The layout is the same for every symbol table codec:
 *
 * <pre>
 *   [1B] offset encoding: 0 = PLAIN, 1 = DELTA_BINARY_PACKED
 *   [4B] number of values, little-endian
 *   [4B] byte length of the offset section, little-endian
 *   [..] one end offset into the code stream per value
 *   [..] the code stream
 * </pre>
 *
 * <p>Offsets are ends rather than starts, so a value's codes run from the previous end to its own
 * and the first value starts at zero. That is what makes a value readable without expanding the
 * ones in front of it, and it is why a reader that only wants the tenth value pays for the offsets
 * alone.
 *
 * <p>The number of values restates a field the page header already carries. Both copies are read
 * and the page header's is treated as the bound: a payload may describe fewer values than the page
 * holds, never more.
 *
 * <p>This class is the read view. {@link SymbolTablePayloadWriter} produces the same layout.
 */
public final class SymbolTablePayload {

  /**
   * How the offset section is encoded.
   *
   * <p>Delta encoding is worth having here rather than being a tuning knob: offsets grow with the
   * page, so on a page whose values compress to a handful of bytes each, four bytes of offset per
   * value can outweigh the codes they point at.
   */
  public enum OffsetEncoding {
    PLAIN(0),
    DELTA_BINARY_PACKED(1);

    private final int value;

    OffsetEncoding(int value) {
      this.value = value;
    }

    /** The value written as the payload's first byte. */
    public int value() {
      return value;
    }

    static OffsetEncoding fromValue(int value) {
      for (OffsetEncoding encoding : values()) {
        if (encoding.value == value) {
          return encoding;
        }
      }
      throw new ParquetDecodingException("Unsupported symbol table offset encoding: " + value);
    }
  }

  /** Size of the fixed part: the offset encoding byte, the value count, the offset section length. */
  public static final int HEADER_SIZE = 9;

  private final int valueCount;
  private final byte[] codes;
  private final int codesOffset;
  private final int[] endOffsets;

  private SymbolTablePayload(int valueCount, byte[] codes, int codesOffset, int[] endOffsets) {
    this.valueCount = valueCount;
    this.codes = codes;
    this.codesOffset = codesOffset;
    this.endOffsets = endOffsets;
  }

  /**
   * Reads a payload, consuming the rest of the stream.
   *
   * @param in            positioned at the payload's first byte
   * @param pageValueCount the value count from the page header, an upper bound on the payload's own
   */
  public static SymbolTablePayload parse(ByteBufferInputStream in, int pageValueCount) throws IOException {
    int length = in.available();
    if (length < HEADER_SIZE) {
      throw new ParquetDecodingException(
          "Symbol table page body is shorter than its " + HEADER_SIZE + "-byte header: " + length);
    }
    ByteBuffer header = in.slice(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    OffsetEncoding offsetEncoding = OffsetEncoding.fromValue(header.get(header.position()) & 0xFF);
    int valueCount = header.getInt(header.position() + 1);
    int offsetSectionSize = header.getInt(header.position() + 5);

    if (valueCount < 0 || valueCount > pageValueCount) {
      throw new ParquetDecodingException("Symbol table page describes " + valueCount
          + " values, but the page header says the page holds " + pageValueCount);
    }
    if (offsetSectionSize < 0 || offsetSectionSize > length - HEADER_SIZE) {
      throw new ParquetDecodingException("Invalid symbol table offset section length: " + offsetSectionSize);
    }

    int[] endOffsets = readOffsets(in, offsetEncoding, valueCount, offsetSectionSize);

    int codeSectionSize = length - HEADER_SIZE - offsetSectionSize;
    ByteBuffer codeSection = in.slice(codeSectionSize);
    byte[] codes;
    int codesOffset;
    if (codeSection.hasArray()) {
      codes = codeSection.array();
      codesOffset = codeSection.arrayOffset() + codeSection.position();
    } else {
      codes = new byte[codeSectionSize];
      codeSection.get(codes);
      codesOffset = 0;
    }

    checkOffsets(endOffsets, codeSectionSize);
    return new SymbolTablePayload(valueCount, codes, codesOffset, endOffsets);
  }

  private static int[] readOffsets(
      ByteBufferInputStream in, OffsetEncoding offsetEncoding, int valueCount, int offsetSectionSize)
      throws IOException {
    if (valueCount == 0) {
      if (offsetSectionSize != 0) {
        throw new ParquetDecodingException(
            "Symbol table page holds no values but has a " + offsetSectionSize + "-byte offset section");
      }
      return new int[0];
    }
    int[] endOffsets = new int[valueCount];
    if (offsetEncoding == OffsetEncoding.PLAIN) {
      long expected = (long) valueCount * Integer.BYTES;
      if (offsetSectionSize != expected) {
        throw new ParquetDecodingException("Symbol table PLAIN offset section is " + offsetSectionSize
            + " bytes; expected " + expected + " for " + valueCount + " values");
      }
      ByteBuffer offsets = in.slice(offsetSectionSize).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < valueCount; i++) {
        endOffsets[i] = offsets.getInt(offsets.position() + i * Integer.BYTES);
      }
    } else {
      DeltaBinaryPackingValuesReader offsets = new DeltaBinaryPackingValuesReader();
      offsets.initFromPage(valueCount, in.sliceStream(offsetSectionSize));
      for (int i = 0; i < valueCount; i++) {
        endOffsets[i] = offsets.readInteger();
      }
    }
    return endOffsets;
  }

  /**
   * Rejects offsets that do not describe a partition of the code section.
   *
   * <p>Every later read trusts these bounds, so they are checked once here rather than per value.
   * A payload whose offsets stop short of the code section is rejected too: the leftover bytes
   * would be unreachable, which means the payload is not the one the writer produced.
   */
  private static void checkOffsets(int[] endOffsets, int codeSectionSize) {
    int previous = 0;
    for (int endOffset : endOffsets) {
      if (endOffset < previous || endOffset > codeSectionSize) {
        throw new ParquetDecodingException("Symbol table offsets are not monotonic within a " + codeSectionSize
            + "-byte code section: " + previous + " then " + endOffset);
      }
      previous = endOffset;
    }
    if (previous != codeSectionSize) {
      throw new ParquetDecodingException("Symbol table offsets end at " + previous + " but the code section is "
          + codeSectionSize + " bytes");
    }
  }

  /** How many values this payload holds, which may be fewer than the page holds. */
  public int valueCount() {
    return valueCount;
  }

  /** The array holding the code stream; {@link #codeStart} indexes into it. */
  public byte[] codes() {
    return codes;
  }

  /** Where this value's codes begin in {@link #codes}. */
  public int codeStart(int index) {
    return codesOffset + (index == 0 ? 0 : endOffsets[index - 1]);
  }

  /** How many bytes of codes belong to this value. */
  public int codeLength(int index) {
    return index == 0 ? endOffsets[0] : endOffsets[index] - endOffsets[index - 1];
  }
}
