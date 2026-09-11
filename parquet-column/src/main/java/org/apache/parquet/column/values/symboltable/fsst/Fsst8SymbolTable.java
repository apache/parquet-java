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
package org.apache.parquet.column.values.symboltable.fsst;

import java.util.Arrays;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.symboltable.CodeStreamDecoder;
import org.apache.parquet.column.values.symboltable.SymbolTable;
import org.apache.parquet.column.values.symboltable.SymbolTableType;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * An 8-bit FSST symbol table in the form a file carries it.
 *
 * <p>The codes here are not the codes the trainer works with. The trainer numbers symbols in an order
 * that suits compression, and the format numbers them in order of length, shortest first, so that a
 * reader can rebuild the table from a length histogram without storing a length per symbol. The
 * permutation between the two is built once, when a trained table is converted, and is what the
 * encoder applies to every code it emits.
 *
 * <p>Serialized body: one byte holding the symbol count, then eight bytes counting the symbols of
 * each length from one to eight, then the symbol bytes end to end in that same order. At most 255
 * symbols of at most 8 bytes each, so at most 2049 bytes. That layout is single-byte-code only, which
 * is why the 16-bit variant needs a table format of its own rather than an extra field here.
 */
public class Fsst8SymbolTable implements SymbolTable {

  /** Code standing for one literal byte, which follows it in the code stream. */
  public static final int ESCAPE = 0xFF;

  static final int MAX_SYMBOL_LENGTH = 8;

  /** Symbol count plus the eight histogram entries. */
  static final int HEADER_SIZE = 9;

  /** Largest serialized body: the header plus 255 symbols of 8 bytes. */
  static final int MAX_SERIALIZED_SIZE = HEADER_SIZE + FsstCodes.MAX_SYMBOLS * MAX_SYMBOL_LENGTH;

  /** Symbol bytes end to end, in code order. */
  private final byte[] symbolBytes;

  /** Where each symbol starts in {@link #symbolBytes}, with one extra entry for the end. */
  private final int[] symbolOffsets;

  private final int symbolCount;

  /**
   * Maps a trainer code to the code written to a file, or {@link #ESCAPE} where the trainer has no
   * such symbol. Null on a table read back from a file, which never encodes.
   */
  private final byte[] encodeCodeMap;

  private Fsst8SymbolTable(byte[] symbolBytes, int[] symbolOffsets, int symbolCount, byte[] encodeCodeMap) {
    this.symbolBytes = symbolBytes;
    this.symbolOffsets = symbolOffsets;
    this.symbolCount = symbolCount;
    this.encodeCodeMap = encodeCodeMap;
  }

  /**
   * Converts a trained table into the file representation, renumbering symbols into length order.
   *
   * <p>Symbols of the same length keep their relative order from the trainer, which is what makes the
   * renumbering reproducible.
   */
  static Fsst8SymbolTable of(FsstSymbolTable trained) {
    int symbolCount = trained.symbolCount;
    if (symbolCount > FsstCodes.MAX_SYMBOLS) {
      throw new IllegalArgumentException(
          "FSST8 holds at most " + FsstCodes.MAX_SYMBOLS + " symbols, got " + symbolCount);
    }
    // A counting sort by length, which is stable and needs no comparator.
    int[] countByLength = new int[MAX_SYMBOL_LENGTH + 1];
    for (int code = 0; code < symbolCount; code++) {
      countByLength[FsstCodes.length(trained.symbolDescriptors[code])]++;
    }
    int[] nextCodeForLength = new int[MAX_SYMBOL_LENGTH + 2];
    int[] nextOffsetForLength = new int[MAX_SYMBOL_LENGTH + 2];
    int runningCode = 0;
    int runningOffset = 0;
    for (int length = 1; length <= MAX_SYMBOL_LENGTH; length++) {
      nextCodeForLength[length] = runningCode;
      nextOffsetForLength[length] = runningOffset;
      runningCode += countByLength[length];
      runningOffset += countByLength[length] * length;
    }

    byte[] symbolBytes = new byte[runningOffset];
    int[] symbolOffsets = new int[symbolCount + 1];
    byte[] encodeCodeMap = new byte[FsstCodes.MAX_SYMBOLS];
    Arrays.fill(encodeCodeMap, (byte) ESCAPE);

    for (int trainerCode = 0; trainerCode < symbolCount; trainerCode++) {
      int length = FsstCodes.length(trained.symbolDescriptors[trainerCode]);
      int fileCode = nextCodeForLength[length]++;
      int offset = nextOffsetForLength[length];
      nextOffsetForLength[length] += length;
      long value = trained.symbolValues[trainerCode];
      for (int i = 0; i < length; i++) {
        symbolBytes[offset + i] = (byte) (value >>> (i * 8));
      }
      symbolOffsets[fileCode] = offset;
      encodeCodeMap[trainerCode] = (byte) fileCode;
    }
    symbolOffsets[symbolCount] = runningOffset;
    return new Fsst8SymbolTable(symbolBytes, symbolOffsets, symbolCount, encodeCodeMap);
  }

  /** Reads a table back from the bytes a file carries. */
  public static Fsst8SymbolTable deserialize(byte[] body, int offset, int length) {
    if (length < HEADER_SIZE || length > MAX_SERIALIZED_SIZE) {
      throw new ParquetDecodingException("Invalid FSST symbol table body size: " + length);
    }
    int symbolCount = body[offset] & 0xFF;
    int[] histogram = new int[MAX_SYMBOL_LENGTH];
    int histogramSum = 0;
    int expectedSymbolBytes = 0;
    for (int i = 0; i < MAX_SYMBOL_LENGTH; i++) {
      histogram[i] = body[offset + 1 + i] & 0xFF;
      histogramSum += histogram[i];
      expectedSymbolBytes += histogram[i] * (i + 1);
    }
    // Validate before allocating, so a corrupt header cannot ask for a large buffer.
    if (histogramSum != symbolCount) {
      throw new ParquetDecodingException("FSST length histogram sums to " + histogramSum
          + " but the table declares " + symbolCount + " symbols");
    }
    if (expectedSymbolBytes != length - HEADER_SIZE) {
      throw new ParquetDecodingException("FSST symbol bytes are " + (length - HEADER_SIZE)
          + " bytes but the length histogram accounts for " + expectedSymbolBytes);
    }

    byte[] symbolBytes = new byte[expectedSymbolBytes];
    System.arraycopy(body, offset + HEADER_SIZE, symbolBytes, 0, expectedSymbolBytes);
    int[] symbolOffsets = new int[symbolCount + 1];
    int code = 0;
    int position = 0;
    for (int length1 = 1; length1 <= MAX_SYMBOL_LENGTH; length1++) {
      for (int i = 0; i < histogram[length1 - 1]; i++) {
        symbolOffsets[code++] = position;
        position += length1;
      }
    }
    symbolOffsets[symbolCount] = position;
    return new Fsst8SymbolTable(symbolBytes, symbolOffsets, symbolCount, null);
  }

  @Override
  public SymbolTableType type() {
    return SymbolTableType.FSST_8;
  }

  @Override
  public int symbolCount() {
    return symbolCount;
  }

  @Override
  public int symbolLength(int code) {
    return symbolOffsets[code + 1] - symbolOffsets[code];
  }

  @Override
  public int copySymbol(int code, byte[] destination, int position) {
    int length = symbolLength(code);
    System.arraycopy(symbolBytes, symbolOffsets[code], destination, position, length);
    return length;
  }

  @Override
  public BytesInput serialize() {
    int[] histogram = new int[MAX_SYMBOL_LENGTH];
    for (int code = 0; code < symbolCount; code++) {
      histogram[symbolLength(code) - 1]++;
    }
    byte[] body = new byte[HEADER_SIZE + symbolBytes.length];
    body[0] = (byte) symbolCount;
    for (int i = 0; i < MAX_SYMBOL_LENGTH; i++) {
      body[1 + i] = (byte) histogram[i];
    }
    System.arraycopy(symbolBytes, 0, body, HEADER_SIZE, symbolBytes.length);
    return BytesInput.from(body);
  }

  @Override
  public CodeStreamDecoder decoder() {
    return new Fsst8CodeStreamDecoder(symbolBytes, symbolOffsets, symbolCount);
  }

  /** The trainer-code to file-code map, for the encoder paired with this table. */
  byte[] encodeCodeMap() {
    if (encodeCodeMap == null) {
      throw new IllegalStateException("a symbol table read from a file cannot encode");
    }
    return encodeCodeMap;
  }
}
