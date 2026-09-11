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

import org.apache.parquet.column.values.symboltable.CodeStreamDecoder;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Expands 8-bit FSST codes back into value bytes.
 *
 * <p>Decoding needs nothing but the table: each code either names a symbol to copy or announces that
 * the next byte stands for itself. There is no state carried between values, which is what lets a
 * reader expand one value without touching the ones before it.
 */
class Fsst8CodeStreamDecoder implements CodeStreamDecoder {

  private final byte[] symbolBytes;
  private final int[] symbolOffsets;
  private final int symbolCount;

  Fsst8CodeStreamDecoder(byte[] symbolBytes, int[] symbolOffsets, int symbolCount) {
    this.symbolBytes = symbolBytes;
    this.symbolOffsets = symbolOffsets;
    this.symbolCount = symbolCount;
  }

  @Override
  public int expandedLength(byte[] codes, int codesOffset, int codesLength) {
    int length = 0;
    int position = codesOffset;
    int end = codesOffset + codesLength;
    while (position < end) {
      int code = codes[position++] & 0xFF;
      if (code == Fsst8SymbolTable.ESCAPE) {
        requireLiteral(position, end);
        position++;
        length++;
      } else {
        length += symbolLength(code);
      }
    }
    return length;
  }

  @Override
  public int expand(byte[] codes, int codesOffset, int codesLength, byte[] destination, int destinationOffset) {
    int written = destinationOffset;
    int position = codesOffset;
    int end = codesOffset + codesLength;
    while (position < end) {
      int code = codes[position++] & 0xFF;
      if (code == Fsst8SymbolTable.ESCAPE) {
        requireLiteral(position, end);
        destination[written++] = codes[position++];
      } else {
        int length = symbolLength(code);
        System.arraycopy(symbolBytes, symbolOffsets[code], destination, written, length);
        written += length;
      }
    }
    return written - destinationOffset;
  }

  private int symbolLength(int code) {
    if (code >= symbolCount) {
      throw new ParquetDecodingException(
          "FSST value references symbol code " + code + " but the table holds " + symbolCount + " symbols");
    }
    return symbolOffsets[code + 1] - symbolOffsets[code];
  }

  private static void requireLiteral(int position, int end) {
    if (position >= end) {
      throw new ParquetDecodingException("FSST value ends with an escape and no literal byte");
    }
  }
}
