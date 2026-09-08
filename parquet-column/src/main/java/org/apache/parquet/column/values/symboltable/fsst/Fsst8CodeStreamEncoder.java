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

import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.CODE_BASE;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.ICL_FREE;

import org.apache.parquet.column.values.symboltable.CodeStreamEncoder;

/**
 * Compresses value bytes into 8-bit FSST codes.
 *
 * <p>Two things here are not free choices. The first is chunking: a value is compressed 511 bytes at
 * a time, with the table's separator byte written just past each chunk. That bound is what the
 * reference implementation's vectorized compressor uses, and matching it is what keeps the code
 * stream identical whichever compressor produced it. Since no symbol contains the separator, no
 * match can reach across a chunk boundary, so the boundary is also what makes the eight-byte read at
 * the end of a chunk safe without a shorter tail path.
 *
 * <p>The compression loop is a port of FSST's reference implementation by Peter Boncz, Viktor Leis
 * and Thomas Neumann (CWI / TU Munich), MIT licensed, at https://github.com/cwida/fsst, commit
 * 89f49c580c6388acf3b6ed2a49e1bfde6c05e616. Its three hand-tuned variants differ only in branch
 * layout and produce the same bytes, so one is enough here.
 *
 * <p>The second is the code translation. The trainer numbers symbols in an order that lets the
 * compressor skip work, and a file numbers them by length. So every code leaves this class through
 * the permutation between the two; an escape passes through untouched, since it is a fixed marker in
 * both orders rather than a symbol.
 */
class Fsst8CodeStreamEncoder implements CodeStreamEncoder {

  /** Bytes compressed at a time, matching the reference implementation's vectorized compressor. */
  private static final int CHUNK_SIZE = 511;

  /** Room past the chunk for the separator byte and for an eight-byte read at the chunk's end. */
  private static final int CHUNK_PADDING = 8;

  private final FsstSymbolTable table;
  private final byte[] fileCodeForTrainerCode;
  private final byte[] chunk = new byte[CHUNK_SIZE + CHUNK_PADDING];

  Fsst8CodeStreamEncoder(FsstSymbolTable table, Fsst8SymbolTable fileTable) {
    this.table = table;
    this.fileCodeForTrainerCode = fileTable.encodeCodeMap();
  }

  @Override
  public int maxCompressedLength(int rawLength) {
    // Every byte could escape, which costs the marker plus the byte itself.
    return 2 * rawLength;
  }

  @Override
  public int compress(byte[] source, int sourceOffset, int sourceLength, byte[] destination, int destinationOffset) {
    int written = destinationOffset;
    int consumed = 0;
    while (consumed < sourceLength) {
      int chunkLength = Math.min(CHUNK_SIZE, sourceLength - consumed);
      System.arraycopy(source, sourceOffset + consumed, chunk, 0, chunkLength);
      // Bytes past the separator keep whatever a longer previous chunk left there. That is harmless,
      // and is what the reference implementation does: a match reaching past the separator would
      // have to contain it, and no symbol does.
      chunk[chunkLength] = (byte) table.terminator;
      written = compressChunk(chunkLength, destination, written);
      consumed += chunkLength;
    }
    return written - destinationOffset;
  }

  /** Compresses one chunk out of {@link #chunk}, returning the new write position. */
  private int compressChunk(int chunkLength, byte[] destination, int writePosition) {
    char[] shortCodes = table.shortCodes;
    long[] hashValues = table.hashValues;
    long[] hashDescriptors = table.hashDescriptors;
    byte[] codeMap = fileCodeForTrainerCode;
    int byteLimit = table.byteLimit;
    int written = writePosition;
    int position = 0;
    while (position < chunkLength) {
      long word = FsstCodes.loadWordUnchecked(chunk, position);
      int shortCode = shortCodes[(int) (word & 0xFFFF)];
      int bucket = FsstCodes.hashBucket(word);
      long descriptor = hashDescriptors[bucket];
      if (descriptor < ICL_FREE && hashValues[bucket] == FsstCodes.maskWord(word, descriptor)) {
        // A symbol of three bytes or more.
        destination[written++] = codeMap[FsstCodes.code(descriptor)];
        position += FsstCodes.length(descriptor);
      } else if ((shortCode & 0xFF) < byteLimit) {
        // A two-byte symbol, and the miss above rules out a longer one starting here.
        destination[written++] = codeMap[shortCode & 0xFF];
        position += 2;
      } else if ((shortCode & CODE_BASE) != 0) {
        // No symbol matches, so the byte stands for itself behind the escape marker.
        destination[written++] = (byte) Fsst8SymbolTable.ESCAPE;
        destination[written++] = (byte) word;
        position += 1;
      } else {
        destination[written++] = codeMap[shortCode & 0xFF];
        position += 1;
      }
    }
    return written;
  }
}
