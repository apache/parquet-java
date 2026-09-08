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
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.CODE_MASK;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.CODE_MAX;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.HASH_TAB_SIZE;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.ICL_FREE;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.LEN_BITS;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.MAX_SYMBOLS;
import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.MAX_SYMBOL_LENGTH;

/**
 * The mutable symbol table the FSST trainer builds and the compressor reads.
 *
 * <p>Java port of {@code SymbolTable} from the FSST reference implementation by Peter Boncz,
 * Viktor Leis and Thomas Neumann (CWI / TU Munich), MIT licensed, at
 * https://github.com/cwida/fsst, commit 89f49c580c6388acf3b6ed2a49e1bfde6c05e616.
 *
 * <p>Symbols are held in parallel {@code long} arrays rather than as objects. The reference
 * implementation copies symbols by value throughout; parallel arrays reproduce that without the
 * aliasing that an array of mutable objects would introduce, and keep the inner loops free of
 * pointer chasing. The two code lookup tables use {@code char}, which is Java's only unsigned
 * 16-bit type and so matches the reference implementation's {@code u16} without masking.
 *
 * <p>Two representations of a code appear here and must not be confused. The lookup tables hold
 * {@code (length << 12) | code}; a symbol descriptor holds {@code (length << 28) | (code << 16) |
 * ignoredBits}.
 */
final class FsstSymbolTable {

  /** Code for a 2-byte pattern, else the pseudo code for its escaped first byte. */
  final char[] shortCodes = new char[65536];

  /** Code for a 1-byte symbol, else its escaped pseudo code. Not needed after {@link #finish}. */
  final char[] byteCodes = new char[256];

  /** Symbol bytes, little-endian. Indices below {@link FsstCodes#CODE_BASE} are pseudo symbols. */
  final long[] symbolValues = new long[CODE_MAX];

  /** Symbol descriptors, parallel to {@link #symbolValues}. */
  final long[] symbolDescriptors = new long[CODE_MAX];

  /** Symbols of three bytes or more, replicated here to avoid an indirection. */
  final long[] hashValues = new long[HASH_TAB_SIZE];

  final long[] hashDescriptors = new long[HASH_TAB_SIZE];

  /** Count of symbols of each byte length, indexed by length minus one. */
  final int[] lengthHistogram = new int[FsstCodes.CODE_BITS];

  int symbolCount;

  /** Codes at or above this value may have a longer suffix; only meaningful after {@link #finish}. */
  int suffixLimit = CODE_MAX;

  /** Codes at or above this value are one byte long; only meaningful after {@link #finish}. */
  int byteLimit;

  /** A 1-byte symbol usable as a separator during compression. */
  int terminator;

  FsstSymbolTable() {
    for (int i = 0; i < 256; i++) {
      // Pseudo symbols: one per byte value, standing for that byte escaped.
      symbolValues[i] = i;
      symbolDescriptors[i] = FsstCodes.icl(i | (1 << LEN_BITS), 1);
    }
    long unused = FsstCodes.icl(CODE_MASK, 1);
    for (int i = 256; i < CODE_MAX; i++) {
      symbolValues[i] = 0;
      symbolDescriptors[i] = unused;
    }
    for (int i = 0; i < HASH_TAB_SIZE; i++) {
      hashDescriptors[i] = ICL_FREE;
    }
    for (int i = 0; i < 256; i++) {
      byteCodes[i] = (char) ((1 << LEN_BITS) | i);
    }
    for (int i = 0; i < 65536; i++) {
      shortCodes[i] = (char) ((1 << LEN_BITS) | (i & 255));
    }
  }

  /** Empties the table, touching only the positions that were used. */
  void clear() {
    java.util.Arrays.fill(lengthHistogram, 0);
    for (int i = CODE_BASE; i < CODE_BASE + symbolCount; i++) {
      long val = symbolValues[i];
      int length = FsstCodes.length(symbolDescriptors[i]);
      if (length == 1) {
        int b = FsstCodes.first(val);
        byteCodes[b] = (char) ((1 << LEN_BITS) | b);
      } else if (length == 2) {
        int b2 = FsstCodes.first2(val);
        shortCodes[b2] = (char) ((1 << LEN_BITS) | (b2 & 255));
      } else {
        int idx = FsstCodes.hashBucket(val);
        hashValues[idx] = 0;
        hashDescriptors[idx] = ICL_FREE;
      }
    }
    symbolCount = 0;
  }

  private boolean hashInsert(long val, long icl) {
    int idx = FsstCodes.hashBucket(val);
    if (hashDescriptors[idx] < ICL_FREE) {
      return false; // bucket taken
    }
    hashDescriptors[idx] = icl;
    hashValues[idx] = FsstCodes.maskWord(val, icl);
    return true;
  }

  /** Copies another table over this one, so the best round's table can be kept aside. */
  void copyFrom(FsstSymbolTable other) {
    System.arraycopy(other.shortCodes, 0, shortCodes, 0, shortCodes.length);
    System.arraycopy(other.byteCodes, 0, byteCodes, 0, byteCodes.length);
    System.arraycopy(other.symbolValues, 0, symbolValues, 0, symbolValues.length);
    System.arraycopy(other.symbolDescriptors, 0, symbolDescriptors, 0, symbolDescriptors.length);
    System.arraycopy(other.hashValues, 0, hashValues, 0, hashValues.length);
    System.arraycopy(other.hashDescriptors, 0, hashDescriptors, 0, hashDescriptors.length);
    System.arraycopy(other.lengthHistogram, 0, lengthHistogram, 0, lengthHistogram.length);
    symbolCount = other.symbolCount;
    suffixLimit = other.suffixLimit;
    byteLimit = other.byteLimit;
    terminator = other.terminator;
  }

  /** Adds a symbol, returning false when its hash bucket is already taken. */
  boolean add(long val, long icl) {
    int length = FsstCodes.length(icl);
    int code = CODE_BASE + symbolCount;
    long descriptor = FsstCodes.icl(code, length);
    if (length == 1) {
      byteCodes[FsstCodes.first(val)] = (char) (code + (1 << LEN_BITS));
    } else if (length == 2) {
      shortCodes[FsstCodes.first2(val)] = (char) (code + (2 << LEN_BITS));
    } else if (!hashInsert(val, descriptor)) {
      return false;
    }
    symbolValues[code] = val;
    symbolDescriptors[code] = descriptor;
    symbolCount++;
    lengthHistogram[length - 1]++;
    return true;
  }

  /** Returns the code of the longest symbol matching the input at {@code position}. */
  int findLongestSymbol(byte[] input, int position, int end) {
    int length = Math.min(MAX_SYMBOL_LENGTH, end - position);
    long val = FsstCodes.loadSymbolBytes(input, position, length);
    long icl = FsstCodes.icl(CODE_MAX, length);
    int idx = FsstCodes.hashBucket(val);
    if (hashDescriptors[idx] <= icl && hashValues[idx] == FsstCodes.maskWord(val, hashDescriptors[idx])) {
      return FsstCodes.code(hashDescriptors[idx]);
    }
    if (length >= 2) {
      int code = shortCodes[FsstCodes.first2(val)] & CODE_MASK;
      if (code >= CODE_BASE) {
        return code;
      }
    }
    return byteCodes[FsstCodes.first(val)] & CODE_MASK;
  }

  /**
   * Renumbers codes into a single byte each and groups symbols by length.
   *
   * <p>Named {@code finalize} in the reference implementation; renamed here because that name is
   * reserved on {@link Object}. Afterwards real codes occupy {@code [0, symbolCount)} grouped by
   * length as 2,3,4,5,6,7,8 then 1, two-byte symbols with no longer suffix come first so the
   * compressor can take a shortcut, escapes in {@link #shortCodes} carry the eighth bit, and
   * {@link #byteCodes} is folded into {@link #shortCodes} so the compressor never consults it.
   *
   * <p>The reference implementation also supports zero-terminated input, which Parquet never
   * produces because values carry an explicit length. That mode is fixed off here, so terms that
   * depend on it fall away.
   */
  void finish() {
    if (symbolCount > MAX_SYMBOLS) {
      throw new IllegalStateException("FSST symbol table holds " + symbolCount + " symbols, at most "
          + MAX_SYMBOLS + " can be renumbered into one byte each");
    }
    int[] newCode = new int[256];
    int[] runningSum = new int[8];
    byteLimit = symbolCount - lengthHistogram[0];

    runningSum[0] = byteLimit; // 1-byte codes sort highest
    runningSum[1] = 0;
    for (int i = 1; i < 7; i++) {
      runningSum[i + 1] = runningSum[i] + lengthHistogram[i];
    }

    suffixLimit = runningSum[1];
    newCode[0] = 0;
    symbolValues[0] = symbolValues[CODE_BASE];
    symbolDescriptors[0] = symbolDescriptors[CODE_BASE];

    for (int i = 0, j = runningSum[2]; i < symbolCount; i++) {
      long val = symbolValues[CODE_BASE + i];
      int length = FsstCodes.length(symbolDescriptors[CODE_BASE + i]);
      // For 2-byte symbols, scan for a longer symbol sharing the same first two bytes. The scan
      // bound doubles as the answer: clearing it both records the find and ends the loop, exactly
      // as the reference implementation does.
      int scan = (length == 2) ? symbolCount : 0;
      if (scan != 0) {
        int first2 = FsstCodes.first2(val);
        for (int k = 0; k < scan; k++) {
          long otherIcl = symbolDescriptors[CODE_BASE + k];
          if (k != i
              && FsstCodes.length(otherIcl) > 1
              && first2 == FsstCodes.first2(symbolValues[CODE_BASE + k])) {
            scan = 0;
          }
        }
        newCode[i] = (scan != 0) ? suffixLimit++ : --j;
      } else {
        newCode[i] = runningSum[length - 1]++;
      }
      symbolValues[newCode[i]] = val;
      symbolDescriptors[newCode[i]] = FsstCodes.icl(newCode[i], length);
    }

    for (int i = 0; i < 256; i++) {
      if ((byteCodes[i] & CODE_MASK) >= CODE_BASE) {
        byteCodes[i] = (char) (newCode[byteCodes[i] & 0xFF] + (1 << LEN_BITS));
      } else {
        byteCodes[i] = (char) (511 + (1 << LEN_BITS));
      }
    }

    for (int i = 0; i < 65536; i++) {
      if ((shortCodes[i] & CODE_MASK) >= CODE_BASE) {
        shortCodes[i] = (char) (newCode[shortCodes[i] & 0xFF] + (shortCodes[i] & (15 << LEN_BITS)));
      } else {
        shortCodes[i] = byteCodes[i & 0xFF];
      }
    }

    for (int i = 0; i < HASH_TAB_SIZE; i++) {
      if (hashDescriptors[i] < ICL_FREE) {
        int renumbered = newCode[FsstCodes.code(hashDescriptors[i]) & 0xFF];
        hashValues[i] = symbolValues[renumbered];
        hashDescriptors[i] = symbolDescriptors[renumbered];
      }
    }
  }
}
