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

/**
 * Constants and bit-field helpers for the FSST symbol representation.
 *
 * <p>This file is a Java port of parts of the FSST reference implementation by Peter Boncz,
 * Viktor Leis and Thomas Neumann (CWI / TU Munich), distributed under the MIT license and
 * available at https://github.com/cwida/fsst. The port follows commit
 * 89f49c580c6388acf3b6ed2a49e1bfde6c05e616. Encoding decisions are reproduced exactly, because
 * the symbol table a writer produces has to match what other implementations produce for the
 * same input.
 *
 * <p>A symbol is a byte sequence of length 1 to 8, held as a little-endian {@code long} together
 * with a packed descriptor called {@code icl}: {@code (length << 28) | (code << 16) |
 * ignoredBits}, where {@code ignoredBits} is {@code (8 - length) * 8}. The descriptor is kept in
 * a {@code long} rather than an {@code int} on purpose: at length 8 the length field alone is
 * {@code 8 << 28}, which overflows a signed 32-bit integer. Because every descriptor fits in 32
 * bits, signed {@code long} comparison of two descriptors matches the unsigned comparison the
 * reference implementation performs.
 */
final class FsstCodes {

  private FsstCodes() {}

  /** Maximum number of bytes in one symbol. */
  static final int MAX_SYMBOL_LENGTH = 8;

  /** Width of the length field in the packed values held by the code lookup tables. */
  static final int LEN_BITS = 12;

  static final int CODE_BITS = 9;

  /** Codes below this value are pseudo codes standing for an escaped single byte. */
  static final int CODE_BASE = 256;

  /** One past the highest representable code; also marks a symbol with no code assigned yet. */
  static final int CODE_MAX = 1 << CODE_BITS;

  static final int CODE_MASK = CODE_MAX - 1;

  /** Number of buckets in the symbol hash table, which holds symbols of three bytes or more. */
  static final int HASH_TAB_SIZE = 1 << 10;

  /** Descriptor value marking a free hash bucket: length 15 and an unassigned code. */
  static final long ICL_FREE = (15L << 28) | ((long) CODE_MASK << 16);

  /** Highest number of symbols a table may hold, so that every code fits in one byte. */
  static final int MAX_SYMBOLS = 255;

  private static final long HASH_PRIME = 2971215073L;
  private static final int HASH_SHIFT = 15;

  /**
   * The reference implementation's hash, used both for symbol lookup and to drive the sampler.
   *
   * <p>The shift must be unsigned. For symbol lookup the input is the next three bytes, so the
   * product cannot reach the sign bit and the distinction does not arise; the sampler chains this
   * function over full 64-bit values, where a signed shift would send the whole sequence down a
   * different path and produce a different symbol table.
   */
  static long hash(long w) {
    long product = w * HASH_PRIME;
    return product ^ (product >>> HASH_SHIFT);
  }

  /** Packs a symbol descriptor from a code and a length. */
  static long icl(int code, int length) {
    return ((long) length << 28) | ((long) code << 16) | ((long) (MAX_SYMBOL_LENGTH - length) * 8);
  }

  static int length(long icl) {
    return (int) (icl >>> 28);
  }

  static int code(long icl) {
    return (int) ((icl >>> 16) & CODE_MASK);
  }

  /** Number of high bits to clear in an input word before comparing it against a symbol. */
  static int ignoredBits(long icl) {
    return (int) (icl & 0xFF);
  }

  /** The symbol's first byte, as an unsigned value. */
  static int first(long val) {
    return (int) (val & 0xFF);
  }

  /** The symbol's first two bytes, as an unsigned value. */
  static int first2(long val) {
    return (int) (val & 0xFFFF);
  }

  /** Hash bucket for a symbol, keyed on its first three bytes. */
  static int hashBucket(long val) {
    return (int) (hash(val & 0xFFFFFF) & (HASH_TAB_SIZE - 1));
  }

  /** Builds the little-endian word for the first {@code length} bytes at {@code offset}. */
  static long loadSymbolBytes(byte[] src, int offset, int length) {
    long val = 0;
    for (int i = 0; i < length; i++) {
      val |= (long) (src[offset + i] & 0xFF) << (i * 8);
    }
    return val;
  }

  /**
   * Reads eight bytes little-endian, substituting zeros past {@code limit}.
   *
   * <p>The reference implementation reads eight bytes unconditionally and relies on the input
   * buffer being padded. Java cannot read out of bounds, so the tail is zero-filled here; callers
   * that must reproduce the reference behaviour byte for byte pad their input instead of relying
   * on this.
   */
  static long loadWord(byte[] src, int offset, int limit) {
    long val = 0;
    int n = Math.min(MAX_SYMBOL_LENGTH, limit - offset);
    for (int i = 0; i < n; i++) {
      val |= (long) (src[offset + i] & 0xFF) << (i * 8);
    }
    return val;
  }

  /**
   * Reads eight bytes little-endian with no bounds reasoning beyond the array itself.
   *
   * <p>For the compressor's inner loop, which works out of a buffer padded past its logical end so
   * that the read is always in bounds. A byte-array view is used rather than eight shifts because
   * this is the single hottest read in the codec.
   */
  static long loadWordUnchecked(byte[] src, int offset) {
    return (long) LITTLE_ENDIAN_LONG.get(src, offset);
  }

  private static final java.lang.invoke.VarHandle LITTLE_ENDIAN_LONG =
      java.lang.invoke.MethodHandles.byteArrayViewVarHandle(long[].class, java.nio.ByteOrder.LITTLE_ENDIAN);

  /** Clears the high bits of {@code word} that a symbol with this descriptor ignores. */
  static long maskWord(long word, long icl) {
    return word & (-1L >>> ignoredBits(icl));
  }
}
