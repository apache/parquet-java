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

import static org.apache.parquet.column.values.symboltable.fsst.FsstCodes.CODE_MAX;

import java.util.Arrays;

/**
 * Occurrence counters used while training a symbol table: how often each symbol occurs, and how
 * often each ordered pair of symbols occurs back to back.
 *
 * <p>Java port of the 64-bit {@code Counters} from the FSST reference implementation by Peter Boncz,
 * Viktor Leis and Thomas Neumann (CWI / TU Munich), MIT licensed, at https://github.com/cwida/fsst,
 * commit 89f49c580c6388acf3b6ed2a49e1bfde6c05e616.
 *
 * <p>Each counter is split into a low byte and a high part, and the pair counter's high part is only
 * four bits wide, two counters to a byte. That is not only a space optimization to be simplified
 * away: four bits saturate, so the split decides which candidate symbols survive a training round
 * and therefore which table comes out. The reference implementation has a wider variant for 32-bit
 * platforms that produces different tables; this follows the 64-bit variant, which is what other
 * implementations run.
 *
 * <p>The high part is incremented when the low byte wraps from zero rather than when it saturates,
 * which is what makes a nonzero high part equivalent to a nonzero count and lets the scans below
 * skip runs of empty counters eight bytes at a time.
 *
 * <p>Those scans deliberately read past the counter they were asked about. The reference
 * implementation lets the read spill into whichever array is laid out next; here each array carries
 * eight bytes of zero padding instead. The two agree: a spilled read only changes the result if
 * every valid counter it covers is zero, and in that case the scan has already advanced past the end
 * of the range and returns zero either way.
 */
final class FsstCounters {

  /** Slack for scans that read eight bytes from the last counter. */
  private static final int PADDING = 8;

  private final byte[] count1High = new byte[CODE_MAX + PADDING];
  private final byte[] count1Low = new byte[CODE_MAX + PADDING];
  private final byte[] count2High = new byte[CODE_MAX * (CODE_MAX / 2) + PADDING];
  private final byte[] count2Low = new byte[CODE_MAX * CODE_MAX + PADDING];

  /** Position reached by the most recent scan, which the caller must adopt. */
  private int scanPosition;

  void reset() {
    Arrays.fill(count1High, (byte) 0);
    Arrays.fill(count1Low, (byte) 0);
    Arrays.fill(count2High, (byte) 0);
    Arrays.fill(count2Low, (byte) 0);
  }

  void count1Set(int position, int value) {
    count1Low[position] = (byte) (value & 255);
    count1High[position] = (byte) (value >>> 8);
  }

  void count1Increment(int position) {
    // Post-increment on a byte array wraps, and yields the value before the increment, so this
    // raises the high part exactly when the low byte wraps back to zero.
    if (count1Low[position]++ == 0) {
      count1High[position]++;
    }
  }

  void count2Increment(int first, int second) {
    if (count2Low[first * CODE_MAX + second]++ == 0) {
      // Add one to the four-bit counter, in the low or the high nibble according to parity.
      count2High[first * (CODE_MAX / 2) + (second >> 1)] += (byte) (1 << ((second & 1) << 2));
    }
  }

  /**
   * Reads the counter for a single symbol, skipping forward over empty counters.
   *
   * @return the count, or zero once the scan leaves the range. Either way the caller must adopt
   *     {@link #scanPosition()} as its new position.
   */
  int count1Next(int position) {
    long high = loadLittleEndianLong(count1High, position);
    int zeroBytes = (high != 0) ? (Long.numberOfTrailingZeros(high) >>> 3) : 7;
    high = (high >>> (zeroBytes << 3)) & 255;
    position += zeroBytes;
    scanPosition = position;
    if (position >= CODE_MAX || high == 0) {
      return 0;
    }
    int low = count1Low[position] & 0xFF;
    if (low != 0) {
      high--; // the high part was raised early
    }
    return (int) ((high << 8) + low);
  }

  /**
   * Reads the counter for a pair of symbols, skipping forward over empty counters.
   *
   * @return the count, or zero once the scan leaves the range. Either way the caller must adopt
   *     {@link #scanPosition()} as its new second position.
   */
  int count2Next(int first, int second) {
    long high = loadLittleEndianLong(count2High, first * (CODE_MAX / 2) + (second >> 1));
    high >>>= ((second & 1) << 2); // an odd position starts halfway into its byte
    int zeroNibbles = (high != 0) ? (Long.numberOfTrailingZeros(high) >>> 2) : (15 - (second & 1));
    high = (high >>> (zeroNibbles << 2)) & 15;
    second += zeroNibbles;
    scanPosition = second;
    if (second >= CODE_MAX || high == 0) {
      return 0;
    }
    int low = count2Low[first * CODE_MAX + second] & 0xFF;
    if (low != 0) {
      high--; // the high part was raised early
    }
    return (int) ((high << 8) + low);
  }

  int scanPosition() {
    return scanPosition;
  }

  /** Size of the buffer {@link #backupSingleCounts} needs. */
  static int backupSize() {
    return 2 * CODE_MAX;
  }

  /** Saves the single-symbol counters, so the best round's counts can be brought back. */
  void backupSingleCounts(byte[] buffer) {
    System.arraycopy(count1High, 0, buffer, 0, CODE_MAX);
    System.arraycopy(count1Low, 0, buffer, CODE_MAX, CODE_MAX);
  }

  void restoreSingleCounts(byte[] buffer) {
    System.arraycopy(buffer, 0, count1High, 0, CODE_MAX);
    System.arraycopy(buffer, CODE_MAX, count1Low, 0, CODE_MAX);
  }

  private static long loadLittleEndianLong(byte[] source, int offset) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value |= (long) (source[offset + i] & 0xFF) << (i * 8);
    }
    return value;
  }
}
