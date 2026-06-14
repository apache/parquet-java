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
package org.apache.parquet.column.values.fallback;

import static org.junit.Assert.assertTrue;

import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Demonstrates the bias in PARQUET-3479: when the dictionary check fires on a later page,
 * it compares the CUMULATIVE dictionary size against only the CURRENT page's raw bytes,
 * causing spurious fallback even when dictionary encoding is cumulatively beneficial.
 *
 * <p>Scenario: 100 ints per page, 50 new distinct values per page, 2 pages before check.
 * <ul>
 *   <li>dictionaryByteSize = 100 distinct × 4 bytes = 400 bytes (cumulative after 2 pages)</li>
 *   <li>Per-page rawDataByteSize = 100 × 4 = 400 bytes</li>
 *   <li>Page 0 encoded: 80 bytes (RLE, 6-bit width for values 0..49)</li>
 *   <li>Page 1 encoded: 93 bytes (RLE, 7-bit width for values 50..99)</li>
 * </ul>
 *
 * <p>BUG (per-page on page 1): encoded(93) + dict(400) = 493 >= pageRaw(400) → FALLBACK
 * <p>CORRECT (cumulative): totalEncoded(173) + dict(400) = 573 < totalRaw(800) → KEEP
 */
public class TestFallbackCumulativeBias {

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  /**
   * Baseline: With threshold=0 and low cardinality data repeated on page 1,
   * dictionary IS kept because the full dictionary cost is small relative to raw data.
   * This proves the dictionary is genuinely beneficial for this type of data.
   */
  @Test
  public void testBaselineDictionaryKeptWithThresholdZero() throws Exception {
    FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> writer = createWriter(0);
    try {
      // 100 values, all mod 5 → only 5 distinct values, dict = 20 bytes, raw = 400 bytes
      for (int i = 0; i < 100; i++) {
        writer.writeInteger(i % 5);
      }
      writer.getBytes();
      assertTrue(
          "Baseline: dictionary should be kept with low cardinality (threshold=0)",
          writer.getEncoding().usesDictionary());
    } finally {
      writer.close();
    }
  }

  /**
   * BUG DEMONSTRATION: Even with genuinely-beneficial dictionary data across all pages,
   * a delayed threshold check causes spurious fallback because the cumulative dictionary
   * size is compared against only a single page's raw bytes.
   *
   * <p>This test uses moderate-cardinality data where each page introduces new distinct values.
   * The dictionary grows cumulatively but encoding is still more efficient than raw overall.
   * The test asserts CORRECT behavior (dictionary kept) and FAILS on current buggy code.
   */
  @Test
  public void testDelayedCheckShouldNotCauseFallbackWhenDictionaryIsBeneficial() throws Exception {
    // Threshold = 800 bytes. Each page has 100 ints × 4 = 400 raw bytes.
    // Cumulative crosses 800 on page 2's getBytes() (page0=400, page1=800 >= 800).
    // Actually: cumulativeRawBytes is updated INSIDE getBytes() via addExact, so:
    //   After page 0 getBytes(): cumulative = 400 (< 800, no check)
    //   After page 1 getBytes(): cumulative = 800 (>= 800, CHECK FIRES)
    long threshold = 800;
    FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> writer = createWriter(threshold);
    try {
      // Page 0: 100 values cycling 0..49 (50 distinct). Dict = 200 bytes.
      for (int i = 0; i < 100; i++) {
        writer.writeInteger(i % 50);
      }
      writer.getBytes(); // cumulative = 400, < 800, no check
      writer.reset();

      // Page 1: 100 values cycling 50..99 (50 new distinct). Dict grows to 400 bytes.
      for (int i = 0; i < 100; i++) {
        writer.writeInteger(50 + (i % 50));
      }
      writer.getBytes(); // cumulative = 800, >= 800, CHECK FIRES HERE
      // At this point:
      //   dictionaryByteSize = 100 × 4 = 400 (cumulative, all distinct values seen so far)
      //   rawDataByteSize (current page) = 100 × 4 = 400
      //   bytes.size() (current page encoded) ≈ ~115 bytes (RLE, 7-bit width for 100 values)
      //
      // BUG comparison: encoded(~115) + dict(400) = ~515 >= pageRaw(400) → FALLBACK!
      // CORRECT comparison: totalEncoded(~230) + dict(400) = ~630 < totalRaw(800) → KEEP!
      Encoding enc = writer.getEncoding();

      assertTrue(
          "Dictionary encoding should be preserved when it is cumulatively beneficial, "
              + "even when the check fires on a later page (PARQUET-3479 bias bug). "
              + "The per-page comparison incorrectly charges the full cumulative dictionary "
              + "against a single page's raw bytes.",
          enc.usesDictionary());
    } finally {
      writer.close();
    }
  }

  private FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> createWriter(long threshold) {
    int dictPageSize = 1024 * 1024; // large enough to not trigger shouldFallBack()
    PlainIntegerDictionaryValuesWriter dictWriter = new PlainIntegerDictionaryValuesWriter(
        dictPageSize, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN_DICTIONARY, allocator);
    PlainValuesWriter plainWriter = new PlainValuesWriter(1024, 1024 * 1024, allocator);
    return FallbackValuesWriter.of(dictWriter, plainWriter, threshold);
  }

  /**
   * Threshold=0: a non-beneficial dictionary still falls back on page 1 (backward compat).
   * High cardinality (all unique) means encodedSize + dictSize >= rawSize → FALLBACK.
   */
  @Test
  public void testThresholdZeroNonBeneficialFallsBack() throws Exception {
    FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> writer = createWriter(0);
    try {
      // 100 unique values → dict = 400 bytes, encoded ≈ 100 bytes, raw = 400.
      // encoded + dict = ~500 >= 400 → fallback
      for (int i = 0; i < 100; i++) {
        writer.writeInteger(i);
      }
      writer.getBytes();
      assertTrue(
          "Threshold=0: non-beneficial dictionary should fall back on page 1",
          !writer.getEncoding().usesDictionary());
    } finally {
      writer.close();
    }
  }

  /**
   * Adversarial: high-cardinality data with a delayed threshold STILL correctly falls back.
   * Proves the cumulative fix doesn't blanket-keep dictionaries.
   */
  @Test
  public void testHighCardinalityWithDelayedThresholdStillFallsBack() throws Exception {
    // Threshold = 800. Each page: 100 unique ints × 4 = 400 raw bytes.
    long threshold = 800;
    FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> writer = createWriter(threshold);
    try {
      // Page 0: 100 unique values 0..99
      for (int i = 0; i < 100; i++) {
        writer.writeInteger(i);
      }
      writer.getBytes(); // cumulative = 400 < 800, no check
      writer.reset();

      // Page 1: 100 unique values 100..199, dict now has 200 entries = 800 bytes
      for (int i = 100; i < 200; i++) {
        writer.writeInteger(i);
      }
      writer.getBytes(); // cumulative = 800, check fires
      // cumulativeEncoded(page0 + page1) + dict(800) vs cumulativeRaw(800)
      // Even cumulatively: encoded(~200) + dict(800) = ~1000 >= 800 → FALLBACK
      assertTrue(
          "High-cardinality data should still trigger fallback even with cumulative comparison",
          !writer.getEncoding().usesDictionary());
    } finally {
      writer.close();
    }
  }

  /**
   * resetDictionary() zeroes the accumulator: second column chunk's decision
   * is independent of the first chunk.
   */
  @Test
  public void testResetDictionaryIsolatesChunks() throws Exception {
    long threshold = 800;
    FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> writer = createWriter(threshold);
    try {
      // Chunk 1: low cardinality → keeps dictionary (so initialWriter.resetDictionary is called)
      for (int i = 0; i < 100; i++) {
        writer.writeInteger(i % 5);
      }
      writer.getBytes();
      writer.reset();
      for (int i = 0; i < 100; i++) {
        writer.writeInteger(i % 5);
      }
      writer.getBytes(); // check fires: cumulative=800, dict=20, encoded tiny → keep
      assertTrue(
          "Chunk 1 should keep dictionary (low cardinality)",
          writer.getEncoding().usesDictionary());

      // Reset for chunk 2 (reset per-page state, then reset dictionary for new column chunk)
      writer.reset();
      writer.resetDictionary();

      // Chunk 2: high cardinality → should fall back (proves accumulator was zeroed,
      // not left with chunk 1's favorable cumulative values)
      for (int i = 0; i < 100; i++) {
        writer.writeInteger(i);
      }
      writer.getBytes();
      writer.reset();
      for (int i = 100; i < 200; i++) {
        writer.writeInteger(i);
      }
      writer.getBytes(); // check fires with fresh accumulators
      assertTrue(
          "Chunk 2 should fall back (high cardinality, accumulators were reset)",
          !writer.getEncoding().usesDictionary());
    } finally {
      writer.close();
    }
  }

  /**
   * Helper test that prints exact encoded sizes to verify arithmetic witness.
   * Uses DictionaryValuesWriter directly (not via FallbackValuesWriter) to measure
   * without triggering fallback logic.
   */
  @Test
  public void testArithmeticWitness() throws Exception {
    int dictPageSize = 1024 * 1024;
    PlainIntegerDictionaryValuesWriter dictWriter = new PlainIntegerDictionaryValuesWriter(
        dictPageSize, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN_DICTIONARY, allocator);
    try {
      // Page 0: 100 values cycling 0..49
      for (int i = 0; i < 100; i++) {
        dictWriter.writeInteger(i % 50);
      }
      long page0Encoded = dictWriter.getBytes().size();
      dictWriter.reset();

      // Page 1: 100 values cycling 50..99
      for (int i = 0; i < 100; i++) {
        dictWriter.writeInteger(50 + (i % 50));
      }
      long page1Encoded = dictWriter.getBytes().size();

      long dictSize = 100 * 4; // 100 distinct ints × 4 bytes = 400
      long pageRaw = 100 * 4; // 100 ints × 4 bytes = 400
      long totalRaw = pageRaw * 2; // 800

      // Verify the arithmetic demonstrates the bias
      assertTrue(
          "Bug comparison should trigger fallback: encoded(" + page1Encoded + ") + dict(" + dictSize + ") = "
              + (page1Encoded + dictSize) + " >= pageRaw(" + pageRaw + ")",
          (page1Encoded + dictSize) >= pageRaw);
      assertTrue(
          "Correct cumulative comparison should keep dictionary: totalEncoded("
              + (page0Encoded + page1Encoded) + ") + dict(" + dictSize + ") = "
              + (page0Encoded + page1Encoded + dictSize) + " < totalRaw(" + totalRaw + ")",
          (page0Encoded + page1Encoded + dictSize) < totalRaw);
    } finally {
      dictWriter.close();
    }
  }
}
