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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFallbackValuesWriter {

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
   * With threshold=0, the check fires on the first page and falls back for high-cardinality data.
   */
  @Test
  public void testThresholdZeroFallsBackImmediately() throws Exception {
    int dictPageSize = 1024 * 1024;

    PlainIntegerDictionaryValuesWriter dictWriter = new PlainIntegerDictionaryValuesWriter(
        dictPageSize, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN_DICTIONARY, allocator);
    PlainValuesWriter plainWriter = new PlainValuesWriter(1024, 1024 * 1024, allocator);
    FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> writer =
        FallbackValuesWriter.of(dictWriter, plainWriter, 0);

    try {
      for (int i = 0; i < 1000; i++) {
        writer.writeInteger(i);
      }
      writer.getBytes();

      assertFalse(
          "Should fall back to plain encoding with threshold=0 and high cardinality",
          writer.getEncoding().usesDictionary());
    } finally {
      writer.close();
    }
  }

  /**
   * With a large threshold, the check never fires and dictionary encoding is preserved.
   */
  @Test
  public void testLargeThresholdPreservesDictionary() throws Exception {
    int dictPageSize = 1024 * 1024;

    PlainIntegerDictionaryValuesWriter dictWriter = new PlainIntegerDictionaryValuesWriter(
        dictPageSize, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN_DICTIONARY, allocator);
    PlainValuesWriter plainWriter = new PlainValuesWriter(1024, 1024 * 1024, allocator);
    FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> writer =
        FallbackValuesWriter.of(dictWriter, plainWriter, Long.MAX_VALUE);

    try {
      for (int i = 0; i < 1000; i++) {
        writer.writeInteger(i);
      }
      writer.getBytes();

      assertTrue(
          "Dictionary encoding should be preserved with large threshold",
          writer.getEncoding().usesDictionary());
    } finally {
      writer.close();
    }
  }

  /**
   * Threshold is crossed only after a reset() (page flush). cumulativeRawBytes accumulates
   * across pages while rawDataByteSize resets per page.
   */
  @Test
  public void testThresholdCrossedAfterReset() throws Exception {
    int dictPageSize = 1024 * 1024;
    long threshold = 500;

    PlainIntegerDictionaryValuesWriter dictWriter = new PlainIntegerDictionaryValuesWriter(
        dictPageSize, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN_DICTIONARY, allocator);
    PlainValuesWriter plainWriter = new PlainValuesWriter(1024, 1024 * 1024, allocator);
    FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> writer =
        FallbackValuesWriter.of(dictWriter, plainWriter, threshold);

    try {
      // Write ~300 bytes (75 ints * 4 bytes = 300) — below threshold
      for (int i = 0; i < 75; i++) {
        writer.writeInteger(i);
      }
      // Simulate page flush — check should NOT fire (cumulative = 300 < 500)
      writer.getBytes();
      assertTrue(
          "Should still use dictionary before threshold is crossed",
          writer.getEncoding().usesDictionary());
      writer.reset();

      // Write another ~300 bytes (75 ints * 4 = 300, cumulative now 600 > 500)
      for (int i = 75; i < 150; i++) {
        writer.writeInteger(i);
      }
      // Check SHOULD fire now and fall back (high cardinality, bad compression)
      writer.getBytes();
      assertFalse(
          "Should fall back after cumulative bytes cross threshold",
          writer.getEncoding().usesDictionary());
    } finally {
      writer.close();
    }
  }
}
