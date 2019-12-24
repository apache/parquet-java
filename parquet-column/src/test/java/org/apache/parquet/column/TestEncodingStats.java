/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.column;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestEncodingStats {
  @Test
  public void testReusedBuilder() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.withV2Pages();
    builder.addDictEncoding(Encoding.PLAIN);
    builder.addDataEncoding(Encoding.RLE_DICTIONARY, 3);
    builder.addDataEncoding(Encoding.DELTA_BYTE_ARRAY);
    builder.addDataEncoding(Encoding.DELTA_BYTE_ARRAY);
    EncodingStats stats1 = builder.build();

    builder.clear();
    builder.addDataEncoding(Encoding.PLAIN);
    builder.addDataEncoding(Encoding.PLAIN);
    builder.addDataEncoding(Encoding.PLAIN);
    builder.addDataEncoding(Encoding.PLAIN);
    EncodingStats stats2 = builder.build();

    assertEquals("Dictionary stats should be correct", 0,
        stats2.dictStats.size());
    assertEquals("Data stats size should be correct", 1,
        stats2.dataStats.size());
    assertEquals("Data stats content should be correct", 4,
        stats2.dataStats.get(Encoding.PLAIN).intValue());

    assertEquals("Dictionary stats size should be correct after reuse",
        1, stats1.dictStats.size());
    assertEquals("Dictionary stats content should be correct", 1,
        stats1.dictStats.get(Encoding.PLAIN).intValue());

    assertEquals("Data stats size should be correct after reuse", 2,
        stats1.dataStats.size());
    assertEquals("Data stats content should be correct", 3,
        stats1.dataStats.get(Encoding.RLE_DICTIONARY).intValue());
    assertEquals("Data stats content should be correct", 2,
        stats1.dataStats.get(Encoding.DELTA_BYTE_ARRAY).intValue());
  }

  @Test
  public void testNoPages() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    EncodingStats stats = builder.build();

    assertFalse(stats.usesV2Pages());
    assertFalse("Should not have dictionary-encoded pages", stats.hasDictionaryEncodedPages());
    assertFalse("Should not have non-dictionary pages", stats.hasNonDictionaryEncodedPages());
    assertFalse("Should not have dictionary pages", stats.hasDictionaryPages());
  }

  @Test
  public void testNoDataPages() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.addDictEncoding(Encoding.PLAIN_DICTIONARY);
    EncodingStats stats = builder.build();

    assertFalse(stats.usesV2Pages());
    assertFalse("Should not have dictionary-encoded pages", stats.hasDictionaryEncodedPages());
    assertFalse("Should not have non-dictionary pages", stats.hasNonDictionaryEncodedPages());
    assertTrue("Should have dictionary pages", stats.hasDictionaryPages());
  }

  @Test
  public void testV1AllDictionary() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.addDictEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN_DICTIONARY);
    EncodingStats stats = builder.build();

    assertFalse(stats.usesV2Pages());
    assertTrue("Should have dictionary-encoded pages", stats.hasDictionaryEncodedPages());
    assertFalse("Should not have non-dictionary pages", stats.hasNonDictionaryEncodedPages());
    assertTrue("Should have dictionary pages", stats.hasDictionaryPages());
  }

  @Test
  public void testV1NoDictionary() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.addDataEncoding(Encoding.PLAIN);
    EncodingStats stats = builder.build();

    assertFalse(stats.usesV2Pages());
    assertFalse("Should not have dictionary-encoded pages", stats.hasDictionaryEncodedPages());
    assertTrue("Should have non-dictionary pages", stats.hasNonDictionaryEncodedPages());
    assertFalse("Should not have dictionary pages", stats.hasDictionaryPages());
  }

  @Test
  public void testV1Fallback() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.addDictEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN);
    EncodingStats stats = builder.build();

    assertFalse(stats.usesV2Pages());
    assertTrue("Should have dictionary-encoded pages", stats.hasDictionaryEncodedPages());
    assertTrue("Should have non-dictionary pages", stats.hasNonDictionaryEncodedPages());
    assertTrue("Should have dictionary pages", stats.hasDictionaryPages());
  }

  @Test
  public void testV2AllDictionary() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.withV2Pages();
    builder.addDictEncoding(Encoding.PLAIN);
    builder.addDataEncoding(Encoding.RLE_DICTIONARY);
    EncodingStats stats = builder.build();

    assertTrue(stats.usesV2Pages());
    assertTrue("Should have dictionary-encoded pages", stats.hasDictionaryEncodedPages());
    assertFalse("Should not have non-dictionary pages", stats.hasNonDictionaryEncodedPages());
    assertTrue("Should have dictionary pages", stats.hasDictionaryPages());
  }

  @Test
  public void testV2NoDictionary() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.withV2Pages();
    builder.addDataEncoding(Encoding.DELTA_BINARY_PACKED);
    builder.addDataEncoding(Encoding.DELTA_BINARY_PACKED);
    EncodingStats stats = builder.build();

    assertTrue(stats.usesV2Pages());
    assertFalse("Should not have dictionary-encoded pages", stats.hasDictionaryEncodedPages());
    assertTrue("Should have non-dictionary pages", stats.hasNonDictionaryEncodedPages());
    assertFalse("Should not have dictionary pages", stats.hasDictionaryPages());
  }

  @Test
  public void testV2Fallback() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.withV2Pages();
    builder.addDictEncoding(Encoding.PLAIN);
    builder.addDataEncoding(Encoding.RLE_DICTIONARY);
    builder.addDataEncoding(Encoding.DELTA_BYTE_ARRAY);
    builder.addDataEncoding(Encoding.DELTA_BYTE_ARRAY);
    EncodingStats stats = builder.build();

    assertTrue(stats.usesV2Pages());
    assertTrue("Should have dictionary-encoded pages", stats.hasDictionaryEncodedPages());
    assertTrue("Should have non-dictionary pages", stats.hasNonDictionaryEncodedPages());
    assertTrue("Should have dictionary pages", stats.hasDictionaryPages());
  }

  @Test
  public void testCounts() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.withV2Pages();
    builder.addDictEncoding(Encoding.PLAIN);
    builder.addDataEncoding(Encoding.RLE_DICTIONARY, 4);
    builder.addDataEncoding(Encoding.RLE_DICTIONARY);
    builder.addDataEncoding(Encoding.DELTA_BYTE_ARRAY);
    builder.addDataEncoding(Encoding.DELTA_BYTE_ARRAY);
    EncodingStats stats = builder.build();

    assertEquals("Count should match", 1, stats.getNumDictionaryPagesEncodedAs(Encoding.PLAIN));
    assertEquals("Count should match", 0, stats.getNumDictionaryPagesEncodedAs(Encoding.PLAIN_DICTIONARY));
    assertEquals("Count should match", 0, stats.getNumDictionaryPagesEncodedAs(Encoding.RLE));
    assertEquals("Count should match", 0, stats.getNumDictionaryPagesEncodedAs(Encoding.BIT_PACKED));
    assertEquals("Count should match", 0, stats.getNumDictionaryPagesEncodedAs(Encoding.DELTA_BYTE_ARRAY));
    assertEquals("Count should match", 0, stats.getNumDictionaryPagesEncodedAs(Encoding.DELTA_BINARY_PACKED));
    assertEquals("Count should match", 0, stats.getNumDictionaryPagesEncodedAs(Encoding.DELTA_LENGTH_BYTE_ARRAY));

    assertEquals("Count should match", 5, stats.getNumDataPagesEncodedAs(Encoding.RLE_DICTIONARY));
    assertEquals("Count should match", 2, stats.getNumDataPagesEncodedAs(Encoding.DELTA_BYTE_ARRAY));
    assertEquals("Count should match", 0, stats.getNumDataPagesEncodedAs(Encoding.RLE));
    assertEquals("Count should match", 0, stats.getNumDataPagesEncodedAs(Encoding.BIT_PACKED));
    assertEquals("Count should match", 0, stats.getNumDataPagesEncodedAs(Encoding.PLAIN));
    assertEquals("Count should match", 0, stats.getNumDataPagesEncodedAs(Encoding.PLAIN_DICTIONARY));
    assertEquals("Count should match", 0, stats.getNumDataPagesEncodedAs(Encoding.DELTA_BINARY_PACKED));
    assertEquals("Count should match", 0, stats.getNumDataPagesEncodedAs(Encoding.DELTA_LENGTH_BYTE_ARRAY));
  }
}
