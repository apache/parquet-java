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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

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

    assertThat(stats2.dictStats).as("Dictionary stats should be correct").isEmpty();
    assertThat(stats2.dataStats).as("Data stats size should be correct").hasSize(1);
    assertThat(stats2.dataStats.get(Encoding.PLAIN).intValue())
        .as("Data stats content should be correct")
        .isEqualTo(4);

    assertThat(stats1.dictStats)
        .as("Dictionary stats size should be correct after reuse")
        .hasSize(1);
    assertThat(stats1.dictStats.get(Encoding.PLAIN).intValue())
        .as("Dictionary stats content should be correct")
        .isEqualTo(1);

    assertThat(stats1.dataStats)
        .as("Data stats size should be correct after reuse")
        .hasSize(2);
    assertThat(stats1.dataStats.get(Encoding.RLE_DICTIONARY).intValue())
        .as("Data stats content should be correct")
        .isEqualTo(3);
    assertThat(stats1.dataStats.get(Encoding.DELTA_BYTE_ARRAY).intValue())
        .as("Data stats content should be correct")
        .isEqualTo(2);
  }

  @Test
  public void testNoPages() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    EncodingStats stats = builder.build();

    assertThat(stats.usesV2Pages()).isFalse();
    assertThat(stats.hasDictionaryEncodedPages())
        .as("Should not have dictionary-encoded pages")
        .isFalse();
    assertThat(stats.hasNonDictionaryEncodedPages())
        .as("Should not have non-dictionary pages")
        .isFalse();
    assertThat(stats.hasDictionaryPages())
        .as("Should not have dictionary pages")
        .isFalse();
  }

  @Test
  public void testNoDataPages() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.addDictEncoding(Encoding.PLAIN_DICTIONARY);
    EncodingStats stats = builder.build();

    assertThat(stats.usesV2Pages()).isFalse();
    assertThat(stats.hasDictionaryEncodedPages())
        .as("Should not have dictionary-encoded pages")
        .isFalse();
    assertThat(stats.hasNonDictionaryEncodedPages())
        .as("Should not have non-dictionary pages")
        .isFalse();
    assertThat(stats.hasDictionaryPages())
        .as("Should have dictionary pages")
        .isTrue();
  }

  @Test
  public void testV1AllDictionary() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.addDictEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN_DICTIONARY);
    EncodingStats stats = builder.build();

    assertThat(stats.usesV2Pages()).isFalse();
    assertThat(stats.hasDictionaryEncodedPages())
        .as("Should have dictionary-encoded pages")
        .isTrue();
    assertThat(stats.hasNonDictionaryEncodedPages())
        .as("Should not have non-dictionary pages")
        .isFalse();
    assertThat(stats.hasDictionaryPages())
        .as("Should have dictionary pages")
        .isTrue();
  }

  @Test
  public void testV1NoDictionary() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.addDataEncoding(Encoding.PLAIN);
    EncodingStats stats = builder.build();

    assertThat(stats.usesV2Pages()).isFalse();
    assertThat(stats.hasDictionaryEncodedPages())
        .as("Should not have dictionary-encoded pages")
        .isFalse();
    assertThat(stats.hasNonDictionaryEncodedPages())
        .as("Should have non-dictionary pages")
        .isTrue();
    assertThat(stats.hasDictionaryPages())
        .as("Should not have dictionary pages")
        .isFalse();
  }

  @Test
  public void testV1Fallback() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.addDictEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN_DICTIONARY);
    builder.addDataEncoding(Encoding.PLAIN);
    EncodingStats stats = builder.build();

    assertThat(stats.usesV2Pages()).isFalse();
    assertThat(stats.hasDictionaryEncodedPages())
        .as("Should have dictionary-encoded pages")
        .isTrue();
    assertThat(stats.hasNonDictionaryEncodedPages())
        .as("Should have non-dictionary pages")
        .isTrue();
    assertThat(stats.hasDictionaryPages())
        .as("Should have dictionary pages")
        .isTrue();
  }

  @Test
  public void testV2AllDictionary() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.withV2Pages();
    builder.addDictEncoding(Encoding.PLAIN);
    builder.addDataEncoding(Encoding.RLE_DICTIONARY);
    EncodingStats stats = builder.build();

    assertThat(stats.usesV2Pages()).isTrue();
    assertThat(stats.hasDictionaryEncodedPages())
        .as("Should have dictionary-encoded pages")
        .isTrue();
    assertThat(stats.hasNonDictionaryEncodedPages())
        .as("Should not have non-dictionary pages")
        .isFalse();
    assertThat(stats.hasDictionaryPages())
        .as("Should have dictionary pages")
        .isTrue();
  }

  @Test
  public void testV2NoDictionary() {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    builder.withV2Pages();
    builder.addDataEncoding(Encoding.DELTA_BINARY_PACKED);
    builder.addDataEncoding(Encoding.DELTA_BINARY_PACKED);
    EncodingStats stats = builder.build();

    assertThat(stats.usesV2Pages()).isTrue();
    assertThat(stats.hasDictionaryEncodedPages())
        .as("Should not have dictionary-encoded pages")
        .isFalse();
    assertThat(stats.hasNonDictionaryEncodedPages())
        .as("Should have non-dictionary pages")
        .isTrue();
    assertThat(stats.hasDictionaryPages())
        .as("Should not have dictionary pages")
        .isFalse();
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

    assertThat(stats.usesV2Pages()).isTrue();
    assertThat(stats.hasDictionaryEncodedPages())
        .as("Should have dictionary-encoded pages")
        .isTrue();
    assertThat(stats.hasNonDictionaryEncodedPages())
        .as("Should have non-dictionary pages")
        .isTrue();
    assertThat(stats.hasDictionaryPages())
        .as("Should have dictionary pages")
        .isTrue();
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

    assertThat(stats.getNumDictionaryPagesEncodedAs(Encoding.PLAIN))
        .as("Count should match")
        .isEqualTo(1);
    assertThat(stats.getNumDictionaryPagesEncodedAs(Encoding.PLAIN_DICTIONARY))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDictionaryPagesEncodedAs(Encoding.RLE))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDictionaryPagesEncodedAs(Encoding.BIT_PACKED))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDictionaryPagesEncodedAs(Encoding.DELTA_BYTE_ARRAY))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDictionaryPagesEncodedAs(Encoding.DELTA_BINARY_PACKED))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDictionaryPagesEncodedAs(Encoding.DELTA_LENGTH_BYTE_ARRAY))
        .as("Count should match")
        .isEqualTo(0);

    assertThat(stats.getNumDataPagesEncodedAs(Encoding.RLE_DICTIONARY))
        .as("Count should match")
        .isEqualTo(5);
    assertThat(stats.getNumDataPagesEncodedAs(Encoding.DELTA_BYTE_ARRAY))
        .as("Count should match")
        .isEqualTo(2);
    assertThat(stats.getNumDataPagesEncodedAs(Encoding.RLE))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDataPagesEncodedAs(Encoding.BIT_PACKED))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDataPagesEncodedAs(Encoding.PLAIN))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDataPagesEncodedAs(Encoding.PLAIN_DICTIONARY))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDataPagesEncodedAs(Encoding.DELTA_BINARY_PACKED))
        .as("Count should match")
        .isEqualTo(0);
    assertThat(stats.getNumDataPagesEncodedAs(Encoding.DELTA_LENGTH_BYTE_ARRAY))
        .as("Count should match")
        .isEqualTo(0);
  }
}
