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
package org.apache.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

public class CorruptStatisticsTest {

  @Test
  public void testOnlyAppliesToBinary() {
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.DOUBLE))
        .isFalse();
  }

  @Test
  public void testCorruptStatistics() {
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.4.2 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.100 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.7.999 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.22rc99 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.22rc99-SNAPSHOT (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.1-SNAPSHOT (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.0t-01-abcdefg (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();

    assertThat(CorruptStatistics.shouldIgnoreStatistics("unparseable string", PrimitiveTypeName.BINARY))
        .isTrue();

    // missing semver
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version  (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();

    // missing build hash
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.0 (build )", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.0 (build)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version (build)", PrimitiveTypeName.BINARY))
        .isTrue();

    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "imapla version 1.6.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "imapla version 1.10.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.8.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.8.1 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.8.1rc3 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.8.1rc3-SNAPSHOT (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.9.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 2.0.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.9.0t-01-abcdefg (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();

    // missing semver
    assertThat(CorruptStatistics.shouldIgnoreStatistics("impala version (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics("impala version  (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();

    // missing build hash
    assertThat(CorruptStatistics.shouldIgnoreStatistics("impala version 1.6.0 (build )", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics("impala version 1.6.0 (build)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics("impala version (build)", PrimitiveTypeName.BINARY))
        .isFalse();
  }

  @Test
  public void testShouldIgnoreStatisticsWithParsedVersion() throws Exception {
    String createdBy = "parquet-mr version 1.6.0 (build abc)";

    assertThat(CorruptStatistics.shouldIgnoreStatistics(null, null, PrimitiveTypeName.BINARY))
        .isTrue();

    assertThat(CorruptStatistics.shouldIgnoreStatistics(null, null, PrimitiveTypeName.INT32))
        .isFalse();

    ParsedVersion impala = VersionParser.parse("impala version 1.2.0 (build abc)");
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            impala, "impala version 1.2.0 (build abc)", PrimitiveTypeName.BINARY))
        .isFalse();

    ParsedVersion corrupt = VersionParser.parse(createdBy);
    assertThat(CorruptStatistics.shouldIgnoreStatistics(corrupt, createdBy, PrimitiveTypeName.BINARY))
        .isTrue();

    ParsedVersion fixed = VersionParser.parse("parquet-mr version 1.8.0 (build abc)");
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            fixed, "parquet-mr version 1.8.0 (build abc)", PrimitiveTypeName.BINARY))
        .isFalse();

    ParsedVersion newer = VersionParser.parse("parquet-mr version 1.12.0 (build abc)");
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            newer, "parquet-mr version 1.12.0 (build abc)", PrimitiveTypeName.BINARY))
        .isFalse();

    // version field present but not a valid semantic version
    ParsedVersion invalidSemver = new ParsedVersion("parquet-mr", "not-a-semver", "abc");
    assertThat(invalidSemver.hasSemanticVersion()).isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            invalidSemver, "parquet-mr version not-a-semver (build abc)", PrimitiveTypeName.BINARY))
        .isTrue();

    // empty version field
    ParsedVersion emptyVersion = new ParsedVersion("parquet-mr", "", "abc");
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            emptyVersion, "parquet-mr version (build abc)", PrimitiveTypeName.BINARY))
        .isTrue();
  }

  @Test
  public void testDistributionCorruptStatistics() {
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.5.0-cdh5.4.999 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.5.0-cdh5.5.0-SNAPSHOT (build 956ed6c14c611b4c4eaaa1d6e5b9a9c6d4dfa336)",
            PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.5.0-cdh5.5.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.5.0-cdh5.5.1 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.5.0-cdh5.6.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isFalse();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.4.10 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.5.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.5.1 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
    assertThat(CorruptStatistics.shouldIgnoreStatistics(
            "parquet-mr version 1.7.0 (build abcd)", PrimitiveTypeName.BINARY))
        .isTrue();
  }
}
