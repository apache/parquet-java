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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Test;

public class CorruptStatisticsTest {

  @Test
  public void testOnlyAppliesToBinary() {
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.DOUBLE));
  }

  @Test
  public void testCorruptStatistics() {
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.4.2 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.100 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.7.999 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.22rc99 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.22rc99-SNAPSHOT (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.1-SNAPSHOT (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.0t-01-abcdefg (build abcd)", PrimitiveTypeName.BINARY));

    assertTrue(CorruptStatistics.shouldIgnoreStatistics("unparseable string", PrimitiveTypeName.BINARY));

    // missing semver
    assertTrue(
        CorruptStatistics.shouldIgnoreStatistics("parquet-mr version (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(
        CorruptStatistics.shouldIgnoreStatistics("parquet-mr version  (build abcd)", PrimitiveTypeName.BINARY));

    // missing build hash
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.0 (build )", PrimitiveTypeName.BINARY));
    assertTrue(
        CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.6.0 (build)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version (build)", PrimitiveTypeName.BINARY));

    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "imapla version 1.6.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "imapla version 1.10.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.8.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.8.1 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.8.1rc3 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.8.1rc3-SNAPSHOT (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.9.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 2.0.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.9.0t-01-abcdefg (build abcd)", PrimitiveTypeName.BINARY));

    // missing semver
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("impala version (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("impala version  (build abcd)", PrimitiveTypeName.BINARY));

    // missing build hash
    assertFalse(
        CorruptStatistics.shouldIgnoreStatistics("impala version 1.6.0 (build )", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("impala version 1.6.0 (build)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("impala version (build)", PrimitiveTypeName.BINARY));
  }

  @Test
  public void testDistributionCorruptStatistics() {
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.5.0-cdh5.4.999 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.5.0-cdh5.5.0-SNAPSHOT (build 956ed6c14c611b4c4eaaa1d6e5b9a9c6d4dfa336)",
        PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.5.0-cdh5.5.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.5.0-cdh5.5.1 (build abcd)", PrimitiveTypeName.BINARY));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.5.0-cdh5.6.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.4.10 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.5.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.5.1 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.6.0 (build abcd)", PrimitiveTypeName.BINARY));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics(
        "parquet-mr version 1.7.0 (build abcd)", PrimitiveTypeName.BINARY));
  }
}
