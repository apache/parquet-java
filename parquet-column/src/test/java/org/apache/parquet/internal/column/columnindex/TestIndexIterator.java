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
package org.apache.parquet.internal.column.columnindex;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link IndexIterator}.
 */
public class TestIndexIterator {
  @Test
  public void testAll() {
    assertThat(IndexIterator.all(10)).toIterable().containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testFilter() {
    assertThat(IndexIterator.filter(30, value -> value % 3 == 0))
        .toIterable()
        .containsExactly(0, 3, 6, 9, 12, 15, 18, 21, 24, 27);
  }

  @Test
  public void testFilterTranslate() {
    assertThat(IndexIterator.filterTranslate(20, value -> value < 5, Math::negateExact))
        .toIterable()
        .containsExactly(0, -1, -2, -3, -4);
  }

  @Test
  public void testRangeTranslate() {
    assertThat(IndexIterator.rangeTranslate(11, 18, i -> i - 10))
        .toIterable()
        .containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testUnion() {
    // Test deduplication of intersecting ranges
    assertThat(IndexIterator.union(
            IndexIterator.rangeTranslate(0, 7, i -> i), IndexIterator.rangeTranslate(4, 10, i -> i)))
        .toIterable()
        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // Test inversion of LHS and RHS
    assertThat(IndexIterator.union(
            IndexIterator.rangeTranslate(4, 10, i -> i), IndexIterator.rangeTranslate(0, 7, i -> i)))
        .toIterable()
        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // Test non-intersecting ranges
    assertThat(IndexIterator.union(
            IndexIterator.rangeTranslate(2, 5, i -> i), IndexIterator.rangeTranslate(8, 10, i -> i)))
        .toIterable()
        .containsExactly(2, 3, 4, 5, 8, 9, 10);
  }

  @Test
  public void testIntersection() {
    // Case 1: some overlap between LHS and RHS
    // LHS: [0, 1, 2, 3, 4, 5, 6, 7], RHS: [4, 5, 6, 7, 8, 9, 10]
    assertThat(IndexIterator.intersection(
            IndexIterator.rangeTranslate(0, 7, i -> i), IndexIterator.rangeTranslate(4, 10, i -> i)))
        .toIterable()
        .containsExactly(4, 5, 6, 7);

    // Test inversion of LHS and RHS
    assertThat(IndexIterator.intersection(
            IndexIterator.rangeTranslate(4, 10, i -> i), IndexIterator.rangeTranslate(0, 7, i -> i)))
        .toIterable()
        .containsExactly(4, 5, 6, 7);

    // Case 2: Single point of overlap at end of iterator
    // LHS: [1, 3, 5, 7], RHS: [0, 2, 4, 6, 7]
    assertThat(IndexIterator.intersection(
            IndexIterator.filter(8, i -> i % 2 == 1), IndexIterator.filter(8, i -> i % 2 == 0 || i == 7)))
        .toIterable()
        .containsExactly(7);

    // Test inversion of LHS and RHS
    assertThat(IndexIterator.intersection(
            IndexIterator.filter(8, i -> i % 2 == 0 || i == 7), IndexIterator.filter(8, i -> i % 2 == 1)))
        .toIterable()
        .containsExactly(7);

    // Test no intersection between ranges
    // LHS: [2, 3, 4, 5], RHS: [8, 9, 10]
    assertThat(IndexIterator.intersection(
            IndexIterator.rangeTranslate(2, 5, i -> i), IndexIterator.rangeTranslate(8, 10, i -> i)))
        .toIterable()
        .isEmpty();

    // Test inversion of LHS and RHS
    assertThat(IndexIterator.intersection(
            IndexIterator.rangeTranslate(8, 10, i -> i), IndexIterator.rangeTranslate(2, 5, i -> i)))
        .toIterable()
        .isEmpty();
  }
}
