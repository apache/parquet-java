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
package org.apache.parquet.filter2.columnindex;

import static org.apache.parquet.filter2.columnindex.RowRanges.intersection;
import static org.apache.parquet.filter2.columnindex.RowRanges.union;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.PrimitiveIterator;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.junit.Test;

/**
 * Unit test for {@link RowRanges}
 */
public class TestRowRanges {
  @SuppressWarnings("deprecation")
  private static RowRanges buildRanges(long... rowIndexes) {
    if (rowIndexes.length == 0) {
      return RowRanges.EMPTY;
    }
    OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
    for (int i = 0, n = rowIndexes.length; i < n; i += 2) {
      long from = rowIndexes[i];
      long to = rowIndexes[i + 1];
      builder.add(0, 0, from);
      builder.add(0, 0, to + 1);
    }
    PrimitiveIterator.OfInt pageIndexes = new PrimitiveIterator.OfInt() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < rowIndexes.length;
      }

      @Override
      public int nextInt() {
        int ret = index;
        index += 2;
        return ret;
      }
    };
    return org.apache.parquet.internal.filter2.columnindex.RowRanges.create(
        rowIndexes[rowIndexes.length - 1], pageIndexes, builder.build());
  }

  @Test
  public void testCreate() {
    // Offset-index pages never overlap; consecutive pages are at most adjacent ([6, 7] then [8, 10]),
    // which the builder coalesces into [6, 10].
    RowRanges ranges = buildRanges(
        1, 2,
        3, 4,
        6, 7,
        8, 10,
        15, 17);
    assertThat(ranges.iterator()).toIterable().containsExactly(1L, 2L, 3L, 4L, 6L, 7L, 8L, 9L, 10L, 15L, 16L, 17L);
    assertThat(ranges.rowCount()).isEqualTo(12);
    assertThat(ranges.isOverlapping(4, 5)).isTrue();
    assertThat(ranges.isOverlapping(5, 5)).isFalse();
    assertThat(ranges.isOverlapping(10, 14)).isTrue();
    assertThat(ranges.isOverlapping(11, 14)).isFalse();
    assertThat(ranges.isOverlapping(18, Long.MAX_VALUE)).isFalse();

    ranges = RowRanges.createSingle(5);
    assertThat(ranges.iterator()).toIterable().containsExactly(0L, 1L, 2L, 3L, 4L);
    assertThat(ranges.rowCount()).isEqualTo(5);
    assertThat(ranges.isOverlapping(0, 100)).isTrue();
    assertThat(ranges.isOverlapping(5, Long.MAX_VALUE)).isFalse();

    ranges = RowRanges.EMPTY;
    assertThat(ranges.iterator()).toIterable().isEmpty();
    assertThat(ranges.rowCount()).isZero();
    assertThat(ranges.isOverlapping(0, Long.MAX_VALUE)).isFalse();
  }

  @Test
  public void testUnion() {
    RowRanges ranges1 = buildRanges(
        2, 5,
        7, 9,
        14, 14,
        20, 24);
    RowRanges ranges2 = buildRanges(
        1, 2,
        4, 5,
        11, 12,
        14, 15,
        21, 22);
    RowRanges empty = buildRanges();
    assertThat(union(ranges1, ranges2).iterator())
        .toIterable()
        .containsExactly(1L, 2L, 3L, 4L, 5L, 7L, 8L, 9L, 11L, 12L, 14L, 15L, 20L, 21L, 22L, 23L, 24L);
    assertThat(union(ranges2, ranges1).iterator())
        .toIterable()
        .containsExactly(1L, 2L, 3L, 4L, 5L, 7L, 8L, 9L, 11L, 12L, 14L, 15L, 20L, 21L, 22L, 23L, 24L);
    assertThat(union(ranges1, ranges1).iterator())
        .toIterable()
        .containsExactly(2L, 3L, 4L, 5L, 7L, 8L, 9L, 14L, 20L, 21L, 22L, 23L, 24L);
    assertThat(union(ranges1, empty).iterator())
        .toIterable()
        .containsExactly(2L, 3L, 4L, 5L, 7L, 8L, 9L, 14L, 20L, 21L, 22L, 23L, 24L);
    assertThat(union(empty, ranges1).iterator())
        .toIterable()
        .containsExactly(2L, 3L, 4L, 5L, 7L, 8L, 9L, 14L, 20L, 21L, 22L, 23L, 24L);
    assertThat(union(ranges2, ranges2).iterator())
        .toIterable()
        .containsExactly(1L, 2L, 4L, 5L, 11L, 12L, 14L, 15L, 21L, 22L);
    assertThat(union(ranges2, empty).iterator())
        .toIterable()
        .containsExactly(1L, 2L, 4L, 5L, 11L, 12L, 14L, 15L, 21L, 22L);
    assertThat(union(empty, ranges2).iterator())
        .toIterable()
        .containsExactly(1L, 2L, 4L, 5L, 11L, 12L, 14L, 15L, 21L, 22L);
    assertThat(union(empty, empty).iterator()).toIterable().isEmpty();
  }

  @Test
  public void testIntersection() {
    RowRanges ranges1 = buildRanges(
        2, 5,
        7, 9,
        14, 14,
        20, 24);
    RowRanges ranges2 = buildRanges(
        1, 2,
        6, 7,
        9, 9,
        11, 12,
        14, 15,
        21, 22);
    RowRanges empty = buildRanges();
    assertThat(intersection(ranges1, ranges2).iterator()).toIterable().containsExactly(2L, 7L, 9L, 14L, 21L, 22L);
    assertThat(intersection(ranges2, ranges1).iterator()).toIterable().containsExactly(2L, 7L, 9L, 14L, 21L, 22L);
    assertThat(intersection(ranges1, ranges1).iterator())
        .toIterable()
        .containsExactly(2L, 3L, 4L, 5L, 7L, 8L, 9L, 14L, 20L, 21L, 22L, 23L, 24L);
    assertThat(intersection(ranges1, empty).iterator()).toIterable().isEmpty();
    assertThat(intersection(empty, ranges1).iterator()).toIterable().isEmpty();
    assertThat(intersection(ranges2, ranges2).iterator())
        .toIterable()
        .containsExactly(1L, 2L, 6L, 7L, 9L, 11L, 12L, 14L, 15L, 21L, 22L);
    assertThat(intersection(ranges2, empty).iterator()).toIterable().isEmpty();
    assertThat(intersection(empty, ranges2).iterator()).toIterable().isEmpty();
    assertThat(intersection(empty, empty).iterator()).toIterable().isEmpty();
  }

  @Test
  public void testBuilderBasic() {
    // Select rows 2, 3, 4, 5 (one contiguous run)
    RowRanges ranges = RowRanges.builder()
        .addSelectedRow(2)
        .addSelectedRow(3)
        .addSelectedRow(4)
        .addSelectedRow(5)
        .build();
    assertThat(ranges.iterator()).toIterable().containsExactly(2L, 3L, 4L, 5L);
    assertThat(ranges.rowCount()).isEqualTo(4);
  }

  @Test
  public void testBuilderMultipleRanges() {
    // Two runs: 1-2 and 5-7
    RowRanges ranges = RowRanges.builder()
        .addSelectedRow(1)
        .addSelectedRow(2)
        .addSelectedRow(5)
        .addSelectedRow(6)
        .addSelectedRow(7)
        .build();
    assertThat(ranges.iterator()).toIterable().containsExactly(1L, 2L, 5L, 6L, 7L);
    assertThat(ranges.rowCount()).isEqualTo(5);
    assertThat(ranges.isOverlapping(1, 2)).isTrue();
    assertThat(ranges.isOverlapping(5, 7)).isTrue();
    assertThat(ranges.isOverlapping(3, 4)).isFalse();
  }

  @Test
  public void testBuilderEmpty() {
    // No rows selected
    RowRanges ranges = RowRanges.builder().build();
    assertThat(ranges).isEqualTo(RowRanges.EMPTY);
    assertThat(ranges.rowCount()).isZero();
    assertThat(ranges.iterator()).toIterable().isEmpty();
  }

  @Test
  public void testBuilderAllSelected() {
    // Five contiguous rows starting at 0
    RowRanges.Builder builder = RowRanges.builder();
    for (long i = 0; i < 5; i++) {
      builder.addSelectedRow(i);
    }
    RowRanges ranges = builder.build();
    assertThat(ranges.iterator()).toIterable().containsExactly(0L, 1L, 2L, 3L, 4L);
    assertThat(ranges.rowCount()).isEqualTo(5);
  }

  @Test
  public void testBuilderSingleRow() {
    RowRanges ranges = RowRanges.builder().addSelectedRow(3).build();
    assertThat(ranges.iterator()).toIterable().containsExactly(3L);
    assertThat(ranges.rowCount()).isEqualTo(1);
    assertThat(ranges.isOverlapping(3, 3)).isTrue();
    assertThat(ranges.isOverlapping(0, 2)).isFalse();
    assertThat(ranges.isOverlapping(4, 10)).isFalse();
  }

  @Test
  public void testBuilderAlternating() {
    // Every other row selected: 0, 2, 4, 6, 8 — five singleton runs.
    RowRanges.Builder builder = RowRanges.builder();
    for (long i = 0; i < 10; i += 2) {
      builder.addSelectedRow(i);
    }
    RowRanges ranges = builder.build();
    assertThat(ranges.iterator()).toIterable().containsExactly(0L, 2L, 4L, 6L, 8L);
    assertThat(ranges.rowCount()).isEqualTo(5);
  }

  @Test
  public void testBuilderFirstAndLast() {
    RowRanges ranges =
        RowRanges.builder().addSelectedRow(0).addSelectedRow(99).build();
    assertThat(ranges.iterator()).toIterable().containsExactly(0L, 99L);
    assertThat(ranges.rowCount()).isEqualTo(2);
  }

  @Test
  public void testBuilderRejectsOutOfOrder() {
    RowRanges.Builder builder = RowRanges.builder().addSelectedRow(5).addSelectedRow(7);
    assertThatThrownBy(() -> builder.addSelectedRow(6))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("addSelectedRange requires strictly increasing ranges; got [6, 6] after 7");
  }

  @Test
  public void testBuilderRejectsDuplicate() {
    RowRanges.Builder builder = RowRanges.builder().addSelectedRow(3);
    assertThatThrownBy(() -> builder.addSelectedRow(3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("addSelectedRange requires strictly increasing ranges; got [3, 3] after 3");
  }

  @Test
  public void testBuilderRejectsNegativeRow() {
    RowRanges.Builder builder = RowRanges.builder();
    assertThatThrownBy(() -> builder.addSelectedRow(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("addSelectedRange requires a non-negative row index; got -1");
  }

  @Test
  public void testBuilderRejectsFollowUpAfterMaxValue() {
    // After Long.MAX_VALUE, runEnd + 1 would overflow; the strictly-increasing guard must still
    // reject any follow-up index rather than silently starting a new run.
    RowRanges.Builder builder = RowRanges.builder().addSelectedRow(Long.MAX_VALUE);
    assertThatThrownBy(() -> builder.addSelectedRow(5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("addSelectedRange requires strictly increasing ranges; got [5, 5] after " + Long.MAX_VALUE);
    // Long.MAX_VALUE alone is a valid single-row selection.
    assertThat(builder.build().iterator()).toIterable().containsExactly(Long.MAX_VALUE);
  }

  @Test
  public void testBuilderAddSelectedRangeCoalescesAndSeparates() {
    RowRanges ranges = RowRanges.builder()
        .addSelectedRange(0, 2)
        .addSelectedRange(3, 5) // adjacent to the previous run -> coalesced into [0, 5]
        .addSelectedRange(8, 9) // gap -> separate run
        .build();
    assertThat(ranges.iterator()).toIterable().containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 8L, 9L);
    assertThat(ranges.rowCount()).isEqualTo(8);
  }

  @Test
  public void testBuilderAddSelectedRangeRejectsOverlap() {
    RowRanges.Builder builder = RowRanges.builder().addSelectedRange(0, 10);
    assertThatThrownBy(() -> builder.addSelectedRange(5, 15))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("addSelectedRange requires strictly increasing ranges; got [5, 15] after 10");
  }

  @Test
  public void testBuilderAddSelectedRangeRejectsFromGreaterThanTo() {
    RowRanges.Builder builder = RowRanges.builder();
    assertThatThrownBy(() -> builder.addSelectedRange(5, 3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("addSelectedRange requires from <= to; got [5, 3]");
  }

  @Test
  public void testBuilderBuildReturnsSnapshot() {
    // build() must return a snapshot: continuing to use the builder afterwards must not
    // mutate a previously built result.
    RowRanges.Builder builder = RowRanges.builder().addSelectedRow(0).addSelectedRow(1);
    RowRanges first = builder.build();
    assertThat(first.iterator()).toIterable().containsExactly(0L, 1L);
    assertThat(first.rowCount()).isEqualTo(2);

    builder.addSelectedRow(5);
    RowRanges second = builder.build();

    // The first result is unchanged.
    assertThat(first.iterator()).toIterable().containsExactly(0L, 1L);
    assertThat(first.rowCount()).isEqualTo(2);
    // The second result reflects the additional row.
    assertThat(second.iterator()).toIterable().containsExactly(0L, 1L, 5L);
    assertThat(second.rowCount()).isEqualTo(3);
  }
}
