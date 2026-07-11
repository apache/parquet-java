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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.Arrays;
import java.util.PrimitiveIterator;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.junit.Test;

/**
 * Unit test for {@link RowRanges}
 */
public class TestRowRanges {
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
    return RowRanges.create(rowIndexes[rowIndexes.length - 1], pageIndexes, builder.build());
  }

  private static void assertAllRowsEqual(PrimitiveIterator.OfLong actualIt, long... expectedValues) {
    LongList actualList = new LongArrayList();
    actualIt.forEachRemaining((long value) -> actualList.add(value));
    assertArrayEquals(
        Arrays.toString(expectedValues) + "!= " + actualList, expectedValues, actualList.toLongArray());
  }

  @Test
  public void testCreate() {
    RowRanges ranges = buildRanges(
        1, 2,
        3, 4,
        6, 7,
        7, 10,
        15, 17);
    assertAllRowsEqual(ranges.iterator(), 1, 2, 3, 4, 6, 7, 8, 9, 10, 15, 16, 17);
    assertEquals(12, ranges.rowCount());
    assertTrue(ranges.isOverlapping(4, 5));
    assertFalse(ranges.isOverlapping(5, 5));
    assertTrue(ranges.isOverlapping(10, 14));
    assertFalse(ranges.isOverlapping(11, 14));
    assertFalse(ranges.isOverlapping(18, Long.MAX_VALUE));

    ranges = RowRanges.createSingle(5);
    assertAllRowsEqual(ranges.iterator(), 0, 1, 2, 3, 4);
    assertEquals(5, ranges.rowCount());
    assertTrue(ranges.isOverlapping(0, 100));
    assertFalse(ranges.isOverlapping(5, Long.MAX_VALUE));

    ranges = RowRanges.EMPTY;
    assertAllRowsEqual(ranges.iterator());
    assertEquals(0, ranges.rowCount());
    assertFalse(ranges.isOverlapping(0, Long.MAX_VALUE));
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
    assertAllRowsEqual(
        union(ranges1, ranges2).iterator(), 1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 14, 15, 20, 21, 22, 23, 24);
    assertAllRowsEqual(
        union(ranges2, ranges1).iterator(), 1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 14, 15, 20, 21, 22, 23, 24);
    assertAllRowsEqual(union(ranges1, ranges1).iterator(), 2, 3, 4, 5, 7, 8, 9, 14, 20, 21, 22, 23, 24);
    assertAllRowsEqual(union(ranges1, empty).iterator(), 2, 3, 4, 5, 7, 8, 9, 14, 20, 21, 22, 23, 24);
    assertAllRowsEqual(union(empty, ranges1).iterator(), 2, 3, 4, 5, 7, 8, 9, 14, 20, 21, 22, 23, 24);
    assertAllRowsEqual(union(ranges2, ranges2).iterator(), 1, 2, 4, 5, 11, 12, 14, 15, 21, 22);
    assertAllRowsEqual(union(ranges2, empty).iterator(), 1, 2, 4, 5, 11, 12, 14, 15, 21, 22);
    assertAllRowsEqual(union(empty, ranges2).iterator(), 1, 2, 4, 5, 11, 12, 14, 15, 21, 22);
    assertAllRowsEqual(union(empty, empty).iterator());
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
    assertAllRowsEqual(intersection(ranges1, ranges2).iterator(), 2, 7, 9, 14, 21, 22);
    assertAllRowsEqual(intersection(ranges2, ranges1).iterator(), 2, 7, 9, 14, 21, 22);
    assertAllRowsEqual(intersection(ranges1, ranges1).iterator(), 2, 3, 4, 5, 7, 8, 9, 14, 20, 21, 22, 23, 24);
    assertAllRowsEqual(intersection(ranges1, empty).iterator());
    assertAllRowsEqual(intersection(empty, ranges1).iterator());
    assertAllRowsEqual(intersection(ranges2, ranges2).iterator(), 1, 2, 6, 7, 9, 11, 12, 14, 15, 21, 22);
    assertAllRowsEqual(intersection(ranges2, empty).iterator());
    assertAllRowsEqual(intersection(empty, ranges2).iterator());
    assertAllRowsEqual(intersection(empty, empty).iterator());
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
    assertAllRowsEqual(ranges.iterator(), 2, 3, 4, 5);
    assertEquals(4, ranges.rowCount());
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
    assertAllRowsEqual(ranges.iterator(), 1, 2, 5, 6, 7);
    assertEquals(5, ranges.rowCount());
    assertTrue(ranges.isOverlapping(1, 2));
    assertTrue(ranges.isOverlapping(5, 7));
    assertFalse(ranges.isOverlapping(3, 4));
  }

  @Test
  public void testBuilderEmpty() {
    // No rows selected
    RowRanges ranges = RowRanges.builder().build();
    assertEquals(RowRanges.EMPTY, ranges);
    assertEquals(0, ranges.rowCount());
    assertAllRowsEqual(ranges.iterator());
  }

  @Test
  public void testBuilderAllSelected() {
    // Five contiguous rows starting at 0
    RowRanges.Builder builder = RowRanges.builder();
    for (long i = 0; i < 5; i++) {
      builder.addSelectedRow(i);
    }
    RowRanges ranges = builder.build();
    assertAllRowsEqual(ranges.iterator(), 0, 1, 2, 3, 4);
    assertEquals(5, ranges.rowCount());
  }

  @Test
  public void testBuilderSingleRow() {
    RowRanges ranges = RowRanges.builder().addSelectedRow(3).build();
    assertAllRowsEqual(ranges.iterator(), 3);
    assertEquals(1, ranges.rowCount());
    assertTrue(ranges.isOverlapping(3, 3));
    assertFalse(ranges.isOverlapping(0, 2));
    assertFalse(ranges.isOverlapping(4, 10));
  }

  @Test
  public void testBuilderAlternating() {
    // Every other row selected: 0, 2, 4, 6, 8 — five singleton runs.
    RowRanges.Builder builder = RowRanges.builder();
    for (long i = 0; i < 10; i += 2) {
      builder.addSelectedRow(i);
    }
    RowRanges ranges = builder.build();
    assertAllRowsEqual(ranges.iterator(), 0, 2, 4, 6, 8);
    assertEquals(5, ranges.rowCount());
  }

  @Test
  public void testBuilderFirstAndLast() {
    RowRanges ranges =
        RowRanges.builder().addSelectedRow(0).addSelectedRow(99).build();
    assertAllRowsEqual(ranges.iterator(), 0, 99);
    assertEquals(2, ranges.rowCount());
  }

  @Test
  public void testBuilderRejectsOutOfOrder() {
    RowRanges.Builder builder = RowRanges.builder().addSelectedRow(5).addSelectedRow(7);
    try {
      builder.addSelectedRow(6);
      org.junit.Assert.fail("expected IllegalArgumentException for out-of-order index");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testBuilderRejectsDuplicate() {
    RowRanges.Builder builder = RowRanges.builder().addSelectedRow(3);
    try {
      builder.addSelectedRow(3);
      org.junit.Assert.fail("expected IllegalArgumentException for duplicate index");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testBuilderRejectsNegativeRow() {
    RowRanges.Builder builder = RowRanges.builder();
    try {
      builder.addSelectedRow(-1);
      org.junit.Assert.fail("expected IllegalArgumentException for negative index");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testBuilderRejectsFollowUpAfterMaxValue() {
    // After Long.MAX_VALUE, runEnd + 1 would overflow; the strictly-increasing guard must still
    // reject any follow-up index rather than silently starting a new run.
    RowRanges.Builder builder = RowRanges.builder().addSelectedRow(Long.MAX_VALUE);
    try {
      builder.addSelectedRow(5);
      org.junit.Assert.fail("expected IllegalArgumentException for index after Long.MAX_VALUE");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    // Long.MAX_VALUE alone is a valid single-row selection.
    assertAllRowsEqual(builder.build().iterator(), Long.MAX_VALUE);
  }

  @Test
  public void testBuilderBuildReturnsSnapshot() {
    // build() must return a snapshot: continuing to use the builder afterwards must not
    // mutate a previously built result.
    RowRanges.Builder builder = RowRanges.builder().addSelectedRow(0).addSelectedRow(1);
    RowRanges first = builder.build();
    assertAllRowsEqual(first.iterator(), 0, 1);
    assertEquals(2, first.rowCount());

    builder.addSelectedRow(5);
    RowRanges second = builder.build();

    // The first result is unchanged.
    assertAllRowsEqual(first.iterator(), 0, 1);
    assertEquals(2, first.rowCount());
    // The second result reflects the additional row.
    assertAllRowsEqual(second.iterator(), 0, 1, 5);
    assertEquals(3, second.rowCount());
  }
}
