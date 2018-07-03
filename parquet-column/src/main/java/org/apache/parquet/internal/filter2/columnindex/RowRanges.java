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
package org.apache.parquet.internal.filter2.columnindex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

/**
 * Class representing row ranges in a row-group. These row ranges are calculated as a result of the column index based
 * filtering.
 *
 * @see ColumnIndexFilter#calculateRowRanges(Filter, ColumnIndexStore, Collection, long)
 */
public class RowRanges {
  private static class Range implements Comparable<Range> {
    private static Range union(Range left, Range right) {
      if (left.from <= right.from) {
        if (left.to + 1 >= right.from) {
          return new Range(left.from, Math.max(left.to, right.to));
        }
      } else if (right.to + 1 >= left.from) {
        return new Range(right.from, Math.max(left.to, right.to));
      }
      return null;
    }

    private static Range intersection(Range left, Range right) {
      if (left.from <= right.from) {
        if (left.to >= right.from) {
          return new Range(right.from, Math.min(left.to, right.to));
        }
      } else if (right.to >= left.from) {
        return new Range(left.from, Math.min(left.to, right.to));
      }
      return null;
    }

    final long from;
    final long to;

    Range(long from, long to) {
      assert from <= to;
      this.from = from;
      this.to = to;
    }

    long count() {
      return to - from + 1;
    }

    @Override
    public int compareTo(Range other) {
      if (to < other.from) {
        return -1;
      } else if (from > other.to) {
        return 1;
      } else {
        // Equality means the two ranges are connected
        return 0;
      }
    }

    @Override
    public String toString() {
      return "[" + from + ", " + to + ']';
    }
  }

  static final RowRanges EMPTY = new RowRanges();
  
  static RowRanges single(long rowCount) {
    RowRanges ranges = new RowRanges();
    ranges.add(new Range(0, rowCount - 1));
    return ranges;
  }

  static RowRanges build(long rowCount, PrimitiveIterator.OfInt pageIndexes, OffsetIndex offsetIndex) {
    RowRanges ranges = new RowRanges();
    int pageCount = offsetIndex.getPageCount();
    while (pageIndexes.hasNext()) {
      int pageIndex = pageIndexes.nextInt();
      long nextRowIndex = pageIndex + 1 < pageCount ? offsetIndex.getFirstRowIndex(pageIndex + 1) : rowCount;
      ranges.add(new Range(offsetIndex.getFirstRowIndex(pageIndex), nextRowIndex - 1));
    }
    return ranges;
  }

  static RowRanges union(RowRanges left, RowRanges right) {
    RowRanges result = new RowRanges();
    Iterator<Range> it1 = left.ranges.iterator();
    Iterator<Range> it2 = right.ranges.iterator();
    if (it2.hasNext()) {
      Range range2 = it2.next();
      while (it1.hasNext()) {
        Range range1 = it1.next();
        if (range1.compareTo(range2) <= 0) {
          result.add(range1);
        } else {
          result.add(range2);
          range2 = range1;
          Iterator<Range> tmp = it1;
          it1 = it2;
          it2 = tmp;
        }
      }
      result.add(range2);
    } else {
      it2 = it1;
    }
    while (it2.hasNext()) {
      result.add(it2.next());
    }

    return result;
  }

  static RowRanges intersection(RowRanges left, RowRanges right) {
    RowRanges result = new RowRanges();

    int rightIndex = 0;
    for (Range l : left.ranges) {
      for (int i = rightIndex, n = right.ranges.size(); i < n; ++i) {
        Range r = right.ranges.get(i);
        int cmp = l.compareTo(r);
        if (cmp < 0) {
          rightIndex = i;
          break;
        } else if (cmp > 0) {
          rightIndex = i + 1;
          continue;
        }
        result.add(Range.intersection(l, r));
      }
    }

    return result;
  }

  private final List<Range> ranges = new ArrayList<>();

  private RowRanges() {
  }

  private void add(Range range) {
    if (!ranges.isEmpty()) {
      int lastIndex = ranges.size() - 1;
      Range last = ranges.get(lastIndex);
      assert last.compareTo(range) <= 0;
      Range u = Range.union(last, range);
      if (u != null) {
        ranges.set(lastIndex, u);
      } else {
        ranges.add(range);
      }
    } else {
      ranges.add(range);
    }
  }

  /**
   * @return the number of rows in the ranges
   */
  public long rowCount() {
    long cnt = 0;
    for (Range range : ranges) {
      cnt += range.count();
    }
    return cnt;
  }

  /**
   * @return the ascending iterator of the row indexes contained in the ranges
   */
  public PrimitiveIterator.OfLong allRows() {
    return new PrimitiveIterator.OfLong() {
      private int actualRangeIndex = -1;
      private Range actualRange;
      private long next = findNext();

      private long findNext() {
        if (actualRange == null || next + 1 > actualRange.to) {
          if (actualRangeIndex + 1 < ranges.size()) {
            actualRange = ranges.get(++actualRangeIndex);
            next = actualRange.from;
          } else {
            return -1;
          }
        } else {
          ++next;
        }
        return next;
      }

      @Override
      public boolean hasNext() {
        return next >= 0;
      }

      @Override
      public long nextLong() {
        long ret = next;
        if (ret < 0) {
          throw new NoSuchElementException();
        }
        next = findNext();
        return ret;
      }
    };
  }

  /**
   * @param from
   *          the first row of the range to be checked for connection
   * @param to
   *          the last row of the range to be checked for connection
   * @return {@code true} if the specified range is connected (have common elements) to one of the ranges
   */
  public boolean isConnected(long from, long to) {
    return Collections.binarySearch(ranges, new Range(from, to)) >= 0;
  }

  @Override
  public String toString() {
    return ranges.toString();
  }
}
