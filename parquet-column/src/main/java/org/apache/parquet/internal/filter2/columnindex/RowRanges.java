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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

import org.apache.parquet.filter2.compat.FilterCompat.Filter;

/**
 * Class representing row ranges in a row-group. These row ranges are calculated as a result of the column index based
 * filtering.
 *
 * @see ColumnIndexFilter#calculateRowRanges(Filter, ColumnIndexStore, Collection, long)
 */
public class RowRanges {
  private static class Range implements Comparable<Range> {
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

  static RowRanges single(long rowCount) {
    return new RowRanges(new Range(0, rowCount - 1));
  }

  private final List<Range> ranges = new ArrayList<>();

  private RowRanges(Range range) {
    ranges.add(range);
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

  // TODO[GS]: implement set operations (union, intersection, complement)
}
