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

import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfInt;

import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder.ColumnIndexBase;

/**
 * Enum for {@link org.apache.parquet.format.BoundaryOrder}. It also contains
 * the implementations of searching for matching page indexes for column index
 * based filtering.
 */
public enum BoundaryOrder {
  UNORDERED {
    @Override
    PrimitiveIterator.OfInt eq(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMin(arrayIndex) >= 0 && comparator.compareValueToMax(arrayIndex) <= 0,
          comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt gt(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMax(arrayIndex) < 0, comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt gtEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMax(arrayIndex) <= 0, comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt lt(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMin(arrayIndex) > 0, comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt ltEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMin(arrayIndex) >= 0, comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt notEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMin(arrayIndex) != 0 || comparator.compareValueToMax(arrayIndex) != 0,
          comparator::translate);
    }
  },
  ASCENDING {
    @Override
    OfInt eq(ColumnIndexBase<?>.ValueComparator comparator) {
      Bounds bounds = findBounds(comparator);
      if (bounds == null) {
        return IndexIterator.EMPTY;
      }
      return IndexIterator.rangeTranslate(bounds.lower, bounds.upper, comparator::translate);
    }

    @Override
    OfInt gt(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      if (length == 0) {
        // No matching rows if the column index contains null pages only
        return IndexIterator.EMPTY;
      }
      int left = 0;
      int right = length;
      do {
        int i = floorMid(left, right);
        if (comparator.compareValueToMax(i) >= 0) {
          left = i + 1;
        } else {
          right = i;
        }
      } while (left < right);
      return IndexIterator.rangeTranslate(right, length - 1, comparator::translate);
    }

    @Override
    OfInt gtEq(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      if (length == 0) {
        // No matching rows if the column index contains null pages only
        return IndexIterator.EMPTY;
      }
      int left = 0;
      int right = length;
      do {
        int i = floorMid(left, right);
        if (comparator.compareValueToMax(i) > 0) {
          left = i + 1;
        } else {
          right = i;
        }
      } while (left < right);
      return IndexIterator.rangeTranslate(right, length - 1, comparator::translate);
    }

    @Override
    OfInt lt(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      if (length == 0) {
        // No matching rows if the column index contains null pages only
        return IndexIterator.EMPTY;
      }
      int left = -1;
      int right = length - 1;
      do {
        int i = ceilingMid(left, right);
        if (comparator.compareValueToMin(i) <= 0) {
          right = i - 1;
        } else {
          left = i;
        }
      } while (left < right);
      return IndexIterator.rangeTranslate(0, left, comparator::translate);
    }

    @Override
    OfInt ltEq(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      if (length == 0) {
        // No matching rows if the column index contains null pages only
        return IndexIterator.EMPTY;
      }
      int left = -1;
      int right = length - 1;
      do {
        int i = ceilingMid(left, right);
        if (comparator.compareValueToMin(i) < 0) {
          right = i - 1;
        } else {
          left = i;
        }
      } while (left < right);
      return IndexIterator.rangeTranslate(0, left, comparator::translate);
    }

    @Override
    OfInt notEq(ColumnIndexBase<?>.ValueComparator comparator) {
      Bounds bounds = findBounds(comparator);
      int length = comparator.arrayLength();
      if (bounds == null) {
        return IndexIterator.all(comparator);
      }
      return IndexIterator.filterTranslate(length, i -> i < bounds.lower || i > bounds.upper
          || comparator.compareValueToMin(i) != 0 || comparator.compareValueToMax(i) != 0, comparator::translate);
    }

    private Bounds findBounds(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      int lowerLeft = 0;
      int upperLeft = 0;
      int lowerRight = length - 1;
      int upperRight = length - 1;
      do {
        if (lowerLeft > lowerRight) {
          return null;
        }
        int i = floorMid(lowerLeft, lowerRight);
        if (comparator.compareValueToMin(i) < 0) {
          lowerRight = upperRight = i - 1;
        } else if (comparator.compareValueToMax(i) > 0) {
          lowerLeft = upperLeft = i + 1;
        } else {
          lowerRight = upperLeft = i;
        }
      } while (lowerLeft != lowerRight);
      do {
        if (upperLeft > upperRight) {
          return null;
        }
        int i = ceilingMid(upperLeft, upperRight);
        if (comparator.compareValueToMin(i) < 0) {
          upperRight = i - 1;
        } else if (comparator.compareValueToMax(i) > 0) {
          upperLeft = i + 1;
        } else {
          upperLeft = i;
        }
      } while (upperLeft != upperRight);
      return new Bounds(lowerLeft, upperRight);
    }
  },
  DESCENDING {
    @Override
    OfInt eq(ColumnIndexBase<?>.ValueComparator comparator) {
      Bounds bounds = findBounds(comparator);
      if (bounds == null) {
        return IndexIterator.EMPTY;
      }
      return IndexIterator.rangeTranslate(bounds.lower, bounds.upper, comparator::translate);
    }

    @Override
    OfInt gt(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      if (length == 0) {
        // No matching rows if the column index contains null pages only
        return IndexIterator.EMPTY;
      }
      int left = -1;
      int right = length - 1;
      do {
        int i = ceilingMid(left, right);
        if (comparator.compareValueToMax(i) >= 0) {
          right = i - 1;
        } else {
          left = i;
        }
      } while (left < right);
      return IndexIterator.rangeTranslate(0, left, comparator::translate);
    }

    @Override
    OfInt gtEq(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      if (length == 0) {
        // No matching rows if the column index contains null pages only
        return IndexIterator.EMPTY;
      }
      int left = -1;
      int right = length - 1;
      do {
        int i = ceilingMid(left, right);
        if (comparator.compareValueToMax(i) > 0) {
          right = i - 1;
        } else {
          left = i;
        }
      } while (left < right);
      return IndexIterator.rangeTranslate(0, left, comparator::translate);
    }

    @Override
    OfInt lt(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      if (length == 0) {
        // No matching rows if the column index contains null pages only
        return IndexIterator.EMPTY;
      }
      int left = 0;
      int right = length;
      do {
        int i = floorMid(left, right);
        if (comparator.compareValueToMin(i) <= 0) {
          left = i + 1;
        } else {
          right = i;
        }
      } while (left < right);
      return IndexIterator.rangeTranslate(right, length - 1, comparator::translate);
    }

    @Override
    OfInt ltEq(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      if (length == 0) {
        // No matching rows if the column index contains null pages only
        return IndexIterator.EMPTY;
      }
      int left = 0;
      int right = length;
      do {
        int i = floorMid(left, right);
        if (comparator.compareValueToMin(i) < 0) {
          left = i + 1;
        } else {
          right = i;
        }
      } while (left < right);
      return IndexIterator.rangeTranslate(right, length - 1, comparator::translate);
    }

    @Override
    OfInt notEq(ColumnIndexBase<?>.ValueComparator comparator) {
      Bounds bounds = findBounds(comparator);
      int length = comparator.arrayLength();
      if (bounds == null) {
        return IndexIterator.all(comparator);
      }
      return IndexIterator.filterTranslate(length, i -> i < bounds.lower || i > bounds.upper
          || comparator.compareValueToMin(i) != 0 || comparator.compareValueToMax(i) != 0, comparator::translate);
    }

    private Bounds findBounds(ColumnIndexBase<?>.ValueComparator comparator) {
      int length = comparator.arrayLength();
      int lowerLeft = 0;
      int upperLeft = 0;
      int lowerRight = length - 1;
      int upperRight = length - 1;
      do {
        if (lowerLeft > lowerRight) {
          return null;
        }
        int i = floorMid(lowerLeft, lowerRight);
        if (comparator.compareValueToMax(i) > 0) {
          lowerRight = upperRight = i - 1;
        } else if (comparator.compareValueToMin(i) < 0) {
          lowerLeft = upperLeft = i + 1;
        } else {
          lowerRight = upperLeft = i;
        }
      } while (lowerLeft != lowerRight);
      do {
        if (upperLeft > upperRight) {
          return null;
        }
        int i = ceilingMid(upperLeft, upperRight);
        if (comparator.compareValueToMax(i) > 0) {
          upperRight = i - 1;
        } else if (comparator.compareValueToMin(i) < 0) {
          upperLeft = i + 1;
        } else {
          upperLeft = i;
        }
      } while (upperLeft != upperRight);
      return new Bounds(lowerLeft, upperRight);
    }
  };

  private static class Bounds {
    final int lower, upper;

    Bounds(int lower, int upper) {
      assert lower <= upper;
      this.lower = lower;
      this.upper = upper;
    }
  }

  private static int floorMid(int left, int right) {
    // Avoid the possible overflow might happen in case of (left + right) / 2
    return left + ((right - left) / 2);
  }

  private static int ceilingMid(int left, int right) {
    // Avoid the possible overflow might happen in case of (left + right + 1) / 2
    return left + ((right - left + 1) / 2);
  }

  abstract PrimitiveIterator.OfInt eq(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt gt(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt gtEq(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt lt(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt ltEq(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt notEq(ColumnIndexBase<?>.ValueComparator comparator);
}
