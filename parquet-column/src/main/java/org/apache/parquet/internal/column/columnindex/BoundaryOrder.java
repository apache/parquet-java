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
 * Enum for {@link org.apache.parquet.format.BoundaryOrder}. It also contains the implementations of searching for
 * matching page indexes for column index based filtering.
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
          arrayIndex -> comparator.compareValueToMax(arrayIndex) < 0,
          comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt gtEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMax(arrayIndex) <= 0,
          comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt lt(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMin(arrayIndex) > 0,
          comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt ltEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMin(arrayIndex) >= 0,
          comparator::translate);
    }

    @Override
    PrimitiveIterator.OfInt notEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return IndexIterator.filterTranslate(comparator.arrayLength(),
          arrayIndex -> comparator.compareValueToMin(arrayIndex) != 0 || comparator.compareValueToMax(arrayIndex) != 0,
          comparator::translate);
    }
  },
  // TODO[GS]: Implement better performing algorithms
  ASCENDING {
    @Override
    OfInt eq(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.eq(comparator);
    }

    @Override
    OfInt gt(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.gt(comparator);
    }

    @Override
    OfInt gtEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.gtEq(comparator);
    }

    @Override
    OfInt lt(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.lt(comparator);
    }

    @Override
    OfInt ltEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.ltEq(comparator);
    }

    @Override
    OfInt notEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.notEq(comparator);
    }
  },
  // TODO[GS]: Implement better performing algorithms
  DESCENDING {
    @Override
    OfInt eq(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.eq(comparator);
    }

    @Override
    OfInt gt(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.gt(comparator);
    }

    @Override
    OfInt gtEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.gtEq(comparator);
    }

    @Override
    OfInt lt(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.lt(comparator);
    }

    @Override
    OfInt ltEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.ltEq(comparator);
    }

    @Override
    OfInt notEq(ColumnIndexBase<?>.ValueComparator comparator) {
      return UNORDERED.notEq(comparator);
    }
  };
  abstract PrimitiveIterator.OfInt eq(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt gt(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt gtEq(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt lt(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt ltEq(ColumnIndexBase<?>.ValueComparator comparator);

  abstract PrimitiveIterator.OfInt notEq(ColumnIndexBase<?>.ValueComparator comparator);
}
