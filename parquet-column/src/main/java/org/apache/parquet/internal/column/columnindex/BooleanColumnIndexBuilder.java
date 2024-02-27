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

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import java.nio.ByteBuffer;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

class BooleanColumnIndexBuilder extends ColumnIndexBuilder {
  private static class BooleanColumnIndex extends ColumnIndexBase<Boolean> {
    private boolean[] minValues;
    private boolean[] maxValues;

    private BooleanColumnIndex(PrimitiveType type) {
      super(type);
    }

    @Override
    ByteBuffer getMinValueAsBytes(int pageIndex) {
      return convert(minValues[pageIndex]);
    }

    @Override
    ByteBuffer getMaxValueAsBytes(int pageIndex) {
      return convert(maxValues[pageIndex]);
    }

    @Override
    String getMinValueAsString(int pageIndex) {
      return stringifier.stringify(minValues[pageIndex]);
    }

    @Override
    String getMaxValueAsString(int pageIndex) {
      return stringifier.stringify(maxValues[pageIndex]);
    }

    @Override
    @SuppressWarnings("unchecked")
    <T extends Comparable<T>> Statistics<T> createStats(int arrayIndex) {
      return (Statistics<T>) new Statistics<>(minValues[arrayIndex], maxValues[arrayIndex], comparator);
    }

    @Override
    ValueComparator createValueComparator(Object value) {
      final boolean v = (boolean) value;
      return new ValueComparator() {
        @Override
        int compareValueToMin(int arrayIndex) {
          return comparator.compare(v, minValues[arrayIndex]);
        }

        @Override
        int compareValueToMax(int arrayIndex) {
          return comparator.compare(v, maxValues[arrayIndex]);
        }
      };
    }
  }

  private final BooleanList minValues = new BooleanArrayList();
  private final BooleanList maxValues = new BooleanArrayList();

  private static boolean convert(ByteBuffer buffer) {
    return buffer.get(0) != 0;
  }

  private static ByteBuffer convert(boolean value) {
    return ByteBuffer.allocate(1).put(0, value ? (byte) 1 : 0);
  }

  @Override
  void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max) {
    minValues.add(convert(min));
    maxValues.add(convert(max));
  }

  @Override
  void addMinMax(Object min, Object max) {
    minValues.add((boolean) min);
    maxValues.add((boolean) max);
  }

  @Override
  ColumnIndexBase<Boolean> createColumnIndex(PrimitiveType type) {
    BooleanColumnIndex columnIndex = new BooleanColumnIndex(type);
    columnIndex.minValues = minValues.toBooleanArray();
    columnIndex.maxValues = maxValues.toBooleanArray();
    return columnIndex;
  }

  @Override
  void clearMinMax() {
    minValues.clear();
    maxValues.clear();
  }

  @Override
  int compareMinValues(PrimitiveComparator<Binary> comparator, int index1, int index2) {
    return comparator.compare(minValues.get(index1), minValues.get(index2));
  }

  @Override
  int compareMaxValues(PrimitiveComparator<Binary> comparator, int index1, int index2) {
    return comparator.compare(maxValues.get(index1), maxValues.get(index2));
  }

  @Override
  int sizeOf(Object value) {
    return 1;
  }

  @Override
  public long getMinMaxSize() {
    return minValues.size() + maxValues.size();
  }
}
