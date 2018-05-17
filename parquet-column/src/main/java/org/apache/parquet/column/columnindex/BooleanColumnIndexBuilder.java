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
package org.apache.parquet.column.columnindex;

import java.nio.ByteBuffer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;

class BooleanColumnIndexBuilder extends ColumnIndexBuilder {
  private static class BooleanColumnIndex extends ColumnIndexBase {
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
    minValues.add(min == null ? false : convert(min));
    maxValues.add(max == null ? false : convert(max));
  }

  @Override
  void addMinMax(Object min, Object max) {
    minValues.add(min == null ? false : (boolean) min);
    maxValues.add(max == null ? false : (boolean) max);
  }

  @Override
  ColumnIndexBase createColumnIndex(PrimitiveType type) {
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
  int compareMaxMin(PrimitiveComparator<Binary> comparator, int maxIndex, int minIndex) {
    return comparator.compare(maxValues.get(maxIndex), minValues.get(minIndex));
  }
}
