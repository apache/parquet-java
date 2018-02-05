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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

class IntColumnIndexBuilder extends ColumnIndexBuilder {
  private static class IntColumnIndex extends ColumnIndexBase {
    private int[] minValues;
    private int[] maxValues;

    private IntColumnIndex(PrimitiveType type) {
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

  private final IntList minValues = new IntArrayList();
  private final IntList maxValues = new IntArrayList();

  private static int convert(ByteBuffer buffer) {
    return buffer.getInt(0);
  }

  private static ByteBuffer convert(int value) {
    return ByteBuffer.allocate(Integer.SIZE / 8).putInt(0, value);
  }

  @Override
  void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max) {
    minValues.add(min == null ? 0 : convert(min));
    maxValues.add(max == null ? 0 : convert(max));
  }

  @Override
  void addMinMax(Object min, Object max) {
    minValues.add(min == null ? 0 : (int) min);
    maxValues.add(max == null ? 0 : (int) max);
  }

  @Override
  ColumnIndexBase createColumnIndex(PrimitiveType type) {
    IntColumnIndex columnIndex = new IntColumnIndex(type);
    columnIndex.minValues = minValues.toIntArray();
    columnIndex.maxValues = maxValues.toIntArray();
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
