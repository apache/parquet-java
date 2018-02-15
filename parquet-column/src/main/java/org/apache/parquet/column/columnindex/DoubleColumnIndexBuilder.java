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

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.nio.ByteBuffer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

class DoubleColumnIndexBuilder extends ColumnIndexBuilder {
  private static class DoubleColumnIndex extends ColumnIndexBase {
    private double[] minValues;
    private double[] maxValues;

    private DoubleColumnIndex(PrimitiveType type) {
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

  private final DoubleList minValues = new DoubleArrayList();
  private final DoubleList maxValues = new DoubleArrayList();

  private static double convert(ByteBuffer buffer) {
    return buffer.order(LITTLE_ENDIAN).getDouble(0);
  }

  private static ByteBuffer convert(double value) {
    return ByteBuffer.allocate(Double.SIZE / 8).order(LITTLE_ENDIAN).putDouble(0, value);
  }

  @Override
  void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max) {
    minValues.add(min == null ? 0 : convert(min));
    maxValues.add(max == null ? 0 : convert(max));
  }

  @Override
  void addMinMax(Object min, Object max) {
    minValues.add(min == null ? 0 : (double) min);
    maxValues.add(max == null ? 0 : (double) max);
  }

  @Override
  ColumnIndexBase createColumnIndex(PrimitiveType type) {
    DoubleColumnIndex columnIndex = new DoubleColumnIndex(type);
    columnIndex.minValues = minValues.toDoubleArray();
    columnIndex.maxValues = maxValues.toDoubleArray();
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
}
