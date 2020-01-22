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

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.nio.ByteBuffer;

import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;

class FloatColumnIndexBuilder extends ColumnIndexBuilder {
  private static class FloatColumnIndex extends ColumnIndexBase<Float> {
    private float[] minValues;
    private float[] maxValues;

    private FloatColumnIndex(PrimitiveType type) {
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
      final float v = (float) value;
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

  private final FloatList minValues = new FloatArrayList();
  private final FloatList maxValues = new FloatArrayList();
  private boolean invalid;

  private static float convert(ByteBuffer buffer) {
    return buffer.order(LITTLE_ENDIAN).getFloat(0);
  }

  private static ByteBuffer convert(float value) {
    return ByteBuffer.allocate(Float.BYTES).order(LITTLE_ENDIAN).putFloat(0, value);
  }

  @Override
  void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max) {
    minValues.add(convert(min));
    maxValues.add(convert(max));
  }

  @Override
  void addMinMax(Object min, Object max) {
    float fMin = (float) min;
    float fMax = (float) max;
    if (Float.isNaN(fMin) || Float.isNaN(fMax)) {
      // Invalidate this column index in case of NaN as the sorting order of values is
      // undefined for this case
      invalid = true;
    }

    // Sorting order is undefined for -0.0 so let min = -0.0 and max = +0.0 to
    // ensure that no 0.0 values are skipped
    if (Float.compare(fMin, +0.0f) == 0) {
      fMin = -0.0f;
    }
    if (Float.compare(fMax, -0.0f) == 0) {
      fMax = +0.0f;
    }

    minValues.add(fMin);
    maxValues.add(fMax);
  }

  @Override
  ColumnIndexBase<Float> createColumnIndex(PrimitiveType type) {
    if (invalid) {
      return null;
    }
    FloatColumnIndex columnIndex = new FloatColumnIndex(type);
    columnIndex.minValues = minValues.toFloatArray();
    columnIndex.maxValues = maxValues.toFloatArray();
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
    return Float.BYTES;
  }
}
