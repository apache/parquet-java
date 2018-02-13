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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

class LongColumnIndexBuilder extends ColumnIndexBuilder {
  private static class LongColumnIndex extends ColumnIndexBase {
    private long[] minValues;
    private long[] maxValues;

    private LongColumnIndex(PrimitiveType type) {
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

  private final LongList minValues = new LongArrayList();
  private final LongList maxValues = new LongArrayList();

  private static long convert(ByteBuffer buffer) {
    return buffer.order(LITTLE_ENDIAN).getLong(0);
  }

  private static ByteBuffer convert(long value) {
    return ByteBuffer.allocate(Long.SIZE / 8).order(LITTLE_ENDIAN).putLong(0, value);
  }

  @Override
  void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max) {
    minValues.add(min == null ? 0 : convert(min));
    maxValues.add(max == null ? 0 : convert(max));
  }

  @Override
  void addMinMax(Object min, Object max) {
    minValues.add(min == null ? 0 : (long) min);
    maxValues.add(max == null ? 0 : (long) max);
  }

  @Override
  ColumnIndexBase createColumnIndex(PrimitiveType type) {
    LongColumnIndex columnIndex = new LongColumnIndex(type);
    columnIndex.minValues = minValues.toLongArray();
    columnIndex.maxValues = maxValues.toLongArray();
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
