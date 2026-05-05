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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Float16;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

class BinaryColumnIndexBuilder extends ColumnIndexBuilder {
  private static class BinaryColumnIndex extends ColumnIndexBase<Binary> {
    private Binary[] minValues;
    private Binary[] maxValues;

    private BinaryColumnIndex(PrimitiveType type) {
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
      final Binary v = (Binary) value;
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

  private final List<Binary> minValues = new ArrayList<>();
  private final List<Binary> maxValues = new ArrayList<>();
  private final BinaryTruncator truncator;
  private final int truncateLength;
  private final boolean isFloat16;
  private boolean invalid;

  private static Binary convert(ByteBuffer buffer) {
    return Binary.fromReusedByteBuffer(buffer);
  }

  private static ByteBuffer convert(Binary value) {
    return value.toByteBuffer();
  }

  BinaryColumnIndexBuilder(PrimitiveType type, int truncateLength) {
    truncator = BinaryTruncator.getTruncator(type);
    this.truncateLength = truncateLength;
    this.isFloat16 = type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
  }

  @Override
  void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max) {
    minValues.add(convert(min));
    maxValues.add(convert(max));
  }

  @Override
  void addMinMax(Object min, Object max) {
    Binary bMin = (Binary) min;
    Binary bMax = (Binary) max;

    if (isFloat16 && bMin != null && bMax != null) {
      if (bMin.length() != LogicalTypeAnnotation.Float16LogicalTypeAnnotation.BYTES
          || bMax.length() != LogicalTypeAnnotation.Float16LogicalTypeAnnotation.BYTES) {
        // Should not happen for Float16
        invalid = true;
      } else {
        short sMin = bMin.get2BytesLittleEndian();
        short sMax = bMax.get2BytesLittleEndian();

        if (Float16.isNaN(sMin) || Float16.isNaN(sMax)) {
          invalid = true;
        }

        // Sorting order is undefined for -0.0 so let min = -0.0 and max = +0.0 to
        // ensure that no 0.0 values are skipped
        // +0.0 is 0x0000, -0.0 is 0x8000 (little endian: 00 00, 00 80)
        if (sMin == (short) 0x0000) {
          bMin = Float16.NEGATIVE_ZERO_LITTLE_ENDIAN;
        }
        if (sMax == (short) 0x8000) {
          bMax = Float16.POSITIVE_ZERO_LITTLE_ENDIAN;
        }
      }
    }

    minValues.add(bMin == null ? null : truncator.truncateMin(bMin, truncateLength));
    maxValues.add(bMax == null ? null : truncator.truncateMax(bMax, truncateLength));
  }

  @Override
  ColumnIndexBase<Binary> createColumnIndex(PrimitiveType type) {
    if (invalid) {
      return null;
    }
    BinaryColumnIndex columnIndex = new BinaryColumnIndex(type);
    columnIndex.minValues = minValues.toArray(new Binary[0]);
    columnIndex.maxValues = maxValues.toArray(new Binary[0]);
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
    return ((Binary) value).length();
  }

  @Override
  public long getMinMaxSize() {
    long minSizesSum = minValues.stream().mapToLong(Binary::length).sum();
    long maxSizesSum = maxValues.stream().mapToLong(Binary::length).sum();
    return minSizesSum + maxSizesSum;
  }
}
