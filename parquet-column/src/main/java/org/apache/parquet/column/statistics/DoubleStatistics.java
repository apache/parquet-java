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
package org.apache.parquet.column.statistics;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class DoubleStatistics extends Statistics<Double> {

  // A fake type object to be used to generate the proper comparator
  private static final PrimitiveType DEFAULT_FAKE_TYPE =
      Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("fake_double_type");

  private double max;
  private double min;

  /**
   * @deprecated will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead
   */
  @Deprecated
  public DoubleStatistics() {
    this(DEFAULT_FAKE_TYPE);
  }

  DoubleStatistics(PrimitiveType type) {
    super(type);
    incrementNanCount(0);
  }

  private DoubleStatistics(DoubleStatistics other) {
    super(other.type());
    if (other.hasNonNullValue()) {
      initializeStats(other.min, other.max);
    }
    setNumNulls(other.getNumNulls());
    incrementNanCount(other.getNanCount());
  }

  @Override
  public void updateStats(double value) {
    if (Double.isNaN(value)) {
      incrementNanCount();
    }
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    DoubleStatistics doubleStats = (DoubleStatistics) stats;
    if (!this.hasNonNullValue()) {
      initializeStats(doubleStats.getMin(), doubleStats.getMax());
    } else {
      updateStats(doubleStats.getMin(), doubleStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Double.longBitsToDouble(BytesUtils.bytesToLong(maxBytes));
    min = Double.longBitsToDouble(BytesUtils.bytesToLong(minBytes));
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.longToBytes(Double.doubleToLongBits(max));
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.longToBytes(Double.doubleToLongBits(min));
  }

  @Override
  String stringify(Double value) {
    return stringifier.stringify(value);
  }

  @Override
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || (16 < size);
  }

  public void updateStats(double min_value, double max_value) {
    if (type().columnOrder().equals(ColumnOrder.ieee754TotalOrder())) {
      if (!Double.isNaN(min_value)) {
        if (Double.isNaN(min) || comparator().compare(min, min_value) > 0) {
          min = min_value;
        }
      }
      if (!Double.isNaN(max_value)) {
        if (Double.isNaN(max) || comparator().compare(max, max_value) < 0) {
          max = max_value;
        }
      }
      return;
    }

    if (comparator().compare(min, min_value) > 0) {
      min = min_value;
    }
    if (comparator().compare(max, max_value) < 0) {
      max = max_value;
    }
  }

  public void initializeStats(double min_value, double max_value) {
    min = min_value;
    max = max_value;
    this.markAsNotEmpty();
  }

  @Override
  public Double genericGetMin() {
    return min;
  }

  @Override
  public Double genericGetMax() {
    return max;
  }

  public int compareMinToValue(double value) {
    return comparator().compare(min, value);
  }

  public int compareMaxToValue(double value) {
    return comparator().compare(max, value);
  }

  public double getMax() {
    return max;
  }

  public double getMin() {
    return min;
  }

  public void setMinMax(double min, double max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }

  @Override
  public DoubleStatistics copy() {
    return new DoubleStatistics(this);
  }
}
