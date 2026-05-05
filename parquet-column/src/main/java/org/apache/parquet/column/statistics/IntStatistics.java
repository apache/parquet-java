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
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class IntStatistics extends Statistics<Integer> {

  // A fake type object to be used to generate the proper comparator
  private static final PrimitiveType DEFAULT_FAKE_TYPE =
      Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("fake_int32_type");

  private int max;
  private int min;

  /**
   * @deprecated will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead
   */
  @Deprecated
  public IntStatistics() {
    this(DEFAULT_FAKE_TYPE);
  }

  IntStatistics(PrimitiveType type) {
    super(type);
  }

  private IntStatistics(IntStatistics other) {
    super(other.type());
    if (other.hasNonNullValue()) {
      initializeStats(other.min, other.max);
    }
    setNumNulls(other.getNumNulls());
  }

  @Override
  public void updateStats(int value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    IntStatistics intStats = (IntStatistics) stats;
    if (!this.hasNonNullValue()) {
      initializeStats(intStats.getMin(), intStats.getMax());
    } else {
      updateStats(intStats.getMin(), intStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToInt(maxBytes);
    min = BytesUtils.bytesToInt(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.intToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.intToBytes(min);
  }

  @Override
  String stringify(Integer value) {
    return stringifier.stringify(value);
  }

  @Override
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || (8 < size);
  }

  public void updateStats(int min_value, int max_value) {
    if (comparator().compare(min, min_value) > 0) {
      min = min_value;
    }
    if (comparator().compare(max, max_value) < 0) {
      max = max_value;
    }
  }

  public void initializeStats(int min_value, int max_value) {
    min = min_value;
    max = max_value;
    this.markAsNotEmpty();
  }

  @Override
  public Integer genericGetMin() {
    return min;
  }

  @Override
  public Integer genericGetMax() {
    return max;
  }

  public int compareMinToValue(int value) {
    return comparator().compare(min, value);
  }

  public int compareMaxToValue(int value) {
    return comparator().compare(max, value);
  }

  public int getMax() {
    return max;
  }

  public int getMin() {
    return min;
  }

  public void setMinMax(int min, int max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }

  @Override
  public IntStatistics copy() {
    return new IntStatistics(this);
  }
}
