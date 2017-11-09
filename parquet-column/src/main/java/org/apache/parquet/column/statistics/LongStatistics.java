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

import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongComparators;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.Comparators;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

public class LongStatistics extends Statistics<Long> {

  private final Comparators.LongComparator comparator;
  private long max;
  private long min;

  /**
   * @deprecated Use {@link Statistics#getStatsBasedOnType(PrimitiveType.PrimitiveTypeName, OriginalType)} instead
   */
  @Deprecated
  public LongStatistics() {
    this(null);
  }

  LongStatistics(OriginalType logicalType) {
    this.comparator = Comparators.int64Comparator(logicalType);
  }

  @Override
  public void updateStats(long value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    LongStatistics longStats = (LongStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(longStats.getMin(), longStats.getMax());
    } else {
      updateStats(longStats.getMin(), longStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToLong(maxBytes);
    min = BytesUtils.bytesToLong(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.longToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.longToBytes(min);
  }

  @Override
  public String minAsString() {
    return comparator.toString(min);
  }

  @Override
  public String maxAsString() {
    return comparator.toString(max);
  }

  @Override
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || (16 < size);
  }

  public void updateStats(long min_value, long max_value) {
    if (comparator.compare(min, min_value) > 0) { min = min_value; }
    if (comparator.compare(max, max_value) < 0) { max = max_value; }
  }

  public void initializeStats(long min_value, long max_value) {
      min = min_value;
      max = max_value;
      this.markAsNotEmpty();
  }

  @Override
  public Long genericGetMin() {
    return min;
  }

  @Override
  public Long genericGetMax() {
    return max;
  }

  @Override
  public LongComparator comparator() {
    return LongComparators.NATURAL_COMPARATOR;
  }

  public int compareToMin(long value) {
    return comparator.compare(min, value);
  }

  public int compareToMax(long value) {
    return comparator.compare(max, value);
  }

  public long getMax() {
    return max;
  }

  public long getMin() {
    return min;
  }

  public void setMinMax(long min, long max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}
