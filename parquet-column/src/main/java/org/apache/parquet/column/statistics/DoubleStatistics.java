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
import org.apache.parquet.column.statistics.bloomfilter.BloomFilter;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOpts;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterStatistics;

public class DoubleStatistics extends Statistics<Double> implements BloomFilterStatistics<Double>{

  private double max;
  private double min;
  private BloomFilter bloomFilter;
  private boolean isBloomFilterEnabled = false;

  public DoubleStatistics(ColumnStatisticsOpts statisticsOpts){
    super();
    if(statisticsOpts!=null){
      updateBloomFilterOptions(statisticsOpts.getBloomFilterOpts());
    }
  }

  private void updateBloomFilterOptions(BloomFilterOpts.BloomFilterEntry statisticsOpts) {
    if (statisticsOpts != null) {
      bloomFilter =
          new BloomFilter(statisticsOpts.getNumBits(), statisticsOpts.getNumHashFunctions());
      isBloomFilterEnabled = true;
    }
  }

  @Override
  public void updateStats(double value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }

    if (isBloomFilterEnabled) {
      add(value);
    }
  }

  @Override
  void mergeBloomFilters(Statistics stats) {
    if (isBloomFilterEnabled && stats instanceof BloomFilterStatistics) {
      this.bloomFilter.merge(((BloomFilterStatistics) stats).getBloomFilter());
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    DoubleStatistics doubleStats = (DoubleStatistics)stats;
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
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || (16 < size);
  }

  @Override
  public String toString() {
    if(this.hasNonNullValue())
      return String.format("min: %.5f, max: %.5f, num_nulls: %d", min, max, this.getNumNulls());
    else if (!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
    else
      return "no stats for this column";
  }

  public void updateStats(double min_value, double max_value) {
    if (min_value < min) { min = min_value; }
    if (max_value > max) { max = max_value; }
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
  public void add(Double value) {
    bloomFilter.addDouble(value);
  }

  @Override
  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  @Override
  public boolean test(Double value) {
    return bloomFilter.testDouble(value);
  }

  @Override
  public boolean isBloomFilterEnabled() {
    return isBloomFilterEnabled;
  }
}
