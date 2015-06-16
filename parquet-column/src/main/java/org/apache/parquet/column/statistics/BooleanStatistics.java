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

public class BooleanStatistics extends Statistics<Boolean> {

  private boolean max;
  private boolean min;

  public void updateStats(boolean value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  void mergeBloomFilters(Statistics stats) {
    // Do nothing
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    BooleanStatistics boolStats = (BooleanStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(boolStats.getMin(), boolStats.getMax());
    } else {
      updateStats(boolStats.getMin(), boolStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToBool(maxBytes);
    min = BytesUtils.bytesToBool(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.booleanToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.booleanToBytes(min);
  }

  @Override
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || (2 < size);
  }

  @Override
  public String toString() {
    if (this.hasNonNullValue())
      return String.format("min: %b, max: %b, num_nulls: %d", min, max, this.getNumNulls());
    else if(!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
    else  
      return "no stats for this column";
  }

  public void updateStats(boolean min_value, boolean max_value) {
    if (min && !min_value) { min = min_value; }
    if (!max && max_value) { max = max_value; }
  }

  public void initializeStats(boolean min_value, boolean max_value) {
      min = min_value;
      max = max_value;
      this.markAsNotEmpty();
  }

  @Override
  public Boolean genericGetMin() {
    return min;
  }

  @Override
  public Boolean genericGetMax() {
    return max;
  }

  public boolean getMax() {
    return max;
  }

  public boolean getMin() {
    return min;
  }

  public void setMinMax(boolean min, boolean max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}
