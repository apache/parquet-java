/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.statistics;

import parquet.bytes.BytesUtils;

public class BooleanStatistics extends Statistics<Boolean> {

  private boolean max;
  private boolean min;

  @Override
  public void updateStats(boolean value) {
    if (this.isEmpty() || !this.hasValidValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    BooleanStatistics boolStats = (BooleanStatistics)stats;
    if (this.isEmpty() || !this.hasValidValue()) {
      initializeStats(boolStats.getMin(), boolStats.getMax());
    } else {
      updateStats(boolStats.getMin(), boolStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToBool(maxBytes);
    min = BytesUtils.bytesToBool(minBytes);
    this.markHasValidValue();
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
  public String toString() {
    if(this.hasValidValue())
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
      this.markHasValidValue();
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
  
  @Override
  public void incrementNumNulls() {
    if (this.isEmpty()) {
      this.markAsNotEmpty();
    }
    
    super.incrementNumNulls();
  }

  @Override
  public void incrementNumNulls(long increment) {
    if (this.isEmpty()) {
      this.markAsNotEmpty();
    }

    super.incrementNumNulls(increment);
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
    this.markHasValidValue();
    this.markAsNotEmpty();
  }
}
