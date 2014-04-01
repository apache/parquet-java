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

public class BooleanStatistics extends Statistics{

  private boolean max;
  private boolean min;

  @Override
  public void updateStats(boolean value) {
    if (this.isEmpty()) {
      max = value;
      min = value;
      this.markAsNotEmpty();
    } else {
      updateMax(value);
      updateMin(value);
    }
  }

  @Override
  public boolean equals(Statistics stats) {
    if (this.getClass() == stats.getClass()) {
      BooleanStatistics booleanStats = (BooleanStatistics)stats;
      return (max == booleanStats.getMax()) &&
             (min == booleanStats.getMin()) &&
             (this.getNumNulls() == stats.getNumNulls());
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      BooleanStatistics boolStats = (BooleanStatistics)stats;
      if (this.isEmpty()) {
        this.setNumNulls(stats.getNumNulls());
        max = boolStats.getMax();
        min = boolStats.getMin();
        this.markAsNotEmpty();
      } else {
        incrementNumNulls(stats.getNumNulls());
        updateMax(boolStats.getMax());
        updateMin(boolStats.getMin());
      }
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
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

  public void updateMax(boolean value){
    if (!max && value) { max = value; }
  }

  public void updateMin(boolean value) {
    if (min && !value) { min = value; }
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