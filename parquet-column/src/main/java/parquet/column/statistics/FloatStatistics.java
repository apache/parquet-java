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

public class FloatStatistics extends Statistics<Float> {

  private float max;
  private float min;

  @Override
  public void updateStats(float value) {
    if (this.isEmpty() || !this.hasValidValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    FloatStatistics floatStats = (FloatStatistics)stats;
    if (this.isEmpty() || !this.hasValidValue()) {
      initializeStats(floatStats.getMin(), floatStats.getMax());
    } else {
      updateStats(floatStats.getMin(), floatStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Float.intBitsToFloat(BytesUtils.bytesToInt(maxBytes));
    min = Float.intBitsToFloat(BytesUtils.bytesToInt(minBytes));
    this.markHasValidValue();
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.intToBytes(Float.floatToIntBits(max));
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.intToBytes(Float.floatToIntBits(min));
  }

  @Override
  public String toString() {
    if(this.hasValidValue())
      return String.format("min: %.5f, max: %.5f, num_nulls: %d", min, max, this.getNumNulls());
    else if(!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
    else
      return "no stats for this column";
  }

  public void updateStats(float min_value, float max_value) {
    if (min_value < min) { min = min_value; }
    if (max_value > max) { max = max_value; }
  }

  public void initializeStats(float min_value, float max_value) {
      min = min_value;
      max = max_value;
      this.markHasValidValue();
      this.markAsNotEmpty();
  }

  @Override
  public Float genericGetMin() {
    return min;
  }

  @Override
  public Float genericGetMax() {
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

  public float getMax() {
    return max;
  }

  public float getMin() {
    return min;
  }

  public void setMinMax(float min, float max) {
    this.max = max;
    this.min = min;
    this.markHasValidValue();
    this.markAsNotEmpty();
  }
}
