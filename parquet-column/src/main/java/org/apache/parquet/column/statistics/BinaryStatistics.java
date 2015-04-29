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

import org.apache.parquet.io.api.Binary;

public class BinaryStatistics extends Statistics<Binary> {

  private Binary max;
  private Binary min;

  @Override
  public void updateStats(Binary value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    BinaryStatistics binaryStats = (BinaryStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(binaryStats.getMin(), binaryStats.getMax());
    } else {
      updateStats(binaryStats.getMin(), binaryStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Binary.fromByteArray(maxBytes);
    min = Binary.fromByteArray(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return max.getBytes();
  }

  @Override
  public byte[] getMinBytes() {
    return min.getBytes();
  }

  @Override
  public String toString() {
    if (this.hasNonNullValue())
      return String.format("min: %s, max: %s, num_nulls: %d", min.toStringUsingUTF8(), max.toStringUsingUTF8(), this.getNumNulls());
   else if (!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
   else
      return "no stats for this column";
  }

  public void updateStats(Binary min_value, Binary max_value) {
    if (min.compareTo(min_value) > 0) { min = min_value; }
    if (max.compareTo(max_value) < 0) { max = max_value; }
  }

  public void initializeStats(Binary min_value, Binary max_value) {
      min = min_value;
      max = max_value;
      this.markAsNotEmpty();
  }

  @Override
  public Binary genericGetMin() {
    return min;
  }

  @Override
  public Binary genericGetMax() {
    return max;
  }

  public Binary getMax() {
    return max;
  }

  public Binary getMin() {
    return min;
  }

  public void setMinMax(Binary min, Binary max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}
