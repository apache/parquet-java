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

import parquet.io.api.Binary;

public class BinaryStatistics extends Statistics{

  private Binary max;
  private Binary min;

  @Override
  public void updateStats(Binary value) {
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
      BinaryStatistics binaryStats = (BinaryStatistics)stats;
      return (max.equals(binaryStats.getMax())) &&
             (min.equals(binaryStats.getMin())) &&
             (this.getNumNulls() == binaryStats.getNumNulls());
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      BinaryStatistics binaryStats = (BinaryStatistics)stats;
      if (this.isEmpty()) {
        this.setNumNulls(stats.getNumNulls());
        max = binaryStats.getMax();
        min = binaryStats.getMin();
        this.markAsNotEmpty();
      } else {
        incrementNumNulls(stats.getNumNulls());
        updateMax(binaryStats.getMax());
        updateMin(binaryStats.getMin());
      }
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
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

  public void updateMax(Binary value){
    //TODO: what's a better comparison without converting to string?
    if (max.toStringUsingUTF8().compareTo(value.toStringUsingUTF8()) < 0) { max = value; }
  }

  public void updateMin(Binary value) {
    //TODO: what's a better comparison without converting to string?
    if (min.toStringUsingUTF8().compareTo(value.toStringUsingUTF8()) > 0) { min = value; }
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