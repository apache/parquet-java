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
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class BooleanStatistics extends Statistics<Boolean> {

  private boolean max;
  private boolean min;

  public BooleanStatistics() {
    // Creating a fake primitive type to have the proper comparator
    this(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(""));
  }

  BooleanStatistics(Type type) {
    super(type.<Boolean>comparator());
  }

  @Override
  public void updateStats(boolean value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
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

  public void updateStats(boolean min_value, boolean max_value) {
    if (comparator().compare(min, min_value) > 0) { min = min_value; }
    if (comparator().compare(max, max_value) < 0) { max = max_value; }
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

  public int compareToMin(boolean value) {
    return comparator().compare(min, value);
  }

  public int compareToMax(boolean value) {
    return comparator().compare(max, value);
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
