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

public class FloatStatistics extends Statistics<Float> {

  // A fake type object to be used to generate the proper comparator
  private static final PrimitiveType DEFAULT_FAKE_TYPE = Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
      .named("fake_float_type");

  private float max;
  private float min;

  /**
   * @deprecated will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead
   */
  @Deprecated
  public FloatStatistics() {
    // Creating a fake primitive type to have the proper comparator
    this(DEFAULT_FAKE_TYPE);
  }

  FloatStatistics(PrimitiveType type) {
    super(type);
  }

  private FloatStatistics(FloatStatistics other) {
    super(other.type());
    if (other.hasNonNullValue()) {
      initializeStats(other.min, other.max);
    }
    setNumNulls(other.getNumNulls());
  }

  @Override
  public void updateStats(float value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    FloatStatistics floatStats = (FloatStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(floatStats.getMin(), floatStats.getMax());
    } else {
      updateStats(floatStats.getMin(), floatStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Float.intBitsToFloat(BytesUtils.bytesToInt(maxBytes));
    min = Float.intBitsToFloat(BytesUtils.bytesToInt(minBytes));
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
  String stringify(Float value) {
    return stringifier.stringify(value);
  }

  @Override
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || (8 < size);
  }

  public void updateStats(float min_value, float max_value) {
    if (comparator().compare(min, min_value) > 0) { min = min_value; }
    if (comparator().compare(max, max_value) < 0) { max = max_value; }
  }

  public void initializeStats(float min_value, float max_value) {
      min = min_value;
      max = max_value;
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

  public int compareMinToValue(float value) {
    return comparator().compare(min, value);
  }

  public int compareMaxToValue(float value) {
    return comparator().compare(max, value);
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
    this.markAsNotEmpty();
  }

  @Override
  public FloatStatistics copy() {
    return new FloatStatistics(this);
  }
}
