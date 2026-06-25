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
import org.apache.parquet.schema.Float16;
import org.apache.parquet.schema.PrimitiveType;

public class Float16Statistics extends BinaryStatistics {

  Float16Statistics(PrimitiveType type) {
    super(type);
    incrementNanCount(0);
  }

  private Float16Statistics(Float16Statistics other) {
    super(other);
    incrementNanCount(other.getNanCount());
  }

  @Override
  public void updateStats(Binary value) {
    if (Float16.isNaN(value.get2BytesLittleEndian())) {
      incrementNanCount();
      return;
    }
    if (!this.hasNonNullValue()) {
      initializeStats(normalizeMinValue(value), normalizeMaxValue(value));
    } else {
      updateStats(normalizeMinValue(value), normalizeMaxValue(value));
    }
  }

  @Override
  @Deprecated
  public void updateStats(Binary min_value, Binary max_value) {
    boolean minIsNaN = Float16.isNaN(min_value.get2BytesLittleEndian());
    boolean maxIsNaN = Float16.isNaN(max_value.get2BytesLittleEndian());
    if (minIsNaN && maxIsNaN) {
      return;
    }
    if (minIsNaN) {
      min_value = max_value;
    }
    if (maxIsNaN) {
      max_value = min_value;
    }

    min_value = normalizeMinValue(min_value);
    max_value = normalizeMaxValue(max_value);

    if (comparator().compare(min, min_value) > 0) {
      min = min_value.copy();
    }
    if (comparator().compare(max, max_value) < 0) {
      max = max_value.copy();
    }
  }

  @Override
  public Float16Statistics copy() {
    return new Float16Statistics(this);
  }

  private Binary normalizeMinValue(Binary value) {
    return isZero(value) ? Float16.NEGATIVE_ZERO_LITTLE_ENDIAN : value;
  }

  private Binary normalizeMaxValue(Binary value) {
    return isZero(value) ? Float16.POSITIVE_ZERO_LITTLE_ENDIAN : value;
  }

  private static boolean isZero(Binary value) {
    return (value.get2BytesLittleEndian() & 0x7fff) == 0;
  }
}
