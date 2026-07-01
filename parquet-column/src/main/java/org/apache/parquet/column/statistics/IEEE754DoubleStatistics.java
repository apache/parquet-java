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

import org.apache.parquet.schema.PrimitiveType;

public class IEEE754DoubleStatistics extends DoubleStatistics {

  IEEE754DoubleStatistics(PrimitiveType type) {
    super(type);
  }

  private IEEE754DoubleStatistics(IEEE754DoubleStatistics other) {
    super(other);
  }

  @Override
  public void updateStats(double value) {
    if (Double.isNaN(value)) {
      incrementNanCount();
    }
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void updateStats(double min_value, double max_value) {
    boolean minValueIsNaN = Double.isNaN(min_value);
    boolean minIsNaN = Double.isNaN(min);
    if (minValueIsNaN) {
      if (minIsNaN && comparator().compare(min, min_value) > 0) {
        min = min_value;
      }
    } else if (minIsNaN || comparator().compare(min, min_value) > 0) {
      min = min_value;
    }

    boolean maxValueIsNaN = Double.isNaN(max_value);
    boolean maxIsNaN = Double.isNaN(max);
    if (maxValueIsNaN) {
      if (maxIsNaN && comparator().compare(max, max_value) < 0) {
        max = max_value;
      }
    } else if (maxIsNaN || comparator().compare(max, max_value) < 0) {
      max = max_value;
    }
  }

  @Override
  public IEEE754DoubleStatistics copy() {
    return new IEEE754DoubleStatistics(this);
  }
}
