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

import org.apache.parquet.ParquetRuntimeException;

/**
 * Thrown if the two Statistics objects have mismatching types
 */
public class StatisticsClassException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public StatisticsClassException(String className1, String className2) {
    this("Statistics classes mismatched: " + className1 + " vs. " + className2);
  }

  private StatisticsClassException(String msg) {
    super(msg);
  }

  static StatisticsClassException create(Statistics<?> stats1, Statistics<?> stats2) {
    if (stats1.getClass() != stats2.getClass()) {
      return new StatisticsClassException(stats1.getClass().toString(), stats2.getClass().toString());
    }
    return new StatisticsClassException(
        "Statistics comparator mismatched: " + stats1.comparator() + " vs. " + stats2.comparator());
  }
}
