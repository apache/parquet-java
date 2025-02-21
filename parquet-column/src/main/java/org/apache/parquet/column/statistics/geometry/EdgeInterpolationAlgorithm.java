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
package org.apache.parquet.column.statistics.geometry;

/**
 * Edge interpolation algorithm for Geography logical type
 */
public enum EdgeInterpolationAlgorithm {
  SPHERICAL(0),
  VINCENTY(1),
  THOMAS(2),
  ANDOYER(3),
  KARNEY(4);

  private final int value;

  private EdgeInterpolationAlgorithm(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static EdgeInterpolationAlgorithm findByValue(int value) {
    switch (value) {
      case 0:
        return SPHERICAL;
      case 1:
        return VINCENTY;
      case 2:
        return THOMAS;
      case 3:
        return ANDOYER;
      case 4:
        return KARNEY;
      default:
        return null;
    }
  }
}
