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
package org.apache.parquet.filter2.predicate;

import java.util.Comparator;
import java.util.Objects;

/**
 * Contains statistics about a group of records
 *
 * @param <T> the type of values described by the statistics instance
 */
public class Statistics<T> {
  private final T min;
  private final T max;
  private final Comparator<T> comparator;

  // Intended for use only within Parquet itself.
  /**
   * @param min the min value
   * @param max the max value
   * @deprecated will be removed in 2.0.0. Use {@link #Statistics(Object, Object, Comparator)} instead
   */
  @Deprecated
  public Statistics(T min, T max) {
    this.min = Objects.requireNonNull(min, "min cannot be null");
    this.max = Objects.requireNonNull(max, "max cannot be null");
    this.comparator = null;
  }

  // Intended for use only within Parquet itself.

  /**
   * @param min the min value
   * @param max the max value
   * @param comparator a comparator to use when comparing values described by this statistics instance
   */
  public Statistics(T min, T max, Comparator<T> comparator) {
    this.min = Objects.requireNonNull(min, "min cannot be null");
    this.max = Objects.requireNonNull(max, "max cannot be null");
    this.comparator = Objects.requireNonNull(comparator, "comparator cannot be null");
  }

  /**
   * Returns the generic object representing the min value in the statistics. The
   * natural ordering of type {@code T} defined by the {@code compareTo} method
   * might not be appropriate for the actual logical type. Use
   * {@link #getComparator()} for comparing.
   *
   * @return the min value
   */
  public T getMin() {
    return min;
  }

  /**
   * Returns the generic object representing the max value in the statistics. The
   * natural ordering of type {@code T} defined by the {@code compareTo} method
   * might not be appropriate for the actual logical type. Use
   * {@link #getComparator()} for comparing.
   *
   * @return the max value
   */
  public T getMax() {
    return max;
  }

  /**
   * Returns the comparator to be used to compare two generic values in the proper way (e.g. unsigned comparison for
   * UINT_32)
   *
   * @return a comparator for the values described by the statistics instance
   */
  public Comparator<T> getComparator() {
    return comparator;
  }
}
