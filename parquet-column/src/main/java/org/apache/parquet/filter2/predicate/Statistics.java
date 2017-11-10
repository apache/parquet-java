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

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * Contains statistics about a group of records
 */
public class Statistics<T> {
  private final T min;
  private final T max;
  private final Comparator<T> comparator;

  public Statistics(T min, T max, Comparator<T> comparator) {
    this.min = checkNotNull(min, "min");
    this.max = checkNotNull(max, "max");
    this.comparator = checkNotNull(comparator, "comparator");
  }

  /**
   * Returns the generic object representing the min value in the statistics.
   * The self-comparison logic of {@code T} might not proper for the actual logical type (e.g. unsigned int). Use {@link
   * #getComparator()} for comparing.
   */
  public T getMin() {
    return min;
  }

  /**
   * Returns the generic object representing the max value in the statistics.
   * The self-comparison logic of {@code T} might not proper for the actual logical type (e.g. unsigned int). Use {@link
   * #getComparator()} for comparing.
   */
  public T getMax() {
    return max;
  }

  /**
   * Returns the comparator to be used to compare two generic values in the proper way (e.g. unsigned comparison for
   * UINT_32)
   */
  public Comparator<T> getComparator() {
    return comparator;
  }
}
