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
package org.apache.parquet.column;

import org.apache.parquet.schema.PrimitiveComparator;

/**
 * This class calculates the max and min values of an iterable collection.
 */
public final class MinMax<T> {
  private T min = null;
  private T max = null;

  public MinMax(PrimitiveComparator comparator, Iterable<T> iterable) {
    getMinAndMax(comparator, iterable);
  }

  public T getMin() {
    return min;
  }

  public T getMax() {
    return max;
  }

  private void getMinAndMax(PrimitiveComparator comparator, Iterable<T> iterable) {
    iterable.forEach(element -> {
      if (max == null) {
        max = element;
      } else if (element != null) {
        if (comparator.compare(max, element) < 0) {
          max = element;
        }
      }
      if (min == null) {
        min = element;
      } else if (element != null) {
        if (comparator.compare(min, element) > 0) {
          min = element;
        }
      }
    });
  }
}
