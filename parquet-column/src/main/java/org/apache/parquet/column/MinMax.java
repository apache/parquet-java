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

import java.util.Iterator;
import java.util.Set;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;

/**
 * This class calculates the max and min values of a Set.
 */
public final class MinMax<T> {
  private PrimitiveComparator comparator;
  private Iterator<T> iterator;
  private T min = null;
  private T max = null;

  public MinMax(PrimitiveComparator comparator, Iterator<T> iterator) {
    this.comparator = comparator;
    this.iterator = iterator;
    getMinAndMax();
  }

  public T getMin() {
    return min;
  }

  public T getMax() {
    return max;
  }

  private void getMinAndMax() {
    while(iterator.hasNext())  {
      T element = iterator.next();
      if (max == null) {
        max = element;
      } else if (max != null && element != null) {
        if ((element instanceof Integer &&
          ((PrimitiveComparator<Integer>)comparator).compare((Integer)max, (Integer)element) < 0) ||
          (element instanceof Binary &&
            ((PrimitiveComparator<Binary>)comparator).compare((Binary)max, (Binary)element) < 0) ||
          (element instanceof Double &&
             ((PrimitiveComparator<Double>)comparator).compare((Double)max, (Double)element) < 0) ||
          (element instanceof Float &&
             ((PrimitiveComparator<Float>)comparator).compare((Float)max, (Float)element) < 0) ||
          (element instanceof Boolean &&
            ((PrimitiveComparator<Boolean>)comparator).compare((Boolean)max, (Boolean)element) < 0) ||
          (element instanceof Long &&
            ((PrimitiveComparator<Long>)comparator).compare((Long) max, (Long)element) < 0))
          max = element;
      }
      if (min == null) {
        min = element;
      } else if (min != null && element != null) {
        if ((element instanceof Integer &&
          ((PrimitiveComparator<Integer>)comparator).compare((Integer)min, (Integer)element) > 0) ||
          (element instanceof Binary &&
            ((PrimitiveComparator<Binary>)comparator).compare((Binary)min, (Binary)element) > 0) ||
          (element instanceof Double &&
            ((PrimitiveComparator<Double>)comparator).compare((Double)min, (Double)element) > 0) ||
          (element instanceof Float &&
             ((PrimitiveComparator<Float>)comparator).compare((Float)min, (Float)element) > 0) ||
          (element instanceof Boolean &&
            ((PrimitiveComparator<Boolean>)comparator).compare((Boolean)min, (Boolean)element) > 0) ||
          (element instanceof Long &&
            ((PrimitiveComparator<Long>)comparator).compare((Long)min, (Long)element) > 0))
          min = element;
      }
    }
  }
}
