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
package parquet.filter2.predicate;

import static parquet.Preconditions.checkNotNull;

/**
 * Contains statistics about a group of records
 */
public class Statistics<T> {
  private final T min;
  private final T max;

  public Statistics(T min, T max) {
    this.min = checkNotNull(min, "min");
    this.max = checkNotNull(max, "max");
  }

  public T getMin() {
    return min;
  }

  public T getMax() {
    return max;
  }
}
