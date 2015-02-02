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
package parquet;

/**
 * Utilities for working with ints
 *
 * @author Alex Levenson
 */
public final class Ints {
  private Ints() { }

  /**
   * Cast value to a an int, or throw an exception
   * if there is an overflow.
   *
   * @param value a long to be casted to an int
   * @return an int that is == to value
   * @throws IllegalArgumentException if value can't be casted to an int
   */
  public static int checkedCast(long value) {
    int valueI = (int) value;
    if (valueI != value) {
      throw new IllegalArgumentException(String.format("Overflow casting %d to an int", value));
    }
    return valueI;
  }
}
