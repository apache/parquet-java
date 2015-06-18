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
package org.apache.parquet.vector;

public class ByteColumnVector extends ColumnVector
{
  public byte[] values;
  public int capacity;
  /**
   * For fixed len byte array type sizeOfValues can be > 1
   * @param sizeOfValues
   */
  public ByteColumnVector(int sizeOfValues) {
    super(byte.class);
    capacity = DEFAULT_VECTOR_LENGTH * sizeOfValues;
    values = new byte[capacity];
  }

  /**
   * Ensure that the vector can hold requiredCapacity bytes
   */
  public void ensureCapacity(int requiredCapacity) {
    if (capacity >= requiredCapacity) {
      return;
    }

    int multiplier = 2;
    while (capacity * multiplier < requiredCapacity) {
      multiplier++;
    }

    multiplier *= 1.25; // have some slack

    byte[] newBuffer = new byte[capacity * multiplier];
    System.arraycopy(values, 0, newBuffer, 0, values.length);
    values = newBuffer;
    capacity = capacity * multiplier;
  }
}
