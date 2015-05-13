/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.vector;

import org.apache.parquet.column.ColumnDescriptor;

import java.nio.ByteBuffer;

public abstract class ColumnVector
{
  public static final int DEFAULT_VECTOR_LENGTH = 1024;
  protected Class valueType;
  public ByteBuffer values;
  public boolean [] isNull;
  private int numValues;

  ColumnVector(Class valueType, int sizeOfValue) {
    this.valueType = valueType;
    this.values = ByteBuffer.allocate(DEFAULT_VECTOR_LENGTH * sizeOfValue);
    this.isNull = new boolean[DEFAULT_VECTOR_LENGTH];
  }

  /**
   * @return the type of this vector
   */
  public Class getType(){
    return valueType;
  }

  /**
   * @return the number of values in this column vector
   */
  public int size() {
    return numValues;
  }

  public void setNumberOfValues(int numValues)
  {
    this.numValues = numValues;
  }

  public static final ColumnVector newColumnVector(ColumnDescriptor descriptor) {
    switch (descriptor.getType()) {
      case BOOLEAN:
        return new BooleanColumnVector();
      case DOUBLE:
        return new DoubleColumnVector();
      case FLOAT:
        return new FloatColumnVector();
      case INT32:
        return new IntColumnVector();
      case INT64:
        return new LongColumnVector();
      case BINARY:
        return new ByteColumnVector(1);
      case INT96:
        return new ByteColumnVector(12); //TODO does this hold for all encodings?
      case FIXED_LEN_BYTE_ARRAY:
        return new ByteColumnVector(descriptor.getTypeLength());
      default:
        throw new IllegalArgumentException("Unhandled column type " + descriptor.getType());
    }
  }

  /**
   * Ensure that the remaining capacity in the values byte buffer > requiredCapacity
   */
  public void ensureCapacity(int requiredCapacity) {
    if (values.remaining() >= requiredCapacity) {
      return;
    }

    int capacity = values.capacity();
    int multiplier = 2;
    while (capacity * multiplier - values.position() < requiredCapacity) {
      multiplier++;
    }

    multiplier *= 1.25; // have some slack

    ByteBuffer newBuffer = ByteBuffer.allocate(capacity * multiplier);
    int currentPosition = values.position();
    System.arraycopy(values.array(), 0, newBuffer.array(), 0, values.array().length);
    newBuffer.position(currentPosition);
    values = newBuffer;
  }
}
