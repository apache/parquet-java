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
package org.apache.parquet.io;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.vector.BooleanColumnVector;
import org.apache.parquet.io.vector.ByteColumnVector;
import org.apache.parquet.io.vector.DoubleColumnVector;
import org.apache.parquet.io.vector.FloatColumnVector;
import org.apache.parquet.io.vector.IntColumnVector;
import org.apache.parquet.io.vector.LongColumnVector;
import org.apache.parquet.io.vector.ObjectColumnVector;

public abstract class ColumnVector
{
  public static final int MAX_VECTOR_LENGTH = 1024;
  protected Class valueType;
  public final boolean [] isNull;
  private int numValues;

  public ColumnVector() {
    this.isNull = new boolean[MAX_VECTOR_LENGTH];
  }

  /**
   * @return the type of the elements in this vector
   */
  public Class getType() {
    return valueType;
  }

  /**
   * @return the number of values in this vector
   */
  public int size() {
    return numValues;
  }

  void setNumberOfValues(int numValues) {
    this.numValues = numValues;
  }

  public static ColumnVector from(ColumnDescriptor descriptor) {
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
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
        return new ByteColumnVector();
      default:
        throw new IllegalArgumentException("Unhandled column type " + descriptor.getType());
    }
  }

  public static <T> ObjectColumnVector<T> ofType(Class<T> clazz) {
    return new ObjectColumnVector<T>(clazz);
  }

  /**
   * Reads a single value from the given reader to the specified index into the vector
   * @param reader
   * @param index
   */
  public void readFrom(ColumnReader reader, int index) {
    if (index >= MAX_VECTOR_LENGTH) {
      throw new IllegalArgumentException("index must be smaller than max vector length");
    }
    doReadFrom(reader, index);
  }

  protected abstract void doReadFrom(ColumnReader reader, int index);
}
