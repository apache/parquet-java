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
package org.apache.parquet.hadoop.vector;

import org.apache.parquet.vector.BooleanColumnVector;
import org.apache.parquet.vector.ByteColumnVector;
import org.apache.parquet.vector.ColumnVector;
import org.apache.parquet.vector.DoubleColumnVector;
import org.apache.parquet.vector.FloatColumnVector;
import org.apache.parquet.vector.IntColumnVector;
import org.apache.parquet.vector.LongColumnVector;
import org.apache.parquet.vector.RowBatch;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ParquetVectorTestUtils {

  public static boolean DEBUG = false;

  public static <T> void assertVectorTypes(RowBatch batch, int expectedColumnCount, Class... vectorType) {
    assertTrue("Must have a single column", batch.getColumns().length == expectedColumnCount);
    for (int i = 0 ; i < expectedColumnCount ; i++) {
      ColumnVector vector = batch.getColumns()[i];
      assertTrue(vectorType[i].isInstance(vector));
      log("Read " + vector.size() + " elements of type " + vectorType[i]);
    }
  }

  public static <T> void assertSingleColumnRead(ColumnVector vector, Class<T> elementType, int index, T expectedValue) {
    if (elementType == int.class) {
      int read = ((IntColumnVector) vector).values[index];
      log(read);
      assertEquals(expectedValue, read);
    } else if (elementType == long.class) {
      long read = ((LongColumnVector) vector).values[index];
      log(read);
      assertEquals(expectedValue, read);
    } else if (elementType == double.class) {
      double read = ((DoubleColumnVector) vector).values[index];
      log(read);
      assertEquals(Double.class.cast(expectedValue), read, 0.01);
    } else if (elementType == float.class) {
      float read = ((FloatColumnVector) vector).values[index];
      log(read);
      assertEquals(Float.class.cast(expectedValue), read, 0.01);
    } else if (elementType == boolean.class) {
      boolean read = ((BooleanColumnVector) vector).values[index];
      log(read);
      assertEquals(expectedValue, read);
    }
  }

  public static <T> void assertFixedLengthByteArrayReads(ByteColumnVector vector, int fixedLength, byte[] expectedValue, int position) {
    byte[] value = new byte[fixedLength];
    System.arraycopy(vector.values, position, value, 0, fixedLength);
    String read = new String(value);
    log(read);
    assertArrayEquals(expectedValue, value);
  }

  public static void log(Object message) {
    if (DEBUG) {
      System.out.println(message);
    }
  }
}
