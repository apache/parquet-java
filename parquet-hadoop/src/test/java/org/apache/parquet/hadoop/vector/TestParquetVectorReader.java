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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.vector.BooleanColumnVector;
import org.apache.parquet.vector.ByteColumnVector;
import org.apache.parquet.vector.DoubleColumnVector;
import org.apache.parquet.vector.FloatColumnVector;
import org.apache.parquet.vector.IntColumnVector;
import org.apache.parquet.vector.LongColumnVector;
import org.apache.parquet.vector.RowBatch;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.*;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertTrue;

public abstract class TestParquetVectorReader
{
  private static final int nElements = 2500;
  protected static final Configuration conf = new Configuration();
  protected static final Path file = new Path("target/test/TestParquetVectorReader/testParquetFile");
  protected static final MessageType schema = parseMessageType(
                                                "message test { "
                                                + "required int32 int32_field; "
                                                + "required int64 int64_field; "
                                                + "required int96 int96_field; "
                                                + "required double double_field; "
                                                + "required float float_field; "
                                                + "required boolean boolean_field; "
                                                + "required fixed_len_byte_array(3) flba_field; "
                                                + "optional fixed_len_byte_array(1) some_null_field; "
                                                + "optional fixed_len_byte_array(1) all_null_field; "
                                                + "} ");

  protected static void writeData(SimpleGroupFactory f, ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < nElements; i++) {
      Group group = f.newGroup()
              .append("int32_field", i)
              .append("int64_field", (long) 2 * i)
              .append("int96_field", Binary.fromByteArray("999999999999".getBytes()))
              .append("double_field", i * 1.0)
              .append("float_field", ((float) (i * 2.0)))
              .append("boolean_field", i % 5 == 0)
              .append("flba_field", "foo");

      if (i % 2 == 1) {
        group.append("some_null_field", "x");
      }
      writer.write(group);
    }
    writer.close();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(file)) {
      fs.delete(file, true);
    }
  }

  private ParquetReader createParquetReader(String schemaString) throws IOException {
    conf.set(PARQUET_READ_SCHEMA, schemaString);
    return ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
  }

  @Test
  public void testInt32Read() throws Exception {
    ParquetReader reader = createParquetReader("message test { required int32 int32_field;}");

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, IntColumnVector.class);
        IntColumnVector vector = IntColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, int.class, i, expected);
          expected++;
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testInt64Read() throws Exception {
    ParquetReader reader = createParquetReader("message test { required int64 int64_field;}");

    try {
      long expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, LongColumnVector.class);
        LongColumnVector vector = LongColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, long.class, i, expected * 2);
          expected++;
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testInt96Read() throws Exception {
    ParquetReader reader = createParquetReader("message test { required int96 int96_field;}");

    try {
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0, position = 0 ; i < vector.size(); i++, position += 12) {
          assertFixedLengthByteArrayReads(vector, 12, "999999999999".getBytes(), position);
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testDoubleRead() throws Exception {
    ParquetReader reader = createParquetReader("message test { required double double_field;}");

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, DoubleColumnVector.class);
        DoubleColumnVector vector = DoubleColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, double.class, i, expected * 1.0);
          expected++;
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testFloatRead() throws Exception {
    ParquetReader reader = createParquetReader("message test { required float float_field;}");

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, FloatColumnVector.class);
        FloatColumnVector vector = FloatColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, float.class, i, (float) (expected * 2.0));
          expected++;
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testBooleanRead() throws Exception {
    ParquetReader reader = createParquetReader("message test { required boolean boolean_field;}");

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, BooleanColumnVector.class);
        BooleanColumnVector vector = BooleanColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, boolean.class, i, expected % 5 == 0);
          expected++;
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testFixedLenByteArrayRead() throws Exception {
    ParquetReader reader = createParquetReader("message test { required fixed_len_byte_array(3) flba_field;}");

    try {
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0, position = 0; i < vector.size(); i++, position += 3) {
          assertFixedLengthByteArrayReads(vector, 3, "foo".getBytes(), position);
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testNullRead() throws Exception {
    ParquetReader reader = createParquetReader("message test { optional fixed_len_byte_array(1) some_null_field;}");

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0, position = 0 ; i < vector.size(); i++, expected++) {
          if (expected % 2 == 1) {
            assertFixedLengthByteArrayReads(vector, 1, "x".getBytes(), position);
            position++;
          } else {
            assertTrue(vector.isNull[i]);
          }
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testAllNullRead() throws Exception {
    ParquetReader reader = createParquetReader("message test { optional fixed_len_byte_array(1) all_null_field;}");

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++, expected++) {
          assertTrue(vector.isNull[i]);
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMultipleReads() throws Exception {
    ParquetReader reader = createParquetReader("message test { required double double_field; required int32 int32_field; }");

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 2, DoubleColumnVector.class, IntColumnVector.class);
        DoubleColumnVector doubleVector = DoubleColumnVector.class.cast(batch.getColumns()[0]);
        IntColumnVector intVector = IntColumnVector.class.cast(batch.getColumns()[1]);
        for (int i = 0 ; i < doubleVector.size(); i++) {
          assertSingleColumnRead(doubleVector, double.class, i, expected * 1.0);
          assertSingleColumnRead(intVector, int.class, i, expected);
          expected++;
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testReadAll() throws Exception {
    ParquetReader reader = createParquetReader(schema.toString());

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 9, IntColumnVector.class, LongColumnVector.class,
                ByteColumnVector.class, DoubleColumnVector.class, FloatColumnVector.class, BooleanColumnVector.class,
                ByteColumnVector.class, ByteColumnVector.class, ByteColumnVector.class);

        IntColumnVector int32_field = IntColumnVector.class.cast(batch.getColumns()[0]);
        LongColumnVector int64_field = LongColumnVector.class.cast(batch.getColumns()[1]);
        ByteColumnVector int96_field = ByteColumnVector.class.cast(batch.getColumns()[2]);
        DoubleColumnVector double_field = DoubleColumnVector.class.cast(batch.getColumns()[3]);
        FloatColumnVector float_field = FloatColumnVector.class.cast(batch.getColumns()[4]);
        BooleanColumnVector boolean_field = BooleanColumnVector.class.cast(batch.getColumns()[5]);
        ByteColumnVector flba_field = ByteColumnVector.class.cast(batch.getColumns()[6]);
        ByteColumnVector some_null_field = ByteColumnVector.class.cast(batch.getColumns()[7]);
        ByteColumnVector all_null_field = ByteColumnVector.class.cast(batch.getColumns()[8]);

        int flba_field_position = 0;
        int null_field_position = 0;
        int int96_field_position = 0;
        for (int i = 0 ; i < int32_field.size(); i++) {
          assertSingleColumnRead(int32_field, int.class, i, expected);
          assertSingleColumnRead(int64_field, long.class, i, (long) (expected * 2));

          assertFixedLengthByteArrayReads(int96_field, 12, "999999999999".getBytes(), int96_field_position);
          int96_field_position += 12;

          assertSingleColumnRead(double_field, double.class, i, expected * 1.0);
          assertSingleColumnRead(float_field, float.class, i, (float) (expected * 2.0));
          assertSingleColumnRead(boolean_field, boolean.class, i, expected % 5 == 0);

          assertFixedLengthByteArrayReads(flba_field, 3, "foo".getBytes(), flba_field_position);
          flba_field_position += 3;

          if (expected % 2 == 1) {
            assertFixedLengthByteArrayReads(some_null_field, 1, "x".getBytes(), null_field_position);
            null_field_position++;
          } else {
            assertTrue(some_null_field.isNull[i]);
          }
          assertTrue(all_null_field.isNull[i]);
          expected++;
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testEnsureCapacity() {
    ByteColumnVector vector = new ByteColumnVector(4);
    byte[] test = "test".getBytes();
    int position = 0;
    for (int i = 0 ; i < 100; i++) {
      System.arraycopy(test, 0, vector.values, position, test.length);
      position += test.length;
    }
    vector.ensureCapacity(600000);
    position = 0;
    for (int i = 0 ; i < 100; i++) {
      assertFixedLengthByteArrayReads(vector, 4, test, position);
      position += test.length;
    }
  }
}
