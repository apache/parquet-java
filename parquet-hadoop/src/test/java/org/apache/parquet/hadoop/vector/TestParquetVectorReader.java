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
import java.util.Arrays;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.assertFixedLengthByteArrayReads;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.assertSingleColumnRead;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.assertVectorTypes;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.log;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class TestParquetVectorReader
{
  protected static final int nElements = 1236;
  protected static final Configuration conf = new Configuration();
  protected static final Path file = new Path("target/test/TestParquetVectorReader/testParquetFile");
  protected static final MessageType schema = parseMessageType(
                                                "message test { "
                                                + "required binary binary_field; "
                                                + "required int32 int32_field; "
                                                + "required int64 int64_field; "
                                                + "required int96 int96_field; "
                                                + "required double double_field; "
                                                + "required float float_field; "
                                                + "required boolean boolean_field; "
                                                + "required fixed_len_byte_array(3) flba_field; "
                                                + "optional binary null_field; "
                                                + "required binary var_len_binary_field; "
                                                + "} ");

  protected static void writeData(SimpleGroupFactory f, ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < nElements; i++) {
      char c  = (char) ((i % 26) + 'a');
      String b = String.valueOf(c);

      char[] charArray = new char[i + 1];
      Arrays.fill(charArray, c);
      Group group = f.newGroup()
              .append("binary_field", b)
              .append("int32_field", i)
              .append("int64_field", (long) 2 * i)
              .append("int96_field", Binary.fromByteArray("999999999999".getBytes()))
              .append("double_field", i * 1.0)
              .append("float_field", ((float) (i * 2.0)))
              .append("boolean_field", i % 5 == 0)
              .append("var_len_binary_field", new String(charArray))
              .append("flba_field", "foo");

      if (i % 2 == 1) {
        group.append("null_field", "test");
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

  @Test
  public void testBinaryRead()  throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required binary binary_field;}");
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    //prepare the expected data
    StringBuilder expected = new StringBuilder();
    for (int i = 0; i < nElements; i++) {
      String b = String.valueOf((char) ((i % 26) + 'a'));
      expected.append(b);
    }

    try {
      RowBatch batch = new RowBatch();
      StringBuilder actual = new StringBuilder();
      while(true) {
        batch = reader.nextBatch(batch);

        if (batch == null) {
          //EOF
          String actualString = actual.toString();
          String expectedString = expected.toString();
          assertEquals(expectedString, actualString);
          break;
        } else {
          assertVectorTypes(batch, 1, ByteColumnVector.class);
          ByteColumnVector byteVector = ByteColumnVector.class.cast(batch.getColumns()[0]);
          //not all bytes are valid as the underlying array is reused across nextBatch() calls
          int validBytes = byteVector.size();
          String s = new String(Arrays.copyOfRange(byteVector.values, 0, validBytes));
          log(s);
          actual.append(s);
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testInt32Read() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required int32 int32_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();
      int expected = 0;
      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

        assertVectorTypes(batch, 1, IntColumnVector.class);

        IntColumnVector vector = IntColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, int.class, i, expected);
          expected++;
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testInt64Read() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required int64 int64_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();
      long expected = 0;
      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

        assertVectorTypes(batch, 1, LongColumnVector.class);
        LongColumnVector vector = LongColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, long.class, i, expected * 2);
          expected++;
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testInt96Read() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required int96 int96_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();
      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0, position = 0 ; i < vector.size(); i++, position += 12) {
          assertFixedLengthByteArrayReads(vector, 12, "999999999999".getBytes(), position);
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testDoubleRead() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required double double_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();
      int expected = 0;
      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

        assertVectorTypes(batch, 1, DoubleColumnVector.class);
        DoubleColumnVector vector = DoubleColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, double.class, i, expected * 1.0);
          expected++;
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testFloatRead() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required float float_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();
      int expected = 0;
      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

        assertVectorTypes(batch, 1, FloatColumnVector.class);
        FloatColumnVector vector = FloatColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, float.class, i, (float) (expected * 2.0));
          expected++;
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testBooleanRead() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required boolean boolean_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();
      int expected = 0;
      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

        assertVectorTypes(batch, 1, BooleanColumnVector.class);
        BooleanColumnVector vector = BooleanColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, boolean.class, i, expected % 5 == 0);
          expected++;
        }

      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testFixedLenByteArrayRead() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required fixed_len_byte_array(3) flba_field;}");
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();

      while (true) {
        batch = reader.nextBatch(batch);

        if (batch == null) {
          break;
        } else {
          assertVectorTypes(batch, 1, ByteColumnVector.class);
          ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
          for (int i = 0, position = 0; i < vector.size(); i++, position += 3) {
            assertFixedLengthByteArrayReads(vector, 3, "foo".getBytes(), position);
          }
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testNullRead() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { optional binary null_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();
      int expected = 0;
      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

        assertVectorTypes(batch, 1, ByteColumnVector.class);

        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0, position = 0 ; i < vector.size(); i++, expected++) {
          if (expected % 2 == 1) {
            assertFixedLengthByteArrayReads(vector, 4, "test".getBytes(), position);
            position += 4;
          } else {
            assertTrue(vector.isNull[i]);
          }
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testVariableLengthBinaryRead() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required binary var_len_binary_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    try {
      RowBatch batch = new RowBatch();
      int expected = 0;
      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

        assertVectorTypes(batch, 1, ByteColumnVector.class);

        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0, position = 0 ; i < vector.size(); i++, expected++) {
          char c  = (char) ((expected % 26) + 'a');
          char[] charArray = new char[expected + 1];
          Arrays.fill(charArray, c);
          assertFixedLengthByteArrayReads(vector, expected + 1, new String(charArray).getBytes(), position);
          position += (expected + 1);
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testMultipleReads() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, "message test { required double double_field; required int32 int32_field; }");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    int expected = 0;
    try {
      RowBatch batch = new RowBatch();

      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          break;
        }

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
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testReadAll() throws Exception {
    conf.set(PARQUET_READ_SCHEMA, schema.toString());
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    //prepare the expected data
    StringBuilder expectedBinary = new StringBuilder();
    for (int i = 0; i < nElements; i++) {
      String b = String.valueOf((char) ((i % 26) + 'a'));
      expectedBinary.append(b);
    }

    int expected = 0;
    try {

      StringBuilder actual = new StringBuilder();
      RowBatch batch = new RowBatch();

      while(true) {
        batch = reader.nextBatch(batch);

        //EOF
        if (batch == null) {
          //assert binary_field contents
          String actualString = actual.toString();
          String expectedString = expectedBinary.toString();
          assertEquals(expectedString, actualString);
          break;
        }

        assertVectorTypes(batch, 10, ByteColumnVector.class, IntColumnVector.class, LongColumnVector.class,
                ByteColumnVector.class, DoubleColumnVector.class, FloatColumnVector.class, BooleanColumnVector.class,
                ByteColumnVector.class, ByteColumnVector.class, ByteColumnVector.class);

        ByteColumnVector binary_field = ByteColumnVector.class.cast(batch.getColumns()[0]);
        IntColumnVector int32_field = IntColumnVector.class.cast(batch.getColumns()[1]);
        LongColumnVector int64_field = LongColumnVector.class.cast(batch.getColumns()[2]);
        ByteColumnVector int96_field = ByteColumnVector.class.cast(batch.getColumns()[3]);
        DoubleColumnVector double_field = DoubleColumnVector.class.cast(batch.getColumns()[4]);
        FloatColumnVector float_field = FloatColumnVector.class.cast(batch.getColumns()[5]);
        BooleanColumnVector boolean_field = BooleanColumnVector.class.cast(batch.getColumns()[6]);
        ByteColumnVector flba_field = ByteColumnVector.class.cast(batch.getColumns()[7]);
        ByteColumnVector null_field = ByteColumnVector.class.cast(batch.getColumns()[8]);
        ByteColumnVector var_len_binary_field = ByteColumnVector.class.cast(batch.getColumns()[9]);

        //not all bytes are valid as the underlying array is reused across nextBatch() calls
        int validBytes = binary_field.size();
        String s = new String(Arrays.copyOfRange(binary_field.values, 0, validBytes));
        log(s);
        actual.append(s);

        int flba_field_position = 0;
        int null_field_position = 0;
        int var_len_binary_field_position = 0;
        int int96_field_position = 0;
        for (int i = 0 ; i < binary_field.size(); i++) {
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
            assertFixedLengthByteArrayReads(null_field, 4, "test".getBytes(), null_field_position);
            null_field_position += 4;
          } else {
            assertTrue(null_field.isNull[i]);
          }

          char c  = (char) ((expected % 26) + 'a');
          char[] charArray = new char[expected + 1];
          Arrays.fill(charArray, c);
          assertFixedLengthByteArrayReads(var_len_binary_field, expected + 1, new String(charArray).getBytes(), var_len_binary_field_position);
          var_len_binary_field_position += (expected + 1);

          expected++;
        }
      }

    } finally {
      if (reader != null)
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
