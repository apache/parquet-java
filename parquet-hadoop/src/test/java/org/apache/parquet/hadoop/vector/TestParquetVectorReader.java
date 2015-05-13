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
import org.apache.parquet.vector.*;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Arrays;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.*;

public abstract class TestParquetVectorReader
{
  protected static boolean DEBUG = false;
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
      System.out.println("Written record " + i);
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
          byteVector.values.flip();
          CharBuffer s = UTF_8.decode(byteVector.values);
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
        vector.values.flip();
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, int.class, expected);
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
        vector.values.flip();
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, long.class, expected * 2);
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
        vector.values.flip();
        for (int i = 0 ; i < vector.size(); i++) {
          assertFixedLengthByteArrayReads(vector, 12, "999999999999".getBytes());
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
        vector.values.flip();
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, double.class, expected * 1.0);
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
        vector.values.flip();
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, float.class, (float) (expected * 2.0));
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
        vector.values.flip();
        for (int i = 0 ; i < vector.size(); i++) {
          assertSingleColumnRead(vector, boolean.class, expected % 5 == 0);
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
          vector.values.flip();
          for (int i = 0; i < vector.size(); i++) {
            assertFixedLengthByteArrayReads(vector, 3, "foo".getBytes());
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
        vector.values.flip();
        for (int i = 0 ; i < vector.size(); i++, expected++) {
          if (expected % 2 == 1) {
            assertFixedLengthByteArrayReads(vector, 4, "test".getBytes());
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
        vector.values.flip();
        for (int i = 0 ; i < vector.size(); i++, expected++) {
          char c  = (char) ((expected % 26) + 'a');
          char[] charArray = new char[expected + 1];
          Arrays.fill(charArray, c);
          assertFixedLengthByteArrayReads(vector, expected + 1, new String(charArray).getBytes());
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

        doubleVector.values.flip();
        intVector.values.flip();
        for (int i = 0 ; i < doubleVector.size(); i++) {
          assertSingleColumnRead(doubleVector, double.class, expected * 1.0);
          assertSingleColumnRead(intVector, int.class, expected);
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

        binary_field.values.flip();
        int32_field.values.flip();
        int64_field.values.flip();
        int96_field.values.flip();
        double_field.values.flip();
        float_field.values.flip();
        boolean_field.values.flip();
        flba_field.values.flip();
        null_field.values.flip();
        var_len_binary_field.values.flip();

        CharBuffer s = UTF_8.decode(binary_field.values);
        actual.append(s);

        for (int i = 0 ; i < binary_field.size(); i++) {
          assertSingleColumnRead(int32_field, int.class, expected);
          assertSingleColumnRead(int64_field, long.class, (long) (expected * 2));
          assertFixedLengthByteArrayReads(int96_field, 12, "999999999999".getBytes());
          assertSingleColumnRead(double_field, double.class, expected * 1.0);
          assertSingleColumnRead(float_field, float.class, (float) (expected * 2.0));
          assertSingleColumnRead(boolean_field, boolean.class, expected % 5 == 0);
          assertFixedLengthByteArrayReads(flba_field, 3, "foo".getBytes());

          if (expected % 2 == 1) {
            assertFixedLengthByteArrayReads(null_field, 4, "test".getBytes());
          } else {
            assertTrue(null_field.isNull[i]);
          }

          char c  = (char) ((expected % 26) + 'a');
          char[] charArray = new char[expected + 1];
          Arrays.fill(charArray, c);
          assertFixedLengthByteArrayReads(var_len_binary_field, expected + 1, new String(charArray).getBytes());

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
    ColumnVector vector = new ByteColumnVector(4);
    for (int i = 0 ; i < 100; i++) {
      vector.values.put("test".getBytes());
    }
    vector.ensureCapacity(600000);
    vector.values.flip();
    for (int i = 0 ; i < 100; i++) {
      assertFixedLengthByteArrayReads(vector, 4, "test".getBytes());
    }
  }

  private <T> void assertVectorTypes(RowBatch batch, int expectedColumnCount, Class... vectorType) {
    assertTrue("Must have a single column", batch.getColumns().length == expectedColumnCount);
    for (int i = 0 ; i < expectedColumnCount ; i++) {
      ColumnVector vector = batch.getColumns()[i];
      assertTrue(vectorType[i].isInstance(vector));
      log("Read " + vector.size() + " elements of type " + vectorType[i]);
    }
  }

  private <T> void assertSingleColumnRead(ColumnVector vector, Class<T> elementType, T expectedValue) {
    if (elementType == int.class) {
      int read = vector.values.getInt();
      log(read);
      assertEquals(expectedValue, read);
    } else if (elementType == long.class) {
      long read = vector.values.getLong();
      log(read);
      assertEquals(expectedValue, read);
    } else if (elementType == double.class) {
      double read = vector.values.getDouble();
      log(read);
      assertEquals(Double.class.cast(expectedValue), read, 0.01);
    } else if (elementType == float.class) {
      float read = vector.values.getFloat();
      log(read);
      assertEquals(Float.class.cast(expectedValue), read, 0.01);
    } else if (elementType == boolean.class) {
      boolean read = vector.values.get() == 0 ? FALSE : TRUE;
      log(read);
      assertEquals(expectedValue, read);
    }
  }

  private <T> void assertFixedLengthByteArrayReads(ColumnVector vector, int fixedLength, byte[] expectedValue) {
    byte[] value = new byte[fixedLength];
    vector.values.get(value);
    String read = new String(value);
    log(read);
    assertArrayEquals(expectedValue, value);
  }

  private void log(Object message) {
    if (DEBUG) {
      System.out.println(message);
    }
  }
}
