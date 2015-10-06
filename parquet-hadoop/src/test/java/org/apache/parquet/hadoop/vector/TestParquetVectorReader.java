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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.VectorizedParquetReader;
import org.apache.parquet.hadoop.VectorizedParquetRecordReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.vector.BooleanColumnVector;
import org.apache.parquet.io.vector.ByteColumnVector;
import org.apache.parquet.io.vector.DoubleColumnVector;
import org.apache.parquet.io.vector.FloatColumnVector;
import org.apache.parquet.io.vector.IntColumnVector;
import org.apache.parquet.io.vector.LongColumnVector;
import org.apache.parquet.io.vector.RowBatch;
import org.apache.parquet.io.vector.VectorizedReader;
import org.apache.parquet.schema.MessageType;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.hadoop.util.ContextUtil.newTaskAttemptContext;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.assertSingleColumnRead;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.assertVectorTypes;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public abstract class TestParquetVectorReader {
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
                                                + "required binary binary_field; "
                                                + "} ");

  protected enum ReaderType {
    NON_MR,
    MR
  }

  @Parameterized.Parameters
  public static Collection<Object[]> readerTypes() {
    Object[][] readerTypes = {
        { ReaderType.NON_MR},
        { ReaderType.MR } };
    return Arrays.asList(readerTypes);
  }

  private ReaderType readerType;

  public TestParquetVectorReader(ReaderType readerType) {
    this.readerType = readerType;
  }

  protected static void writeData(SimpleGroupFactory f, ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < nElements; i++) {
      Group group = f.newGroup()
              .append("int32_field", i)
              .append("int64_field", (long) 2 * i)
              .append("int96_field", Binary.fromReusedByteArray("999999999999".getBytes()))
              .append("double_field", i * 1.0)
              .append("float_field", ((float) (i * 2.0)))
              .append("boolean_field", i % 5 == 0)
              .append("flba_field", "foo");

      if (i % 2 == 1) {
        group.append("some_null_field", "x");
      }

      int binaryLen = i % 10;
      group.append("binary_field", Binary.fromString(new String(new char[binaryLen]).replace("\0", "x")));
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

  private VectorizedReader createParquetReader(String schemaString) throws IOException, InterruptedException {
    conf.set(PARQUET_READ_SCHEMA, schemaString);
    switch (readerType) {
      case NON_MR:
        return new VectorizedParquetReader(ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build());
      case MR:
        Job vectorJob = new Job(conf, "read vector");
        ParquetInputFormat.setInputPaths(vectorJob, file);
        ParquetInputFormat parquetInputFormat = new ParquetInputFormat(GroupReadSupport.class);
        InputSplit split = (InputSplit) parquetInputFormat.getSplits(vectorJob).get(0);
        TaskAttemptContext context = newTaskAttemptContext(vectorJob.getConfiguration(), new TaskAttemptID());
        ParquetRecordReader mrReader = (ParquetRecordReader) parquetInputFormat.createRecordReader(split, context);
        mrReader.initialize(split, context);
        return new VectorizedParquetRecordReader(mrReader);
      default:
        throw new IllegalArgumentException("Invalid reader type");
    }
  }

  @Test
  public void testInt32Read() throws Exception {
    VectorizedReader reader = createParquetReader("message test { required int32 int32_field;}");

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
    VectorizedReader reader = createParquetReader("message test { required int64 int64_field;}");

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
    VectorizedReader reader = createParquetReader("message test { required int96 int96_field;}");

    try {
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0; i < vector.size(); i++) {
          assertArrayEquals("999999999999".getBytes(), vector.values[i]);
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testDoubleRead() throws Exception {
    VectorizedReader reader = createParquetReader("message test { required double double_field;}");

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
    VectorizedReader reader = createParquetReader("message test { required float float_field;}");

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
    VectorizedReader reader = createParquetReader("message test { required boolean boolean_field;}");

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
    VectorizedReader reader = createParquetReader("message test { required fixed_len_byte_array(3) flba_field;}");

    try {
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0, position = 0; i < vector.size(); i++, position += 3) {
          assertArrayEquals("foo".getBytes(), vector.values[i]);
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testNullRead() throws Exception {
    VectorizedReader reader = createParquetReader("message test { optional fixed_len_byte_array(1) some_null_field;}");

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0; i < vector.size(); i++, expected++) {
          if (expected % 2 == 1) {
            assertArrayEquals("x".getBytes(), vector.values[i]);
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
    VectorizedReader reader = createParquetReader("message test { optional fixed_len_byte_array(1) all_null_field;}");

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
    VectorizedReader reader = createParquetReader("message test { required double double_field; required int32 int32_field; }");

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
  public void testBinaryRead() throws Exception {
    VectorizedReader reader = createParquetReader("message test { required binary binary_field;}");

    try {
      int total = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 1, ByteColumnVector.class);
        ByteColumnVector vector = ByteColumnVector.class.cast(batch.getColumns()[0]);
        for (int i = 0 ; i < vector.size(); i++) {
          int binaryLen = total % 10;
          byte[] expected = new String(new char[binaryLen]).replace("\0", "x").getBytes();
          assertArrayEquals(expected, vector.values[i]);
          total++;
        }
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testReadAll() throws Exception {
    VectorizedReader reader = createParquetReader(schema.toString());

    try {
      int expected = 0;
      for (RowBatch batch = reader.nextBatch(null); batch != null; batch = reader.nextBatch(batch)) {
        assertVectorTypes(batch, 10, IntColumnVector.class, LongColumnVector.class,
                ByteColumnVector.class, DoubleColumnVector.class, FloatColumnVector.class, BooleanColumnVector.class,
                ByteColumnVector.class, ByteColumnVector.class, ByteColumnVector.class, ByteColumnVector.class);

        IntColumnVector int32_field = IntColumnVector.class.cast(batch.getColumns()[0]);
        LongColumnVector int64_field = LongColumnVector.class.cast(batch.getColumns()[1]);
        ByteColumnVector int96_field = ByteColumnVector.class.cast(batch.getColumns()[2]);
        DoubleColumnVector double_field = DoubleColumnVector.class.cast(batch.getColumns()[3]);
        FloatColumnVector float_field = FloatColumnVector.class.cast(batch.getColumns()[4]);
        BooleanColumnVector boolean_field = BooleanColumnVector.class.cast(batch.getColumns()[5]);
        ByteColumnVector flba_field = ByteColumnVector.class.cast(batch.getColumns()[6]);
        ByteColumnVector some_null_field = ByteColumnVector.class.cast(batch.getColumns()[7]);
        ByteColumnVector all_null_field = ByteColumnVector.class.cast(batch.getColumns()[8]);

        for (int i = 0 ; i < int32_field.size(); i++) {
          assertSingleColumnRead(int32_field, int.class, i, expected);
          assertSingleColumnRead(int64_field, long.class, i, (long) (expected * 2));
          assertArrayEquals("999999999999".getBytes(), int96_field.values[i]);
          assertSingleColumnRead(double_field, double.class, i, expected * 1.0);
          assertSingleColumnRead(float_field, float.class, i, (float) (expected * 2.0));
          assertSingleColumnRead(boolean_field, boolean.class, i, expected % 5 == 0);
          assertArrayEquals("foo".getBytes(), flba_field.values[i]);
          if (expected % 2 == 1) {
            assertArrayEquals("x".getBytes(), some_null_field.values[i]);
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
}
