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

package org.apache.parquet.tools.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class TestSimpleRecordConverter {

  private static final String INT32_FIELD = "int32_field";
  private static final String INT64_FIELD = "int64_field";
  private static final String FLOAT_FIELD = "float_field";
  private static final String DOUBLE_FIELD = "double_field";
  private static final String BINARY_FIELD = "binary_field";
  private static final String FIXED_LEN_BYTE_ARRAY_FIELD = "flba_field";


  private File testFile;

  @Test
  public void testConverter() throws IOException {
    ParquetReader<SimpleRecord> reader =
      ParquetReader.builder(new SimpleReadSupport(), new Path(this.testFile.getAbsolutePath())).build();
    for (SimpleRecord record = reader.read(); record != null; record = reader.read()) {
      for (SimpleRecord.NameValue value : record.getValues()) {
        switch(value.getName()) {
          case INT32_FIELD:
            Assert.assertEquals(32, value.getValue());
            break;
          case INT64_FIELD:
            Assert.assertEquals(64L, value.getValue());
            break;
          case FLOAT_FIELD:
            Assert.assertEquals(1.0f, value.getValue());
            break;
          case DOUBLE_FIELD:
            Assert.assertEquals(2.0d, value.getValue());
            break;
          case BINARY_FIELD:
            Assert.assertArrayEquals("foobar".getBytes(), (byte[])value.getValue());
            break;
          case FIXED_LEN_BYTE_ARRAY_FIELD:
            Assert.assertArrayEquals(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, (byte[])value.getValue());
            break;
        }
      }
    }
  }

  @Before
  public void setUp() throws IOException {
    this.testFile = createTempFile();
    MessageType schema = createSchema();
    write(schema, testFile);
  }

  @After
  public void tearDown() {
    this.testFile.delete();
  }

  private MessageType createSchema() {
    return new MessageType("schema",
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, INT32_FIELD),
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, INT64_FIELD),
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, FLOAT_FIELD),
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, DOUBLE_FIELD),
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, BINARY_FIELD),
      new PrimitiveType(Type.Repetition.REQUIRED,
        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 12, FIXED_LEN_BYTE_ARRAY_FIELD)
    );
  }

  private void write(MessageType schema, File f) throws IOException {
    Path fsPpath = new Path(f.getPath());
    Configuration conf = new Configuration();
    SimpleGroupFactory fact = new SimpleGroupFactory(schema);
    GroupWriteSupport.setSchema(schema, conf);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(
      fsPpath,
      new GroupWriteSupport(),
      CompressionCodecName.UNCOMPRESSED,
      1024,
      1024,
      512,
      true,
      false,
      ParquetProperties.WriterVersion.PARQUET_2_0,
      conf);
    try {
      writer.write(fact.newGroup()
       .append(INT32_FIELD, 32)
       .append(INT64_FIELD, 64L)
       .append(FLOAT_FIELD, 1.0f)
       .append(DOUBLE_FIELD, 2.0d)
       .append(BINARY_FIELD, Binary.fromString("foobar"))
       .append(FIXED_LEN_BYTE_ARRAY_FIELD,
         Binary.fromConstantByteArray(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 })));
    } finally {
      writer.close();
    }
  }

  private File createTempFile() throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    return tmp;
  }
}
