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
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.vector.ObjectColumnVector;
import org.apache.parquet.io.vector.RowBatch;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.assertVectorTypes;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

public class TestParquetComplexTypeVectorReader {
  private static final Configuration conf = new Configuration();
  private static final int N_ELEMENTS = 1500;

  private static final Path listFile = new Path("target/test/TestParquetVectorReader/testParquetList");
  private static final Path structFile = new Path("target/test/TestParquetVectorReader/testParquetStruct");
  private static final Path mapFile = new Path("target/test/TestParquetVectorReader/testParquetMap");

  private static final MessageType listSchema = new MessageType("list",
          new GroupType(REPEATED, "list",
                  new PrimitiveType(REQUIRED, INT32, "element"))
          );

  private static final MessageType structSchema = new MessageType("struct",
          new GroupType(REPEATED, "struct",
                  new PrimitiveType(REQUIRED, BINARY, "f1"),
                  new PrimitiveType(REQUIRED, BOOLEAN, "f2"),
                  new PrimitiveType(REQUIRED, INT32, "f3"),
                  new PrimitiveType(REQUIRED, DOUBLE, "f4"),
                  new PrimitiveType(REQUIRED, INT64, "f5"))
          );

  private static final MessageType mapSchema = new MessageType("map",
          new GroupType(REPEATED, "map",
                  new GroupType(REPEATED, "map_kv",
                          new PrimitiveType(REQUIRED, INT32, "key"),
                          new PrimitiveType(REQUIRED, INT32, "value")))
          );

  @BeforeClass
  public static void createTestFiles() throws IOException {
    cleanup();
    ParquetWriter listWriter = createWriter(listSchema, listFile);
    writeList(listWriter);
    ParquetWriter structWriter = createWriter(structSchema, structFile);
    writeStruct(structWriter);
    ParquetWriter mapWriter = createWriter(mapSchema, mapFile);
    writeMap(mapWriter);
  }

  private static ParquetWriter createWriter(MessageType schema, Path file) throws IOException {
    boolean dictionaryEnabled = true;
    boolean validating = false;
    GroupWriteSupport.setSchema(schema, conf);
    return new ParquetWriter<Group>(
            file,
            new GroupWriteSupport(),
            GZIP, 1024*1024, 1024, 1024*1024,
            dictionaryEnabled, validating, PARQUET_1_0, conf);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    delete(listFile);
    delete(structFile);
    delete(mapFile);
  }

  private static void delete(Path p) throws IOException {
    FileSystem fs = p.getFileSystem(conf);
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
  }

  private static void writeList(ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < N_ELEMENTS; i++) {
      SimpleGroup list = new SimpleGroup(listSchema);
      Group name = list.addGroup("list");
      name.append("element", i);
      writer.write(list);
    }
    writer.close();
  }

  private static void writeStruct(ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < N_ELEMENTS; i++) {
      SimpleGroup struct = new SimpleGroup(structSchema);
      Group name = struct.addGroup("struct");
      name.append("f1", Binary.fromConstantByteArray(String.valueOf(i).getBytes()));
      name.append("f2", i % 2 == 0);
      name.append("f3", i);
      name.append("f4", i * 1.0);
      name.append("f5", Long.valueOf(i));
      writer.write(struct);
    }
    writer.close();
  }

  private static void writeMap(ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < N_ELEMENTS; i++) {
      SimpleGroup map = new SimpleGroup(mapSchema);
      for (int j = 0; j < 10; j++) {
        Group name = map.addGroup("map");
        name.addGroup("map_kv")
                .append("key", j)
                .append("value", j);
      }
      writer.write(map);
    }
    writer.close();
  }

  private ParquetReader createParquetReader(Path filePath, String schemaString) throws IOException {
    conf.set(PARQUET_READ_SCHEMA, schemaString);
    return ParquetReader.builder(new GroupReadSupport(), filePath).withConf(conf).build();
  }

  @Test
  public void testVectorListRead()  throws Exception {
    ParquetReader reader = createParquetReader(listFile, listSchema.toString());
    try {
      int index = 0;
      int totalRowsRead = 0;
      for (RowBatch batch = reader.nextBatch(null, Group.class); batch != null; batch = reader.nextBatch(batch, Group.class)) {
        assertVectorTypes(batch, 1, ObjectColumnVector.class);
        ObjectColumnVector<Group> objectColumnVector = ObjectColumnVector.class.cast(batch.getColumns()[0]);
        totalRowsRead += objectColumnVector.size();
        for (int i = 0; i < objectColumnVector.size(); i++) {
          Group group = objectColumnVector.values[i];
          SimpleGroup simpleGroup = (SimpleGroup) group.getGroup(0, 0);
          int element = simpleGroup.getInteger(0, 0);
          assertEquals(index, element);
          index++;
        }
      }
      assertEquals(1500, totalRowsRead);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testVectorStructRead()  throws Exception {
    ParquetReader reader = createParquetReader(structFile, structSchema.toString());
    try {
      int index = 0;
      int totalRowsRead = 0;
      for (RowBatch batch = reader.nextBatch(null, Group.class); batch != null; batch = reader.nextBatch(batch, Group.class)) {
        assertVectorTypes(batch, 1, ObjectColumnVector.class);
        ObjectColumnVector<Group> objectColumnVector = ObjectColumnVector.class.cast(batch.getColumns()[0]);
        totalRowsRead += objectColumnVector.size();
        for (int i = 0 ; i < objectColumnVector.size(); i++) {
          Group group = objectColumnVector.values[i];
          SimpleGroup simpleGroup = (SimpleGroup) group.getGroup(0, 0);
          String f1 = simpleGroup.getString(0, 0);
          boolean f2 = simpleGroup.getBoolean(1, 0);
          int f3 = simpleGroup.getInteger(2, 0);
          double f4 = simpleGroup.getDouble(3, 0);
          long f5 = simpleGroup.getLong(4, 0);
          assertEquals(index, Integer.parseInt(f1));
          assertEquals(index % 2 == 0 ? Boolean.TRUE : Boolean.FALSE, f2);
          assertEquals(index, f3);
          assertEquals(index * 1.0, f4, 0.01);
          assertEquals(index, f5);
          index++;
        }
      }
      assertEquals(1500, totalRowsRead);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testVectorMapRead()  throws Exception {
    ParquetReader reader = createParquetReader(mapFile, mapSchema.toString());
    try {
      int totalRowsRead = 0;
      for (RowBatch batch = reader.nextBatch(null, Group.class); batch != null; batch = reader.nextBatch(batch, Group.class)) {
        assertVectorTypes(batch, 1, ObjectColumnVector.class);
        ObjectColumnVector<Group> objectColumnVector = ObjectColumnVector.class.cast(batch.getColumns()[0]);
        totalRowsRead += objectColumnVector.size();
        for (int i = 0 ; i < objectColumnVector.size(); i++) {
          Group group = objectColumnVector.values[i];
          for (int j = 0; j < 10; j++) {
            SimpleGroup simpleGroup = (SimpleGroup) group.getGroup(0, j);
            int key = simpleGroup.getGroup(0,0).getInteger("key",0);
            int value = simpleGroup.getGroup(0,0).getInteger("value", 0);
            assertEquals(j, key);
            assertEquals(j, value);
          }
        }
      }
      assertEquals(1500, totalRowsRead);
    } finally {
      reader.close();
    }
  }
}
