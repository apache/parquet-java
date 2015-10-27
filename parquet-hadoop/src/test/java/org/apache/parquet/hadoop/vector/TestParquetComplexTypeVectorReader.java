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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.vector.ObjectColumnVector;
import org.apache.parquet.vector.RowBatch;
import org.junit.Test;

import java.net.URISyntaxException;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.hadoop.vector.ParquetVectorTestUtils.assertVectorTypes;
import static org.junit.Assert.assertEquals;

public class TestParquetComplexTypeVectorReader
{
  protected static final Configuration conf = new Configuration();

  private Path getTestListFile() {
    try {
      return new Path(TestParquetComplexTypeVectorReader.class.getClassLoader().getResource("int_list.parquet").toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private Path getTestStructFile() {
    try {
      return new Path(TestParquetComplexTypeVectorReader.class.getClassLoader().getResource("struct.parquet").toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private Path getTestMapFile() {
    try {
      return new Path(TestParquetComplexTypeVectorReader.class.getClassLoader().getResource("map_string_string.parquet").toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testVectorListRead()  throws Exception {

    /**
     * The test file schema is
       message org.kitesdk.examples.data.test_record {
         required group list (LIST) {
          repeated int32 array;
         }
         required int32 integer;
       }
     */

    conf.set(PARQUET_READ_SCHEMA, "message org.kitesdk.examples.data.test_record {\n" +
            "  required group list (LIST) {\n" +
            "    repeated int32 array;\n" +
            "  }\n" +
            "}");

    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), getTestListFile()).withConf(conf).build();

    int index = 0;
    int totalRowsRead = 0;
    try {
      RowBatch batch = new RowBatch();
      while(true) {
        batch = reader.nextBatch(batch, Group.class);

        if (batch == null) {
          //EOF
          //the test file has 1500 rows in total
          assertEquals(1500, totalRowsRead);
          break;
        } else {
          assertVectorTypes(batch, 1, ObjectColumnVector.class);
          ObjectColumnVector<Group> objectColumnVector = ObjectColumnVector.class.cast(batch.getColumns()[0]);
          totalRowsRead += objectColumnVector.size();
          for (int i = 0 ; i < objectColumnVector.size(); i++) {
            Group group = objectColumnVector.values[i];
            SimpleGroup simpleGroup = (SimpleGroup) group.getGroup(0, 0);
            int x = simpleGroup.getInteger(0, 0);
            int y = simpleGroup.getInteger(0, 1);
            assertEquals(index, x);
            assertEquals(index * index, y);
            index++;
          }
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testVectorStructRead()  throws Exception {

    /**
     * The test file schema is
     message org.kitesdk.examples.data.test_record {
       required group struct {
         optional binary f1 (UTF8);
         optional boolean f2;
         optional int32 f3;
         optional double f4;
         optional int64 f5;
       }
       optional int64 other_field;
     }
     */

    conf.set(PARQUET_READ_SCHEMA, "message org.kitesdk.examples.data.test_record {\n" +
            "         required group struct {\n" +
            "           optional binary f1 (UTF8);\n" +
            "           optional boolean f2;\n" +
            "           optional int32 f3;\n" +
            "           optional double f4;\n" +
            "           optional int64 f5;\n" +
            "         }\n}");

    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), getTestStructFile()).withConf(conf).build();

    int index = 0;
    int totalRowsRead = 0;
    try {
      RowBatch batch = new RowBatch();
      while(true) {
        batch = reader.nextBatch(batch, Group.class);

        if (batch == null) {
          //EOF
          //the test file has 1500 rows in total
          assertEquals(1500, totalRowsRead);
          break;
        } else {
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
            assertEquals(index % 2 == 0 ? Boolean.TRUE : Boolean.FALSE, f2);
            assertEquals(index, f3);
            assertEquals(index * 1.0, f4, 0.01);
            assertEquals(index, f5);
            index++;
          }
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  @Test
  public void testVectorMapRead()  throws Exception {

    /**
     * The test file schema is
     *
     message org.kitesdk.examples.data.test_record {
       required group map (MAP) {
         repeated group map (MAP_KEY_VALUE) {
           required binary key (UTF8);
           required binary value (UTF8);
         }
       }
      required int32 integer;
     }
     */
    conf.set(PARQUET_READ_SCHEMA, "message org.kitesdk.examples.data.test_record {\n" +
            "  required group map (MAP) {\n" +
            "    repeated group map (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "      required binary value (UTF8);\n" +
            "    }\n" +
            "  }\n" +
            "}");

    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), getTestMapFile()).withConf(conf).build();

    int totalRowsRead = 0;
    try {
      RowBatch batch = new RowBatch();
      while(true) {
        batch = reader.nextBatch(batch, Group.class);

        if (batch == null) {
          //EOF
          //the test file has 1500 rows in total
          assertEquals(1500, totalRowsRead);
          break;
        } else {
          assertVectorTypes(batch, 1, ObjectColumnVector.class);
          ObjectColumnVector<Group> objectColumnVector = ObjectColumnVector.class.cast(batch.getColumns()[0]);
          totalRowsRead += objectColumnVector.size();
          for (int i = 0 ; i < objectColumnVector.size(); i++) {
            Group group = objectColumnVector.values[i];
            SimpleGroup simpleGroup = (SimpleGroup) group.getGroup(0, 0);
            //the map should have 10 elements (1,1), (2,2), ... , (10,10)
            for (int j = 0 ; j < 10; j++) {
              SimpleGroup map = (SimpleGroup) simpleGroup.getGroup(0, j);
              String key = map.getString(0, 0);
              String value = map.getString(1, 0);
              assertEquals(String.valueOf(j+1), key);
              assertEquals(String.valueOf(j+2), value);
            }
          }
        }
      }
    } finally {
      if (reader != null)
        reader.close();
    }
  }
}
