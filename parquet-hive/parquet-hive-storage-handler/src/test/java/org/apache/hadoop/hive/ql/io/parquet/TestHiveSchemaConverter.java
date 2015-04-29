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
package org.apache.hadoop.hive.ql.io.parquet;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.Test;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type.Repetition;

public class TestHiveSchemaConverter {

  private List<String> createHiveColumnsFrom(final String columnNamesStr) {
    List<String> columnNames;
    if (columnNamesStr.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNamesStr.split(","));
    }

    return columnNames;
  }

  private List<TypeInfo> createHiveTypeInfoFrom(final String columnsTypeStr) {
    List<TypeInfo> columnTypes;

    if (columnsTypeStr.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnsTypeStr);
    }

    return columnTypes;
  }

  private void testConversion(final String columnNamesStr, final String columnsTypeStr, final String expectedSchema) throws Exception {
    final List<String> columnNames = createHiveColumnsFrom(columnNamesStr);
    final List<TypeInfo> columnTypes = createHiveTypeInfoFrom(columnsTypeStr);
    final MessageType messageTypeFound = HiveSchemaConverter.convert(columnNames, columnTypes);
    final MessageType expectedMT = MessageTypeParser.parseMessageType(expectedSchema);
    assertEquals("converting " + columnNamesStr + ": " + columnsTypeStr + " to " + expectedSchema, expectedMT, messageTypeFound);
  }

  @Test
  public void testSimpleType() throws Exception {
    testConversion(
            "a,b,c",
            "int,double,boolean",
            "message hive_schema {\n"
            + "  optional int32 a;\n"
            + "  optional double b;\n"
            + "  optional boolean c;\n"
            + "}\n");
  }

  @Test
  public void testArray() throws Exception {
    testConversion("arrayCol",
            "array<int>",
            "message hive_schema {\n"
            + "  optional group arrayCol (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional int32 array_element;\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testStruct() throws Exception {
    testConversion("structCol",
            "struct<a:int,b:double,c:boolean>",
            "message hive_schema {\n"
            + "  optional group structCol {\n"
            + "    optional int32 a;\n"
            + "    optional double b;\n"
            + "    optional boolean c;\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testMap() throws Exception {
    testConversion("mapCol",
            "map<string,string>",
            "message hive_schema {\n"
            + "  optional group mapCol (MAP) {\n"
            + "    repeated group map (MAP_KEY_VALUE) {\n"
            + "      required binary key;\n"
            + "      optional binary value;\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testMapOriginalType() throws Exception {
    final String hiveColumnTypes = "map<string,string>";
    final String hiveColumnNames = "mapCol";
    final List<String> columnNames = createHiveColumnsFrom(hiveColumnNames);
    final List<TypeInfo> columnTypes = createHiveTypeInfoFrom(hiveColumnTypes);
    final MessageType messageTypeFound = HiveSchemaConverter.convert(columnNames, columnTypes);
    // this messageType only has one optional field, whose name is mapCol, original Type is MAP
    assertEquals(1, messageTypeFound.getFieldCount());
    org.apache.parquet.schema.Type topLevel = messageTypeFound.getFields().get(0);
    assertEquals("mapCol",topLevel.getName());
    assertEquals(OriginalType.MAP, topLevel.getOriginalType());
    assertEquals(Repetition.OPTIONAL, topLevel.getRepetition());

    assertEquals(1, topLevel.asGroupType().getFieldCount());
    org.apache.parquet.schema.Type secondLevel = topLevel.asGroupType().getFields().get(0);
    //there is one repeated field for mapCol, the field name is "map" and its original Type is MAP_KEY_VALUE;
    assertEquals("map", secondLevel.getName());
    assertEquals(OriginalType.MAP_KEY_VALUE, secondLevel.getOriginalType());
    assertEquals(Repetition.REPEATED, secondLevel.getRepetition());
  }
}
