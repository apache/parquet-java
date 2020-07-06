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
package org.apache.parquet.pig;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.junit.Assert.assertEquals;
import static org.apache.parquet.pig.PigSchemaConverter.pigSchemaToString;
import static org.apache.parquet.pig.TupleReadSupport.getPigSchemaFromMultipleFiles;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Types;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;


public class TestPigSchemaConverter {

  private final PigSchemaConverter pigSchemaConverter = new PigSchemaConverter();

  private void testPigConversion(String pigSchemaString) throws Exception {
    Schema pigSchema = Utils.getSchemaFromString(pigSchemaString);
    MessageType parquetSchema = pigSchemaConverter.convert(pigSchema);
    Schema convertedSchema = pigSchemaConverter.convert(parquetSchema);
    assertEquals(pigSchema, convertedSchema);
  }

  @Test
  public void testSimpleBag() throws Exception {
    testPigConversion("b:{t:(a:int)}");
  }

  @Test
  public void testMultiBag() throws Exception {
    testPigConversion("x:int, b:{t:(a:int,b:chararray)}");
  }

  @Test
  public void testMapSimple() throws Exception {
    testPigConversion("b:[(c:int)]");
  }

  @Test
  public void testMapTuple() throws Exception {
    testPigConversion("a:chararray, b:[(c:chararray, d:chararray)]");
  }

  @Test
  public void testMapOfList() throws Exception {
    testPigConversion("a:map[{bag: (a:int)}]");
  }

  @Test
  public void testListsOfPrimitive() throws Exception {
    for (Type.Repetition repetition : Type.Repetition.values()) {
      for (Type.Repetition valueRepetition : Type.Repetition.values()) {
        for (PrimitiveType.PrimitiveTypeName primitiveTypeName : PrimitiveType.PrimitiveTypeName.values()) {
          if (primitiveTypeName != PrimitiveType.PrimitiveTypeName.INT96) { // INT96 is NYI
            Types.PrimitiveBuilder<PrimitiveType> value = Types.primitive(primitiveTypeName, valueRepetition);
            if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
              value.length(1);
            GroupType type = Types.buildGroup(repetition).addField(value.named("b")).as(OriginalType.LIST).named("a");
            pigSchemaConverter.convertField(type); // no exceptions, please
          }
        }
      }
    }
  }

  private void testConversion(String pigSchemaString, String schemaString) throws Exception {
    Schema pigSchema = Utils.getSchemaFromString(pigSchemaString);
    MessageType schema = pigSchemaConverter.convert(pigSchema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals("converting "+pigSchemaString+" to "+schemaString, expectedMT, schema);

    MessageType filtered = pigSchemaConverter.filter(schema, pigSchema, null);
    assertEquals("converting "+pigSchemaString+" to "+schemaString+" and filtering", schema.toString(), filtered.toString());
  }

  @Test
  public void testTupleBag() throws Exception {
    testConversion(
        "a:chararray, b:{t:(c:chararray, d:chararray)}",
        "message pig_schema {\n" +
        "  optional binary a (UTF8);\n" +
        "  optional group b (LIST) {\n" +
        "    repeated group t {\n" +
        "      optional binary c (UTF8);\n" +
        "      optional binary d (UTF8);\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testTupleBagWithAnonymousInnerField() throws Exception {
    testConversion(
        "a:chararray, b:{(c:chararray, d:chararray)}",
        "message pig_schema {\n" +
        "  optional binary a (UTF8);\n" +
        "  optional group b (LIST) {\n" +
        "    repeated group bag {\n" + // the inner field in the bag is called "bag"
        "      optional binary c (UTF8);\n" +
        "      optional binary d (UTF8);\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testMap() throws Exception {
    testConversion(
        "a:chararray, b:[(c:chararray, d:chararray)]",
        "message pig_schema {\n" +
        "  optional binary a (UTF8);\n" +
        "  optional group b (MAP) {\n" +
        "    repeated group key_value (MAP_KEY_VALUE) {\n" +
        "      required binary key (UTF8);\n" +
        "      optional group value {\n" +
        "        optional binary c (UTF8);\n" +
        "        optional binary d (UTF8);\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testMap2() throws Exception {
    testConversion("a:map[int]",
        "message pig_schema {\n" +
        "  optional group a (MAP) {\n" +
        "    repeated group key_value (MAP_KEY_VALUE) {\n" +
        "      required binary key (UTF8);\n" +
        "      optional int32 value;" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testMap3() throws Exception {
    testConversion("a:map[map[int]]",
        "message pig_schema {\n" +
        "  optional group a (MAP) {\n" +
        "    repeated group key_value (MAP_KEY_VALUE) {\n" +
        "      required binary key (UTF8);\n" +
        "      optional group value (MAP) {\n" +
        "        repeated group key_value (MAP_KEY_VALUE) {\n" +
        "          required binary key (UTF8);\n" +
        "          optional int32 value;\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testMap4() throws Exception {
    testConversion("a:map[bag{(a:int)}]",
        "message pig_schema {\n" +
        "  optional group a (MAP) {\n" +
        "    repeated group key_value (MAP_KEY_VALUE) {\n" +
        "      required binary key (UTF8);\n" +
        "      optional group value (LIST) {\n" +
        "        repeated group bag {\n" +
        "          optional int32 a;\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testListOfPrimitiveIsABag() throws Exception {
    testFixedConversion(
        "message pig_schema {\n" +
        "  optional group a (LIST) {\n" +
        "    repeated binary b (UTF8);\n" +
        "  }\n" +
        "}\n",
        "a:{" + PigSchemaConverter.ARRAY_VALUE_NAME + ":(b: chararray)}");
  }

  private void testFixedConversion(String schemaString, String pigSchemaString)
      throws Exception {
    Schema expectedPigSchema = Utils.getSchemaFromString(pigSchemaString);
    MessageType parquetSchema = MessageTypeParser.parseMessageType(schemaString);
    Schema pigSchema = pigSchemaConverter.convert(parquetSchema);
    assertEquals("converting " + schemaString + " to " + pigSchemaString,
                 expectedPigSchema, pigSchema);
  }

  @Test
  public void testMapWithFixed() throws Exception {
    testFixedConversion(
        "message pig_schema {\n" +
        "  optional binary a;\n" +
        "  optional group b (MAP) {\n" +
        "    repeated group key_value (MAP_KEY_VALUE) {\n" +
        "      required binary key;\n" +
        "      optional group value {\n" +
        "        optional fixed_len_byte_array(5) c;\n" +
        "        optional fixed_len_byte_array(7) d;\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n",
        "a:bytearray, b:[(c:bytearray, d:bytearray)]");
  }

  @Test
  public void testMapWithFixedWithoutOriginalType() throws Exception {
    testFixedConversion(
      "message spark_schema {\n" +
      "  optional binary a;\n" +
      "  optional group b (MAP) {\n" +
      "    repeated group key_value {\n" +
      "      required binary key;\n" +
      "      optional group value {\n" +
      "        optional fixed_len_byte_array(5) c;\n" +
      "        optional fixed_len_byte_array(7) d;\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}\n",
      "a:bytearray, b:[(c:bytearray, d:bytearray)]");
  }

  @Test
  public void testInt96() throws Exception {
    testFixedConversion(
      "message spark_schema {\n" +
        "  optional int96 datetime;\n" +
        "}",
      "datetime:bytearray"
    );
  }

  @Test
  public void testAnonymousField() throws Exception {
    testConversion(
        "a:chararray, int",
        "message pig_schema {\n" +
        "  optional binary a (UTF8);\n" +
        "  optional int32 val_0;\n" +
        "}\n");
  }

  @Test
  public void testSchemaEvolution() {
    Map<String, Set<String>> map = new LinkedHashMap<String, Set<String>>();
    map.put("pig.schema", new LinkedHashSet<String>(Arrays.asList(
        "a:int, b:int, c:int, d:int, e:int, f:int",
        "aa:int, aaa:int, b:int, c:int, ee:int")));
    Schema result = getPigSchemaFromMultipleFiles(new MessageType("file_schema", new PrimitiveType(OPTIONAL, INT32,"a")), map);
    assertEquals("a: int,b: int,c: int,d: int,e: int,f: int,aa: int,aaa: int,ee: int", pigSchemaToString(result));
  }

}
