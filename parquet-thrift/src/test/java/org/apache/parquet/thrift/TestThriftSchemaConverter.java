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
package org.apache.parquet.thrift;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.thrift.projection.StrictFieldProjectionFilter;
import org.apache.parquet.thrift.projection.ThriftProjectionException;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.test.compat.MapStructV2;
import org.apache.parquet.thrift.test.compat.SetStructV2;
import org.apache.parquet.thrift.test.TestLogicalType;
import org.apache.thrift.TBase;
import org.junit.Test;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.elephantbird.thrift.test.TestStructInMap;

import java.util.Arrays;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.thrift.struct.ThriftField.Requirement.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestThriftSchemaConverter {

  @Test
  public void testToMessageType() throws Exception {
    String expected =
            "message ParquetSchema {\n" +
                    "  optional group persons (LIST) = 1 {\n" +
                    "    repeated group persons_tuple {\n" +
                    "      required group name = 1 {\n" +
                    "        optional binary first_name (UTF8) = 1;\n" +
                    "        optional binary last_name (UTF8) = 2;\n" +
                    "      }\n" +
                    "      optional int32 id = 2;\n" +
                    "      optional binary email (UTF8) = 3;\n" +
                    "      optional group phones (LIST) = 4 {\n" +
                    "        repeated group phones_tuple {\n" +
                    "          optional binary number (UTF8) = 1;\n" +
                    "          optional binary type (ENUM) = 2;\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
    ThriftSchemaConverter schemaConverter = new ThriftSchemaConverter();
    final MessageType converted = schemaConverter.convert(AddressBook.class);
    assertEquals(MessageTypeParser.parseMessageType(expected), converted);
  }

  @Test
  public void testToProjectedThriftType() {

    shouldGetProjectedSchema("name/first_name", "name.first_name", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "  }}", Person.class);

    shouldGetProjectedSchema("name/first_name;name/last_name", "name.first_name;name.last_name" ,"message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }}", Person.class);

    shouldGetProjectedSchema("name/{first,last}_name;", "name.{first,last}_name;", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }}", Person.class);

    shouldGetProjectedSchema("name/*", "name" ,"message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }" +
            "}", Person.class);

    shouldGetProjectedSchema("*/*_name", "*.*_name" ,"message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }" +
            "}", Person.class);

    shouldGetProjectedSchema("name/first_*", "name.first_*","message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "  }" +
            "}", Person.class);

    shouldGetProjectedSchema("*/*", "*.*", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "  optional binary first_name (UTF8) = 1;" +
            "  optional binary last_name (UTF8) = 2;" +
            "} " +
            "  optional group phones (LIST) = 4 {" +
            "    repeated group phones_tuple {" +
            "      optional binary number (UTF8) = 1;" +
            "      optional binary type (ENUM) = 2;" +
            "    }" +
            "}}", Person.class);
  }

  /* Original message type, before projection
 message TestStructInMap {
   optional binary name(UTF8);
   optional group names(MAP) {
     repeated group map(MAP_KEY_VALUE) {
       required binary key(UTF8);
       optional group value {
         optional group name {
           optional binary first_name(UTF8);
           optional binary last_name(UTF8);
         }
         optional group phones(MAP) {
           repeated group map(MAP_KEY_VALUE) {
             required binary key(ENUM);
             optional binary value(UTF8);
           }
         }
       }
     }
   }
 }
 */
  @Test
  public void testProjectMapThriftType() {
    //project nested map
    shouldGetProjectedSchema("name;names/key*;names/value/**", "name;names.key*;names.value", "message ParquetSchema {\n" +
            "  optional binary name (UTF8) = 1;\n" +
            "  optional group names (MAP) = 2 {\n" +
            "    repeated group key_value (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "      optional group value {\n" +
            "        optional group name = 1 {\n" +
            "          optional binary first_name (UTF8) = 1;\n" +
            "          optional binary last_name (UTF8) = 2;\n" +
            "        }\n" +
            "        optional group phones (MAP) = 2 {\n" +
            "          repeated group key_value (MAP_KEY_VALUE) {\n" +
            "            required binary key (ENUM);\n" +
            "            optional binary value (UTF8);\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}", TestStructInMap.class);

    //project only one level of nested map
    shouldGetProjectedSchema("name;names/key;names/value/name/*", "name;names.key;names.value.name","message ParquetSchema {\n" +
            "  optional binary name (UTF8) = 1;\n" +
            "  optional group names (MAP) = 2 {\n" +
            "    repeated group key_value (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "      optional group value {\n" +
            "        optional group name = 1 {\n" +
            "          optional binary first_name (UTF8) = 1;\n" +
            "          optional binary last_name (UTF8) = 2;\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}", TestStructInMap.class);
  }

  @Test
  public void testProjectOnlyKeyInMap() {
    shouldGetProjectedSchema("name;names/key", "name;names.key", "message ParquetSchema {\n" +
            "  optional binary name (UTF8) = 1;\n" +
            "  optional group names (MAP) = 2 {\n" +
            "    repeated group key_value (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "      optional group value {\n" +
            "        optional group name = 1 {\n" +
            "          optional binary first_name (UTF8) = 1;\n" +
            "        }\n" +
            "      }" +
            "    }\n" +
            "  }\n" +
            "}",TestStructInMap.class);
  }

  @Test
  public void testProjectOnlyValueInMap() {

    try {
      getStrictFilteredSchema("name;names.value", TestStructInMap.class);
      fail("this should throw");
    } catch (ThriftProjectionException e) {
      assertEquals("Cannot select only the values of a map, you must keep the keys as well: names", e.getMessage());
    }

  }

  private void doTestPartialKeyProjection(String deprecated, String strict) {

    try {
      getStrictFilteredSchema(strict, MapStructV2.class);
      fail("this should throw");
    } catch (ThriftProjectionException e) {
      assertEquals("Cannot select only a subset of the fields in a map key, for path map1", e.getMessage());
    }
  }

  @Test
  public void testPartialKeyProjection() {
    doTestPartialKeyProjection("map1/key/age", "map1.key.age");
    doTestPartialKeyProjection("map1/key/age;map1/value/**", "map1.{key.age,value}");
  }

  @Test
  public void testSetPartialProjection() {

    try {
      getStrictFilteredSchema("set1.age", SetStructV2.class);
      fail("this should throw");
    } catch (ThriftProjectionException e) {
      assertEquals("Cannot select only a subset of the fields in a set, for path set1", e.getMessage());
    }
  }

  @Test
  public void testConvertStructCreatedViaDeprecatedConstructor() {
    String expected = "message ParquetSchema {\n" +
      "  required binary a (UTF8) = 1;\n" +
      "  required binary b (UTF8) = 2;\n" +
      "}\n";

    ThriftSchemaConverter converter = new ThriftSchemaConverter();

    StructType structType = new StructType(
      Arrays.asList(
        new ThriftField("a", (short) 1, REQUIRED, new ThriftType.StringType()),
        new ThriftField("b", (short) 2, REQUIRED, new ThriftType.StringType())
      )
    );

    final MessageType converted = converter.convert(structType);
    assertEquals(MessageTypeParser.parseMessageType(expected), converted);
  }

  public static void shouldGetProjectedSchema(String deprecatedFilterDesc, String strictFilterDesc, String expectedSchemaStr, Class<? extends TBase<?,?>> thriftClass) {
    MessageType strictRequestedSchema = getStrictFilteredSchema(strictFilterDesc, thriftClass);
    MessageType expectedSchema = parseMessageType(expectedSchemaStr);
    assertEquals(expectedSchema, strictRequestedSchema);
  }

  private static MessageType getStrictFilteredSchema(String semicolonDelimitedString, Class<? extends TBase<?,?>> thriftClass) {
    StrictFieldProjectionFilter fieldProjectionFilter = StrictFieldProjectionFilter.fromSemicolonDelimitedString(semicolonDelimitedString);
    return new ThriftSchemaConverter(fieldProjectionFilter).convert(thriftClass);
  }

  @Test
  public void testToThriftType() throws Exception {
    final StructType converted = ThriftSchemaConverter.toStructType(AddressBook.class);
    final String json = converted.toJSON();
    final ThriftType fromJSON = StructType.fromJSON(json);
    assertEquals(json, fromJSON.toJSON());
  }

  @Test
  public void testLogicalTypeConvertion() throws Exception {
    String expected =
      "message ParquetSchema {\n" +
        "  required int32 test_i16 (INTEGER(16,true)) = 1;" +
        "}";
    ThriftSchemaConverter schemaConverter = new ThriftSchemaConverter();
    final MessageType converted = schemaConverter.convert(TestLogicalType.class);
    assertEquals(MessageTypeParser.parseMessageType(expected), converted);
  }

}
