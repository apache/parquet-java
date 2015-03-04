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
package parquet.thrift;

import static org.junit.Assert.assertEquals;
import static parquet.schema.MessageTypeParser.parseMessageType;

import org.apache.thrift.TBase;
import org.junit.Test;

import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.thrift.projection.FieldProjectionFilter;
import parquet.thrift.projection.ThriftProjectionException;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.StructType;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.elephantbird.thrift.test.TestStructInMap;

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

    shouldGetProjectedSchema("name/first_name", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "  }}", Person.class);

    shouldGetProjectedSchema("name/first_name;name/last_name", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }}", Person.class);

    shouldGetProjectedSchema("name/{first,last}_name;", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }}", Person.class);

    shouldGetProjectedSchema("name/*", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }" +
            "}", Person.class);

    shouldGetProjectedSchema("name/*", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }" +
            "}", Person.class);

    shouldGetProjectedSchema("*/*_name", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "    optional binary last_name (UTF8) = 2;" +
            "  }" +
            "}", Person.class);

    shouldGetProjectedSchema("name/first_*", "message ParquetSchema {" +
            "  required group name = 1 {" +
            "    optional binary first_name (UTF8) = 1;" +
            "  }" +
            "}", Person.class);

    shouldGetProjectedSchema("*/*", "message ParquetSchema {" +
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


//    MessageType mapSchema=  MessageTypeParser.parseMessageType()

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
    shouldGetProjectedSchema("name;names/key*;names/value/**", "message ParquetSchema {\n" +
            "  optional binary name (UTF8) = 1;\n" +
            "  optional group names (MAP) = 2 {\n" +
            "    repeated group map (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "      optional group value {\n" +
            "        optional group name = 1 {\n" +
            "          optional binary first_name (UTF8) = 1;\n" +
            "          optional binary last_name (UTF8) = 2;\n" +
            "        }\n" +
            "        optional group phones (MAP) = 2 {\n" +
            "          repeated group map (MAP_KEY_VALUE) {\n" +
            "            required binary key (ENUM);\n" +
            "            optional binary value (UTF8);\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}", TestStructInMap.class);

    //project only one level of nested map
    shouldGetProjectedSchema("name;names/key;names/value/name/*", "message ParquetSchema {\n" +
            "  optional binary name (UTF8) = 1;\n" +
            "  optional group names (MAP) = 2 {\n" +
            "    repeated group map (MAP_KEY_VALUE) {\n" +
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
    shouldGetProjectedSchema("name;names/key","message ParquetSchema {\n" +
            "  optional binary name (UTF8) = 1;\n" +
            "  optional group names (MAP) = 2 {\n" +
            "    repeated group map (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "    }\n" +
            "  }\n" +
            "}",TestStructInMap.class);
  }

  @Test(expected = ThriftProjectionException.class)
  public void testProjectOnlyValueInMap() {
    System.out.println(getFilteredSchema("name;names/value/**", TestStructInMap.class));
  }

  private void shouldGetProjectedSchema(String filterDesc, String expectedSchemaStr, Class<? extends TBase<?,?>> thriftClass) {
    MessageType requestedSchema = getFilteredSchema(filterDesc, thriftClass);
    MessageType expectedSchema = parseMessageType(expectedSchemaStr);
    assertEquals(expectedSchema, requestedSchema);
  }

  private MessageType getFilteredSchema(String filterDesc, Class<? extends TBase<?,?>> thriftClass) {
    FieldProjectionFilter fieldProjectionFilter = new FieldProjectionFilter(filterDesc);
    return new ThriftSchemaConverter(fieldProjectionFilter).convert(thriftClass);
  }


  @Test
  public void testToThriftType() throws Exception {
    ThriftSchemaConverter schemaConverter = new ThriftSchemaConverter();
    final StructType converted = schemaConverter.toStructType(AddressBook.class);
    final String json = converted.toJSON();
    System.out.println(json);
    final ThriftType fromJSON = StructType.fromJSON(json);
    assertEquals(json, fromJSON.toJSON());
  }
}
