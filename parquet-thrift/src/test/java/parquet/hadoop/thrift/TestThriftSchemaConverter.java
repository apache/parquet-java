/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop.thrift;

import static junit.framework.Assert.assertEquals;
import junit.framework.Assert;

import org.junit.Test;

import parquet.parser.MessageTypeParser;
import parquet.schema.MessageType;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.StructType;

import com.twitter.data.proto.tutorial.thrift.AddressBook;

public class TestThriftSchemaConverter {

  @Test
  public void testToMessageType() throws Exception {
    String expected =
    "message AddressBook {\n" +
    "  optional group persons (LIST) {\n" +
    "    repeated group persons_tuple {\n" +
    "      optional group name {\n" +
    "        optional binary first_name (UTF8);\n" +
    "        optional binary last_name (UTF8);\n" +
    "      }\n" +
    "      optional int32 id;\n" +
    "      optional binary email (UTF8);\n" +
    "      optional group phones (LIST) {\n" +
    "        repeated group phones_tuple {\n" +
    "          optional binary number (UTF8);\n" +
    "          optional binary type (ENUM);\n" +
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
  public void testToThriftType() throws Exception {
    ThriftSchemaConverter schemaConverter = new ThriftSchemaConverter();
    final StructType converted = schemaConverter.toStructType(AddressBook.class);
    final String json = converted.toJSON();
    System.out.println(json);
    final ThriftType fromJSON = StructType.fromJSON(json);
    assertEquals(json, fromJSON.toJSON());
  }
}
