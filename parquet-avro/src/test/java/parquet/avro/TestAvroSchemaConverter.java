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
package parquet.avro;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;
import org.junit.Test;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;

public class TestAvroSchemaConverter {

    public static final String ALL_PARQUET_SCHEMA =
      "message parquet.avro.myrecord {\n" +
      "  required boolean myboolean;\n" +
      "  required int32 myint;\n" +
      "  required int64 mylong;\n" +
      "  required float myfloat;\n" +
      "  required double mydouble;\n" +
      "  required binary mybytes;\n" +
      "  required binary mystring (UTF8);\n" +
      "  required group mynestedrecord {\n" +
      "    required int32 mynestedint;\n" +
      "  }\n" +
      "  required binary myenum (ENUM);\n" +
      "  required group myarray (LIST) {\n" +
      "    repeated int32 array;\n" +
      "  }\n" +
      "  optional group myoptionalarray (LIST) {\n" +
      "    repeated int32 array;\n" +
      "  }\n" +
      "  required group myrecordarray (LIST) {\n" +
      "    repeated group array {\n" +
      "      required int32 a;\n" +
      "      required int32 b;\n" +
      "    }\n" +
      "  }\n" +
      "  required group mymap (MAP) {\n" +
      "    repeated group map (MAP_KEY_VALUE) {\n" +
      "      required binary key (UTF8);\n" +
      "      required int32 value;\n" +
      "    }\n" +
      "  }\n" +
      "  required fixed_len_byte_array(1) myfixed;\n" +
      "}\n";

  private void testAvroToParquetConversion(Schema avroSchema, String schemaString) throws
      Exception {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
    MessageType schema = avroSchemaConverter.convert(avroSchema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals("converting " + schema + " to " + schemaString, expectedMT.toString(),
        schema.toString());
  }

  private void testParquetToAvroConversion(Schema avroSchema, String schemaString) throws
      Exception {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
    Schema schema = avroSchemaConverter.convert(MessageTypeParser.parseMessageType
        (schemaString));
    assertEquals("converting " + schemaString + " to " + avroSchema, avroSchema.toString(),
        schema.toString());
  }

  private void testRoundTripConversion(Schema avroSchema, String schemaString) throws
      Exception {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
    MessageType schema = avroSchemaConverter.convert(avroSchema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals("converting " + schema + " to " + schemaString, expectedMT.toString(),
        schema.toString());
    Schema convertedAvroSchema = avroSchemaConverter.convert(expectedMT);
    assertEquals("converting " + expectedMT + " to " + avroSchema.toString(true),
        avroSchema.toString(), convertedAvroSchema.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTopLevelMustBeARecord() {
    new AvroSchemaConverter().convert(Schema.create(Schema.Type.INT));
  }

  @Test
  public void testAllTypes() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("all.avsc").openStream());
    testAvroToParquetConversion(
        schema,
        "message parquet.avro.myrecord {\n" +
            // Avro nulls are not encoded, unless they are null unions
            "  required boolean myboolean;\n" +
            "  required int32 myint;\n" +
            "  required int64 mylong;\n" +
            "  required float myfloat;\n" +
            "  required double mydouble;\n" +
            "  required binary mybytes;\n" +
            "  required binary mystring (UTF8);\n" +
            "  required group mynestedrecord {\n" +
            "    required int32 mynestedint;\n" +
            "  }\n" +
            "  required binary myenum (ENUM);\n" +
            "  required group myarray (LIST) {\n" +
            "    repeated int32 array;\n" +
            "  }\n" +
            "  required group myemptyarray (LIST) {\n" +
            "    repeated int32 array;\n" +
            "  }\n" +
            "  optional group myoptionalarray (LIST) {\n" +
            "    repeated int32 array;\n" +
            "  }\n" +
            "  required group mymap (MAP) {\n" +
            "    repeated group map (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "      required int32 value;\n" +
            "    }\n" +
            "  }\n" +
            "  required group myemptymap (MAP) {\n" +
            "    repeated group map (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "      required int32 value;\n" +
            "    }\n" +
            "  }\n" +
            "  required fixed_len_byte_array(1) myfixed;\n" +
            "}\n");
  }

  @Test
  public void testAllTypesParquetToAvro() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("allFromParquet.avsc").openStream());
    // Cannot use round-trip assertion because enum is lost
    testParquetToAvroConversion(schema, ALL_PARQUET_SCHEMA);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParquetMapWithNonStringKeyFails() throws Exception {
    MessageType parquetSchema = MessageTypeParser.parseMessageType(
      "message myrecord {\n" +
        "  required group mymap (MAP) {\n" +
        "    repeated group map (MAP_KEY_VALUE) {\n" +
        "      required int32 key;\n" +
        "      required int32 value;\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
    );
    new AvroSchemaConverter().convert(parquetSchema);
  }

  @Test
  public void testOptionalFields() throws Exception {
    Schema schema = Schema.createRecord("record1", null, null, false);
    Schema optionalInt = optional(Schema.create(Schema.Type.INT));
    schema.setFields(Arrays.asList(
        new Schema.Field("myint", optionalInt, null, NullNode.getInstance())
    ));
    testRoundTripConversion(
        schema,
        "message record1 {\n" +
            "  optional int32 myint;\n" +
            "}\n");
  }

  @Test
  public void testOptionalMapValue() throws Exception {
    Schema schema = Schema.createRecord("record1", null, null, false);
    Schema optionalIntMap = Schema.createMap(optional(Schema.create(Schema.Type.INT)));
    schema.setFields(Arrays.asList(
        new Schema.Field("myintmap", optionalIntMap, null, null)
    ));
    testRoundTripConversion(
        schema,
        "message record1 {\n" +
            "  required group myintmap (MAP) {\n" +
            "    repeated group map (MAP_KEY_VALUE) {\n" +
            "      required binary key (UTF8);\n" +
            "      optional int32 value;\n" +
            "    }\n" +
            "  }\n" +
            "}\n");
  }

  @Test
  public void testUnionOfTwoTypes() throws Exception {
    Schema schema = Schema.createRecord("record2", null, null, false);
    Schema multipleTypes = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type
        .NULL),
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.FLOAT)));
    schema.setFields(Arrays.asList(
        new Schema.Field("myunion", multipleTypes, null, NullNode.getInstance())));

    // Avro union is modelled using optional data members of the different
    // types. This does not translate back into an Avro union
    testAvroToParquetConversion(
        schema,
        "message record2 {\n" +
            "  optional group myunion {\n" +
            "    optional int32 member0;\n" +
            "    optional float member1;\n" +
            "  }\n" +
            "}\n");
  }

  @Test
  public void testArrayOfOptionalRecords() throws Exception {
    Schema innerRecord = Schema.createRecord("InnerRecord", null, null, false);
    Schema optionalString = optional(Schema.create(Schema.Type.STRING));
    innerRecord.setFields(Lists.newArrayList(
        new Schema.Field("s1", optionalString, null, NullNode.getInstance()),
        new Schema.Field("s2", optionalString, null, NullNode.getInstance())
    ));
    Schema schema = Schema.createRecord("HasArray", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("myarray", Schema.createArray(optional(innerRecord)),
            null, NullNode.getInstance())
    ));
    System.err.println("Avro schema: " + schema.toString(true));

    // Cannot use round-trip assertion because InnerRecord optional is removed
    testAvroToParquetConversion(schema, "message HasArray {\n" +
        "  required group myarray (LIST) {\n" +
        "    repeated group array {\n" +
        "      optional binary s1 (UTF8);\n" +
        "      optional binary s2 (UTF8);\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  public static Schema optional(Schema original) {
    return Schema.createUnion(Lists.newArrayList(
        Schema.create(Schema.Type.NULL),
        original));
  }
}
