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
package org.apache.parquet.avro;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.node.NullNode;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;

public class TestAvroSchemaConverter {

  private static final Configuration NEW_BEHAVIOR = new Configuration(false);

  @BeforeClass
  public static void setupConf() {
    NEW_BEHAVIOR.setBoolean("parquet.avro.add-list-element-records", false);
    NEW_BEHAVIOR.setBoolean("parquet.avro.write-old-list-structure", false);
  }

  public static final String ALL_PARQUET_SCHEMA =
      "message org.apache.parquet.avro.myrecord {\n" +
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
      "  required group myarrayofoptional (LIST) {\n" +
      "    repeated group list {\n" +
      "      optional int32 element;\n" +
      "    }\n" +
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

  private void testAvroToParquetConversion(
      Schema avroSchema, String schemaString) throws Exception {
    testAvroToParquetConversion(new Configuration(false), avroSchema, schemaString);
  }

  private void testAvroToParquetConversion(
      Configuration conf, Schema avroSchema, String schemaString)
      throws Exception {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(conf);
    MessageType schema = avroSchemaConverter.convert(avroSchema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals("converting " + schema + " to " + schemaString, expectedMT.toString(),
        schema.toString());
  }

  private void testParquetToAvroConversion(
      Schema avroSchema, String schemaString) throws Exception {
    testParquetToAvroConversion(new Configuration(false), avroSchema, schemaString);
  }

  private void testParquetToAvroConversion(
      Configuration conf, Schema avroSchema, String schemaString)
      throws Exception {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(conf);
    Schema schema = avroSchemaConverter.convert(MessageTypeParser.parseMessageType
        (schemaString));
    assertEquals("converting " + schemaString + " to " + avroSchema, avroSchema.toString(),
        schema.toString());
  }

  private void testRoundTripConversion(
      Schema avroSchema, String schemaString) throws Exception {
    testRoundTripConversion(new Configuration(), avroSchema, schemaString);
  }

  private void testRoundTripConversion(
      Configuration conf, Schema avroSchema, String schemaString)
      throws Exception {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(conf);
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
        NEW_BEHAVIOR, schema,
        "message org.apache.parquet.avro.myrecord {\n" +
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
            "    repeated group list {\n" +
            "      required int32 element;\n" +
            "    }\n" +
            "  }\n" +
            "  required group myemptyarray (LIST) {\n" +
            "    repeated group list {\n" +
            "      required int32 element;\n" +
            "    }\n" +
            "  }\n" +
            "  optional group myoptionalarray (LIST) {\n" +
            "    repeated group list {\n" +
            "      required int32 element;\n" +
            "    }\n" +
            "  }\n" +
            "  required group myarrayofoptional (LIST) {\n" +
            "    repeated group list {\n" +
            "      optional int32 element;\n" +
            "    }\n" +
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
  public void testAllTypesOldListBehavior() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("all.avsc").openStream());
    testAvroToParquetConversion(
        schema,
        "message org.apache.parquet.avro.myrecord {\n" +
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
            "  required group myarrayofoptional (LIST) {\n" +
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
        Resources.getResource("allFromParquetNewBehavior.avsc").openStream());
    // Cannot use round-trip assertion because enum is lost
    testParquetToAvroConversion(NEW_BEHAVIOR, schema, ALL_PARQUET_SCHEMA);
  }

  @Test
  public void testAllTypesParquetToAvroOldBehavior() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("allFromParquetOldBehavior.avsc").openStream());
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
  public void testOptionalArrayElement() throws Exception {
    Schema schema = Schema.createRecord("record1", null, null, false);
    Schema optionalIntArray = Schema.createArray(optional(Schema.create(Schema.Type.INT)));
    schema.setFields(Arrays.asList(
        new Schema.Field("myintarray", optionalIntArray, null, null)
    ));
    testRoundTripConversion(
        NEW_BEHAVIOR, schema,
        "message record1 {\n" +
            "  required group myintarray (LIST) {\n" +
            "    repeated group list {\n" +
            "      optional int32 element;\n" +
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
    Schema innerRecord = Schema.createRecord("element", null, null, false);
    Schema optionalString = optional(Schema.create(Schema.Type.STRING));
    innerRecord.setFields(Lists.newArrayList(
        new Schema.Field("s1", optionalString, null, NullNode.getInstance()),
        new Schema.Field("s2", optionalString, null, NullNode.getInstance())
    ));
    Schema schema = Schema.createRecord("HasArray", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("myarray", Schema.createArray(optional(innerRecord)),
            null, null)
    ));
    System.err.println("Avro schema: " + schema.toString(true));

    testRoundTripConversion(NEW_BEHAVIOR, schema, "message HasArray {\n" +
        "  required group myarray (LIST) {\n" +
        "    repeated group list {\n" +
        "      optional group element {\n" +
        "        optional binary s1 (UTF8);\n" +
        "        optional binary s2 (UTF8);\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n");
  }

  @Test
  public void testArrayOfOptionalRecordsOldBehavior() throws Exception {
    Schema innerRecord = Schema.createRecord("InnerRecord", null, null, false);
    Schema optionalString = optional(Schema.create(Schema.Type.STRING));
    innerRecord.setFields(Lists.newArrayList(
        new Schema.Field("s1", optionalString, null, NullNode.getInstance()),
        new Schema.Field("s2", optionalString, null, NullNode.getInstance())
    ));
    Schema schema = Schema.createRecord("HasArray", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("myarray", Schema.createArray(optional(innerRecord)),
            null, null)
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

  @Test
  public void testOldAvroListOfLists() throws Exception {
    Schema listOfLists = optional(Schema.createArray(Schema.createArray(
        Schema.create(Schema.Type.INT))));
    Schema schema = Schema.createRecord("AvroCompatListInList", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("listOfLists", listOfLists, null, NullNode.getInstance())
    ));
    System.err.println("Avro schema: " + schema.toString(true));

    testRoundTripConversion(schema,
        "message AvroCompatListInList {\n" +
            "  optional group listOfLists (LIST) {\n" +
            "    repeated group array (LIST) {\n" +
            "      repeated int32 array;\n" +
            "    }\n" +
            "  }\n" +
            "}");
    // Cannot use round-trip assertion because 3-level representation is used
    testParquetToAvroConversion(NEW_BEHAVIOR, schema,
        "message AvroCompatListInList {\n" +
            "  optional group listOfLists (LIST) {\n" +
            "    repeated group array (LIST) {\n" +
            "      repeated int32 array;\n" +
            "    }\n" +
            "  }\n" +
            "}");
  }

  @Test
  public void testOldThriftListOfLists() throws Exception {
    Schema listOfLists = optional(Schema.createArray(Schema.createArray(
        Schema.create(Schema.Type.INT))));
    Schema schema = Schema.createRecord("ThriftCompatListInList", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("listOfLists", listOfLists, null, NullNode.getInstance())
    ));
    System.err.println("Avro schema: " + schema.toString(true));

    // Cannot use round-trip assertion because repeated group names differ
    testParquetToAvroConversion(schema,
        "message ThriftCompatListInList {\n" +
            "  optional group listOfLists (LIST) {\n" +
            "    repeated group listOfLists_tuple (LIST) {\n" +
            "      repeated int32 listOfLists_tuple_tuple;\n" +
            "    }\n" +
            "  }\n" +
            "}");
    // Cannot use round-trip assertion because 3-level representation is used
    testParquetToAvroConversion(NEW_BEHAVIOR, schema,
        "message ThriftCompatListInList {\n" +
        "  optional group listOfLists (LIST) {\n" +
        "    repeated group listOfLists_tuple (LIST) {\n" +
        "      repeated int32 listOfLists_tuple_tuple;\n" +
        "    }\n" +
        "  }\n" +
        "}");
  }

  @Test
  public void testUnknownTwoLevelListOfLists() throws Exception {
    // This tests the case where we don't detect a 2-level list by the repeated
    // group's name, but it must be 2-level because the repeated group doesn't
    // contain an optional or repeated element as required for 3-level lists
    Schema listOfLists = optional(Schema.createArray(Schema.createArray(
        Schema.create(Schema.Type.INT))));
    Schema schema = Schema.createRecord("UnknownTwoLevelListInList", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("listOfLists", listOfLists, null, NullNode.getInstance())
    ));
    System.err.println("Avro schema: " + schema.toString(true));

    // Cannot use round-trip assertion because repeated group names differ
    testParquetToAvroConversion(schema,
        "message UnknownTwoLevelListInList {\n" +
            "  optional group listOfLists (LIST) {\n" +
            "    repeated group mylist (LIST) {\n" +
            "      repeated int32 innerlist;\n" +
            "    }\n" +
            "  }\n" +
            "}");
    // Cannot use round-trip assertion because 3-level representation is used
    testParquetToAvroConversion(NEW_BEHAVIOR, schema,
        "message UnknownTwoLevelListInList {\n" +
            "  optional group listOfLists (LIST) {\n" +
            "    repeated group mylist (LIST) {\n" +
            "      repeated int32 innerlist;\n" +
            "    }\n" +
            "  }\n" +
            "}");
  }

  @Test
  public void testParquetMapWithoutMapKeyValueAnnotation() throws Exception {
    Schema schema = Schema.createRecord("myrecord", null, null, false);
    Schema map = Schema.createMap(Schema.create(Schema.Type.INT));
    schema.setFields(Collections.singletonList(new Schema.Field("mymap", map, null, null)));
    String parquetSchema =
        "message myrecord {\n" +
            "  required group mymap (MAP) {\n" +
            "    repeated group map {\n" +
            "      required binary key (UTF8);\n" +
            "      required int32 value;\n" +
            "    }\n" +
            "  }\n" +
            "}\n";

    testParquetToAvroConversion(schema, parquetSchema);
    testParquetToAvroConversion(NEW_BEHAVIOR, schema, parquetSchema);
  }

  public static Schema optional(Schema original) {
    return Schema.createUnion(Lists.newArrayList(
        Schema.create(Schema.Type.NULL),
        original));
  }
}
