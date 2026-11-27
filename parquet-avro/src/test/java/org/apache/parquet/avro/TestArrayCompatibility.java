/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.avro;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroTestUtil.array;
import static org.apache.parquet.avro.AvroTestUtil.field;
import static org.apache.parquet.avro.AvroTestUtil.instance;
import static org.apache.parquet.avro.AvroTestUtil.optional;
import static org.apache.parquet.avro.AvroTestUtil.optionalField;
import static org.apache.parquet.avro.AvroTestUtil.primitive;
import static org.apache.parquet.avro.AvroTestUtil.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.DirectWriterTest;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestArrayCompatibility extends DirectWriterTest {

  public static final Configuration OLD_BEHAVIOR_CONF = new Configuration();
  public static final Configuration NEW_BEHAVIOR_CONF = new Configuration();
  public static final Configuration AUTO_DETECT_CONF = new Configuration();

  @BeforeAll
  public static void setupNewBehaviorConfiguration() {
    OLD_BEHAVIOR_CONF.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, true);
    OLD_BEHAVIOR_CONF.setBoolean(AvroReadSupport.AUTO_DETECT_LIST_STRUCTURE, false);
    NEW_BEHAVIOR_CONF.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);
    AUTO_DETECT_CONF.setBoolean(AvroReadSupport.AUTO_DETECT_LIST_STRUCTURE, true);
  }

  @Test
  @Disabled(value = "Not yet supported")
  public void testUnannotatedListOfPrimitives() throws Exception {
    Path test =
        writeDirect("message UnannotatedListOfPrimitives {" + "  repeated int32 list_of_ints;" + "}", rc -> {
          rc.startMessage();
          rc.startField("list_of_ints", 0);

          rc.addInteger(34);
          rc.addInteger(35);
          rc.addInteger(36);

          rc.endField("list_of_ints", 0);
          rc.endMessage();
        });

    Schema expectedSchema = record("OldPrimitiveInList", field("list_of_ints", array(primitive(Schema.Type.INT))));

    GenericRecord expectedRecord = instance(expectedSchema, "list_of_ints", Arrays.asList(34, 35, 36));

    // both should behave the same way
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  @Disabled(value = "Not yet supported")
  public void testUnannotatedListOfGroups() throws Exception {
    Path test = writeDirect(
        "message UnannotatedListOfGroups {" + "  repeated group list_of_points {"
            + "    required float x;"
            + "    required float y;"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("list_of_points", 0);

          rc.startGroup();
          rc.startField("x", 0);
          rc.addFloat(1.0f);
          rc.endField("x", 0);
          rc.startField("y", 1);
          rc.addFloat(1.0f);
          rc.endField("y", 1);
          rc.endGroup();

          rc.startGroup();
          rc.startField("x", 0);
          rc.addFloat(2.0f);
          rc.endField("x", 0);
          rc.startField("y", 1);
          rc.addFloat(2.0f);
          rc.endField("y", 1);
          rc.endGroup();

          rc.endField("list_of_points", 0);
          rc.endMessage();
        });

    Schema point = record("?", field("x", primitive(Schema.Type.FLOAT)), field("y", primitive(Schema.Type.FLOAT)));
    Schema expectedSchema = record("OldPrimitiveInList", field("list_of_points", array(point)));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "list_of_points",
        Arrays.asList(instance(point, "x", 1.0f, "y", 1.0f), instance(point, "x", 2.0f, "y", 2.0f)));

    // both should behave the same way
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  public void testRepeatedPrimitiveInList() throws Exception {
    Path test = writeDirect(
        "message RepeatedPrimitiveInList {" + "  required group list_of_ints (LIST) {"
            + "    repeated int32 array;"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("list_of_ints", 0);

          rc.startGroup();
          rc.startField("array", 0);

          rc.addInteger(34);
          rc.addInteger(35);
          rc.addInteger(36);

          rc.endField("array", 0);
          rc.endGroup();

          rc.endField("list_of_ints", 0);
          rc.endMessage();
        });

    Schema expectedSchema =
        record("RepeatedPrimitiveInList", field("list_of_ints", array(Schema.create(Schema.Type.INT))));

    GenericRecord expectedRecord = instance(expectedSchema, "list_of_ints", Arrays.asList(34, 35, 36));

    // both should behave the same way
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  public void testMultiFieldGroupInList() throws Exception {
    // tests the missing element layer, detected by a multi-field group
    Path test = writeDirect(
        "message MultiFieldGroupInList {" + "  optional group locations (LIST) {"
            + "    repeated group element {"
            + "      required double latitude;"
            + "      required double longitude;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(0.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));
    Schema expectedSchema = record("MultiFieldGroupInList", optionalField("locations", array(location)));

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "locations",
        Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 0.0),
            instance(location, "latitude", 0.0, "longitude", 180.0)));

    // both should behave the same way
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  public void testSingleFieldGroupInList() throws Exception {
    // this tests the case where non-avro older data has an ambiguous list
    Path test = writeDirect(
        "message SingleFieldGroupInList {" + "  optional group single_element_groups (LIST) {"
            + "    repeated group single_element_group {"
            + "      required int64 count;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("single_element_groups", 0);

          rc.startGroup();
          rc.startField("single_element_group", 0); // start writing array contents

          rc.startGroup();
          rc.startField("count", 0);
          rc.addLong(1234L);
          rc.endField("count", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("count", 0);
          rc.addLong(2345L);
          rc.endField("count", 0);
          rc.endGroup();

          rc.endField("single_element_group", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("single_element_groups", 0);
          rc.endMessage();
        });

    // can't tell from storage whether this should be a list of single-field
    // records or if the single_field_group layer is synthetic.

    // old behavior - assume that the repeated type is the element type
    Schema singleElementGroupSchema = record("single_element_group", field("count", primitive(Schema.Type.LONG)));
    Schema oldSchema = record(
        "SingleFieldGroupInList", optionalField("single_element_groups", array(singleElementGroupSchema)));
    GenericRecord oldRecord = instance(
        oldSchema,
        "single_element_groups",
        Arrays.asList(
            instance(singleElementGroupSchema, "count", 1234L),
            instance(singleElementGroupSchema, "count", 2345L)));

    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    // new behavior - assume that single_element_group is synthetic (in spec)
    Schema newSchema = record(
        "SingleFieldGroupInList", optionalField("single_element_groups", array(primitive(Schema.Type.LONG))));
    GenericRecord newRecord = instance(newSchema, "single_element_groups", Arrays.asList(1234L, 2345L));

    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);
  }

  @Test
  public void testSingleFieldGroupInListWithSchema() throws Exception {
    // this tests the case where older data has an ambiguous structure, but the
    // correct interpretation can be determined from the avro schema

    Schema singleElementRecord = record("single_element_group", field("count", primitive(Schema.Type.LONG)));

    Schema expectedSchema =
        record("SingleFieldGroupInList", optionalField("single_element_groups", array(singleElementRecord)));

    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(AvroWriteSupport.AVRO_SCHEMA, expectedSchema.toString());

    Path test = writeDirect(
        "message SingleFieldGroupInList {" + "  optional group single_element_groups (LIST) {"
            + "    repeated group single_element_group {"
            + "      required int64 count;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("single_element_groups", 0);

          rc.startGroup();
          rc.startField("single_element_group", 0); // start writing array contents

          rc.startGroup();
          rc.startField("count", 0);
          rc.addLong(1234L);
          rc.endField("count", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("count", 0);
          rc.addLong(2345L);
          rc.endField("count", 0);
          rc.endGroup();

          rc.endField("single_element_group", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("single_element_groups", 0);
          rc.endMessage();
        },
        metadata);

    GenericRecord expectedRecord = instance(
        expectedSchema,
        "single_element_groups",
        Arrays.asList(
            instance(singleElementRecord, "count", 1234L), instance(singleElementRecord, "count", 2345L)));

    // both should behave the same way because the schema is present
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  public void testNewOptionalGroupInList() throws Exception {
    Path test = writeDirect(
        "message NewOptionalGroupInList {" + "  optional group locations (LIST) {"
            + "    repeated group list {"
            + "      optional group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("list", 0); // start writing array contents

          // write a non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(0.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          // write a null element (element field is omitted)
          rc.startGroup(); // array level
          rc.endGroup(); // array level

          // write a second non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          rc.endField("list", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("list", optionalField("element", location));
    Schema oldSchema = record("NewOptionalGroupInList", optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(
        oldSchema,
        "locations",
        Arrays.asList(
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 0.0)),
            instance(elementRecord),
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 180.0))));

    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    // new behavior - assume that single_element_group is synthetic (in spec)
    Schema newSchema = record("NewOptionalGroupInList", optionalField("locations", array(optional(location))));
    GenericRecord newRecord = instance(
        newSchema,
        "locations",
        Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 0.0),
            null,
            instance(location, "latitude", 0.0, "longitude", 180.0)));

    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);
  }

  @Test
  public void testNewRequiredGroupInList() throws Exception {
    Path test = writeDirect(
        "message NewRequiredGroupInList {" + "  optional group locations (LIST) {"
            + "    repeated group list {"
            + "      required group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("list", 0); // start writing array contents

          // write a non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          // write a second non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(0.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          rc.endField("list", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("list", field("element", location));
    Schema oldSchema = record("NewRequiredGroupInList", optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(
        oldSchema,
        "locations",
        Arrays.asList(
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 0.0))));

    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    // new behavior - assume that single_element_group is synthetic (in spec)
    Schema newSchema = record("NewRequiredGroupInList", optionalField("locations", array(location)));
    GenericRecord newRecord = instance(
        newSchema,
        "locations",
        Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);
  }

  @Test
  public void testAvroCompatOptionalGroupInList() throws Exception {
    Path test = writeDirect(
        "message AvroCompatOptionalGroupInList {" + "  optional group locations (LIST) {"
            + "    repeated group array {"
            + "      optional group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("array", 0); // start writing array contents

          // write a non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          // write a second non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(0.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          rc.endField("array", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("array", optionalField("element", location));
    Schema oldSchema = record("AvroCompatOptionalGroupInList", optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(
        oldSchema,
        "locations",
        Arrays.asList(
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 0.0))));

    // both should detect the "array" name
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);
    assertReaderContains(newBehaviorReader(test), oldSchema, oldRecord);
  }

  @Test
  public void testAvroCompatOptionalGroupInListWithSchema() throws Exception {
    Path test = writeDirect(
        "message AvroCompatOptionalGroupInListWithSchema {" + "  optional group locations (LIST) {"
            + "    repeated group my_list {"
            + "      optional group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("my_list", 0); // start writing array contents

          // write a non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          // write a second non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(0.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          rc.endField("array", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    Schema newSchema = record(
        "AvroCompatOptionalGroupInListWithSchema", optionalField("locations", array(optional(location))));
    GenericRecord newRecord = instance(
        newSchema,
        "locations",
        Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    Configuration oldConfWithSchema = new Configuration();
    AvroReadSupport.setAvroReadSchema(oldConfWithSchema, newSchema);

    // both should use the schema structure that is provided
    assertReaderContains(new AvroParquetReader<GenericRecord>(oldConfWithSchema, test), newSchema, newRecord);

    Configuration newConfWithSchema = new Configuration(NEW_BEHAVIOR_CONF);
    AvroReadSupport.setAvroReadSchema(newConfWithSchema, newSchema);

    assertReaderContains(new AvroParquetReader<GenericRecord>(newConfWithSchema, test), newSchema, newRecord);
  }

  @Test
  public void testAvroCompatListInList() throws Exception {
    Path test = writeDirect(
        "message AvroCompatListInList {" + "  optional group listOfLists (LIST) {"
            + "    repeated group array (LIST) {"
            + "      repeated int32 array;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("array", 0); // start writing array contents

          rc.startGroup();
          rc.startField("array", 0); // start writing inner array contents

          // write [34, 35, 36]
          rc.addInteger(34);
          rc.addInteger(35);
          rc.addInteger(36);

          rc.endField("array", 0); // finished writing inner array contents
          rc.endGroup();

          // write an empty list
          rc.startGroup();
          rc.endGroup();

          rc.startGroup();
          rc.startField("array", 0); // start writing inner array contents

          // write [32, 33, 34]
          rc.addInteger(32);
          rc.addInteger(33);
          rc.addInteger(34);

          rc.endField("array", 0); // finished writing inner array contents
          rc.endGroup();

          rc.endField("array", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema listOfLists = array(array(primitive(Schema.Type.INT)));
    Schema oldSchema = record("AvroCompatListInList", optionalField("listOfLists", listOfLists));

    GenericRecord oldRecord = instance(
        oldSchema,
        "listOfLists",
        Arrays.asList(Arrays.asList(34, 35, 36), Arrays.asList(), Arrays.asList(32, 33, 34)));

    // both should detect the "array" name
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);
    assertReaderContains(newBehaviorReader(test), oldSchema, oldRecord);
  }

  @Test
  public void testThriftCompatListInList() throws Exception {
    Path test = writeDirect(
        "message ThriftCompatListInList {" + "  optional group listOfLists (LIST) {"
            + "    repeated group listOfLists_tuple (LIST) {"
            + "      repeated int32 listOfLists_tuple_tuple;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("listOfLists_tuple", 0); // start writing array contents

          rc.startGroup();
          rc.startField("listOfLists_tuple_tuple", 0); // start writing inner array contents

          // write [34, 35, 36]
          rc.addInteger(34);
          rc.addInteger(35);
          rc.addInteger(36);

          rc.endField("listOfLists_tuple_tuple", 0); // finished writing inner array contents
          rc.endGroup();

          // write an empty list
          rc.startGroup();
          rc.endGroup();

          rc.startGroup();
          rc.startField("listOfLists_tuple_tuple", 0); // start writing inner array contents

          // write [32, 33, 34]
          rc.addInteger(32);
          rc.addInteger(33);
          rc.addInteger(34);

          rc.endField("listOfLists_tuple_tuple", 0); // finished writing inner array contents
          rc.endGroup();

          rc.endField("listOfLists_tuple", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema listOfLists = array(array(primitive(Schema.Type.INT)));
    Schema oldSchema = record("ThriftCompatListInList", optionalField("listOfLists", listOfLists));

    GenericRecord oldRecord = instance(
        oldSchema,
        "listOfLists",
        Arrays.asList(Arrays.asList(34, 35, 36), Arrays.asList(), Arrays.asList(32, 33, 34)));

    // both should detect the "_tuple" names
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);
    assertReaderContains(newBehaviorReader(test), oldSchema, oldRecord);
  }

  @Test
  public void testThriftCompatRequiredGroupInList() throws Exception {
    Path test = writeDirect(
        "message ThriftCompatRequiredGroupInList {" + "  optional group locations (LIST) {"
            + "    repeated group locations_tuple {"
            + "      optional group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("locations_tuple", 0); // start writing array contents

          // write a non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          // write a second non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(0.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          rc.endField("locations_tuple", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("locations_tuple", optionalField("element", location));
    Schema oldSchema = record("ThriftCompatRequiredGroupInList", optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(
        oldSchema,
        "locations",
        Arrays.asList(
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 0.0))));

    // both should detect the "array" name
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);
    assertReaderContains(newBehaviorReader(test), oldSchema, oldRecord);
  }

  @Test
  public void testHiveCompatOptionalGroupInList() throws Exception {
    Path test = writeDirect(
        "message HiveCompatOptionalGroupInList {" + "  optional group locations (LIST) {"
            + "    repeated group bag {"
            + "      optional group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("bag", 0); // start writing array contents

          // write a non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          // write a second non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(0.0);
          rc.endField("longitude", 1);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          rc.endField("bag", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("bag", optionalField("element", location));
    Schema oldSchema = record("HiveCompatOptionalGroupInList", optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(
        oldSchema,
        "locations",
        Arrays.asList(
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 0.0))));

    // both should detect the "array" name
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    Schema newSchema =
        record("HiveCompatOptionalGroupInList", optionalField("locations", array(optional(location))));
    GenericRecord newRecord = instance(
        newSchema,
        "locations",
        Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);
  }

  @Test
  public void testListOfSingleElementStructsWithElementField() throws Exception {
    Path test = writeDirect(
        "message ListOfSingleElementStructsWithElementField {" + "  optional group list_of_structs (LIST) {"
            + "    repeated group list {"
            + "      required group element {"
            + "        required float element;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("list_of_structs", 0);

          rc.startGroup();
          rc.startField("list", 0); // start writing array contents

          // write a non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          // the inner element field
          rc.startGroup();
          rc.startField("element", 0);
          rc.addFloat(33.0F);
          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          // write a second non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);

          // the inner element field
          rc.startGroup();
          rc.startField("element", 0);
          rc.addFloat(34.0F);
          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("element", 0);
          rc.endGroup(); // array level

          rc.endField("list", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("list_of_structs", 0);
          rc.endMessage();
        });

    Schema structWithElementField = record("element", field("element", primitive(Schema.Type.FLOAT)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("list", field("element", structWithElementField));
    Schema oldSchema = record(
        "ListOfSingleElementStructsWithElementField", optionalField("list_of_structs", array(elementRecord)));
    GenericRecord oldRecord = instance(
        oldSchema,
        "list_of_structs",
        Arrays.asList(
            instance(elementRecord, "element", instance(structWithElementField, "element", 33.0F)),
            instance(elementRecord, "element", instance(structWithElementField, "element", 34.0F))));

    // check the schema
    final MessageType fileSchema;
    try (ParquetFileReader reader = ParquetFileReader.open(new Configuration(), test)) {
      fileSchema = reader.getFileMetaData().getSchema();
      assertThat(new AvroSchemaConverter(OLD_BEHAVIOR_CONF).convert(fileSchema))
          .as("Converted schema should assume 2-layer structure")
          .isEqualTo(oldSchema);
    }

    // both should default to the 2-layer structure
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    Schema newSchema = record(
        "ListOfSingleElementStructsWithElementField",
        optionalField("list_of_structs", array(structWithElementField)));
    GenericRecord newRecord = instance(
        newSchema,
        "list_of_structs",
        Arrays.asList(
            instance(structWithElementField, "element", 33.0F),
            instance(structWithElementField, "element", 34.0F)));

    // check the schema
    assertThat(new AvroSchemaConverter(NEW_BEHAVIOR_CONF).convert(fileSchema))
        .as("Converted schema should assume 3-layer structure")
        .isEqualTo(newSchema);
    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);

    // check that this works with compatible nested schemas

    Schema structWithDoubleElementField = record("element", field("element", primitive(Schema.Type.DOUBLE)));

    Schema doubleElementRecord = record("list", field("element", structWithDoubleElementField));
    Schema oldDoubleSchema = record(
        "ListOfSingleElementStructsWithElementField",
        optionalField("list_of_structs", array(doubleElementRecord)));
    GenericRecord oldDoubleRecord = instance(
        oldDoubleSchema,
        "list_of_structs",
        Arrays.asList(
            instance(
                doubleElementRecord,
                "element",
                instance(structWithDoubleElementField, "element", 33.0)),
            instance(
                doubleElementRecord,
                "element",
                instance(structWithDoubleElementField, "element", 34.0))));
    assertReaderContains(oldBehaviorReader(test, oldDoubleSchema), oldDoubleSchema, oldDoubleRecord);

    Schema newDoubleSchema = record(
        "ListOfSingleElementStructsWithElementField",
        optionalField("list_of_structs", array(structWithDoubleElementField)));
    GenericRecord newDoubleRecord = instance(
        newDoubleSchema,
        "list_of_structs",
        Arrays.asList(
            instance(structWithDoubleElementField, "element", 33.0),
            instance(structWithDoubleElementField, "element", 34.0)));
    assertReaderContains(newBehaviorReader(test, newDoubleSchema), newDoubleSchema, newDoubleRecord);
  }

  @Test
  public void testIsElementTypeRequiredRepeatedRecord() {
    // Test `_tuple` style naming
    MessageType parquetSchema = MessageTypeParser.parseMessageType("message SchemaWithList {\n"
        + "  required group list_field (LIST) {\n"
        + "    repeated group list_field_tuple (LIST) {\n"
        + "      repeated int32 int_field;\n"
        + "    }\n"
        + "  }\n"
        + "}");
    Schema avroSchema = new AvroSchemaConverter().convert(parquetSchema);

    assertThat(AvroRecordConverter.isElementType(
            parquetSchema.getType("list_field").asGroupType().getType("list_field_tuple"),
            avroSchema.getFields().get(0).schema()))
        .isTrue();

    // Test `array` style naming
    parquetSchema = MessageTypeParser.parseMessageType("message SchemaWithList {\n"
        + "  required group list_field (LIST) {\n"
        + "    repeated group array {\n"
        + "      required int32 a;\n"
        + "    }\n"
        + "  }\n"
        + "}");
    avroSchema = new AvroSchemaConverter().convert(parquetSchema);

    assertThat(AvroRecordConverter.isElementType(
            parquetSchema.getType("list_field"),
            avroSchema.getFields().get(0).schema()))
        .isTrue();
  }

  @Test
  public void testIsElementTypeInt96Element() {
    Configuration configuration = new Configuration();
    configuration.setBoolean(READ_INT96_AS_FIXED, true);

    MessageType parquetSchema = MessageTypeParser.parseMessageType("message SchemaWithInt96 {\n"
        + "  optional group list (LIST) {\n"
        + "    repeated group list {\n"
        + "      optional int96 a_timestamp;\n"
        + "    }\n"
        + "  }\n"
        + "}");
    Schema avroSchema = new AvroSchemaConverter(configuration).convert(parquetSchema);
    assertThat(AvroRecordConverter.isElementType(
            parquetSchema.getType("list").asGroupType().getType("list"),
            AvroSchemaConverter.getNonNull(
                    avroSchema.getFields().get(0).schema())
                .getElementType()))
        .isTrue();
  }

  @Test
  public void testIsElementTypeOptionalRepeatedRecord() {
    // Test `_tuple` style naming
    MessageType parquetSchema = MessageTypeParser.parseMessageType("message SchemaWithList {\n"
        + "  optional group list_field (LIST) {\n"
        + "    repeated group list_field_tuple (LIST) {\n"
        + "      repeated int32 int_field;\n"
        + "    }\n"
        + "  }\n"
        + "}");
    Schema avroSchema = new AvroSchemaConverter().convert(parquetSchema);

    assertThat(AvroRecordConverter.isElementType(
            parquetSchema.getType("list_field").asGroupType().getType("list_field_tuple"),
            avroSchema.getFields().get(0).schema()))
        .isTrue();

    // Test `array` style naming
    parquetSchema = MessageTypeParser.parseMessageType("message SchemaWithList {\n"
        + "  optional group list_field (LIST) {\n"
        + "    repeated group array {\n"
        + "      required int32 a;\n"
        + "    }\n"
        + "  }\n"
        + "}");
    avroSchema = new AvroSchemaConverter().convert(parquetSchema);

    assertThat(AvroRecordConverter.isElementType(
            parquetSchema.getType("list_field"),
            avroSchema.getFields().get(0).schema()))
        .isTrue();
  }

  @Test
  public void testIsElementTypeFailsInvalidSchema() throws Exception {
    Path test = writeDirect(
        "message MessageWithInvalidArraySchema {"
            + "  optional group locations (LIST) {"
            + "    repeated group array {"
            + "      optional group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("array", 0); // start writing array contents

          // write a non-null element
          rc.startGroup(); // array level
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup(); // array level

          rc.endField("array", 0); // finished writing array contents
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    Schema newSchema =
        record("MessageWithInvalidArraySchema", optionalField("locations", array(optional(location))));
    GenericRecord newRecord = instance(
        newSchema,
        "locations",
        Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    Configuration oldConfWithSchema = new Configuration();
    AvroReadSupport.setAvroReadSchema(oldConfWithSchema, newSchema);

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(oldConfWithSchema, test);
    assertThatThrownBy(reader::read)
        .isInstanceOf(InvalidRecordException.class)
        .hasMessage("Parquet/Avro schema mismatch. Avro field 'element' not found.");
  }

  @Test
  public void testAutoDetectThreeLevelListEncoding() throws Exception {
    Path test = writeDirect(
        "message AutoDetectThreeLevel {"
            + "  optional group locations (LIST) {"
            + "    repeated group list {"
            + "      required group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("locations", 0);

          rc.startGroup();
          rc.startField("list", 0);

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(0.0);
          rc.endField("longitude", 1);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("list", 0);
          rc.endGroup();

          rc.endField("locations", 0);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // without auto-detect, old behavior wraps repeated group in an extra "element" record,
    // e.g. {"locations": [{"element": {"latitude": 0.0, ... }}]}
    Schema elementRecord = record("list", field("element", location));
    Schema oldSchema = record("AutoDetectThreeLevel", optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(
        oldSchema,
        "locations",
        Arrays.asList(
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element", instance(location, "latitude", 0.0, "longitude", 0.0))));

    Configuration nonAutoDetectConf = new Configuration(OLD_BEHAVIOR_CONF);
    assertReaderContains(new AvroParquetReader<>(nonAutoDetectConf, test), oldSchema, oldRecord);

    // with auto-detect, repeated group is represented correctly,
    // e.g. {"locations": [{"latitude": 0.0, ... }]}
    Schema newSchema = record("AutoDetectThreeLevel", optionalField("locations", array(location)));
    GenericRecord newRecord = instance(
        newSchema,
        "locations",
        Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    assertReaderContains(autoDetectReader(test), newSchema, newRecord);
  }

  @Test
  public void testAutoDetectThreeLevelListEncodingWithProjection() throws Exception {
    Path test = writeDirect(
        "message AutoDetectThreeLevelProjection {"
            + "  required int64 year;"
            + "  optional group locations (LIST) {"
            + "    repeated group list {"
            + "      required group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("year", 0);
          rc.addLong(2010L);
          rc.endField("year", 0);

          rc.startField("locations", 1);

          rc.startGroup();
          rc.startField("list", 0);

          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("latitude", 0);
          rc.addDouble(0.0);
          rc.endField("latitude", 0);
          rc.startField("longitude", 1);
          rc.addDouble(180.0);
          rc.endField("longitude", 1);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("list", 0);
          rc.endGroup();

          rc.endField("locations", 1);
          rc.endMessage();
        });

    Schema location = record(
        "element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    Schema projectionSchema = record("AutoDetectThreeLevelProjection", optionalField("locations", array(location)));

    Configuration autoDetectConf = new Configuration(AUTO_DETECT_CONF);
    AvroReadSupport.setRequestedProjection(autoDetectConf, projectionSchema);

    GenericRecord expectedRecord = instance(
        projectionSchema, "locations", Arrays.asList(instance(location, "latitude", 0.0, "longitude", 180.0)));

    assertReaderContains(new AvroParquetReader<>(autoDetectConf, test), projectionSchema, expectedRecord);
  }

  @Test
  public void testAutoDetectMixedSiblingListStructuresUsesOldBehavior() {
    MessageType fileSchema = MessageTypeParser.parseMessageType("message MixedSiblingListStructures {"
        + "  optional group three_level (LIST) {"
        + "    repeated group list {"
        + "      required int32 element;"
        + "    }"
        + "  }"
        + "  optional group two_level (LIST) {"
        + "    repeated int32 element;"
        + "  }"
        + "}");

    assertListStructureIsNotAutoDetected(fileSchema);
  }

  @Test
  public void testAutoDetectMixedNestedListStructuresUsesOldBehavior() {
    MessageType fileSchema = MessageTypeParser.parseMessageType("message MixedNestedListStructures {"
        + "  optional group outer_list (LIST) {"
        + "    repeated group list {"
        + "      required group element (LIST) {"
        + "        repeated int32 element;"
        + "      }"
        + "    }"
        + "  }"
        + "}");

    assertListStructureIsNotAutoDetected(fileSchema);
  }

  @Test
  public void testAutoDetectMalformedThreeLevelListStructuresUsesOldBehavior() {
    for (String schema : Arrays.asList(
        "message RepeatedOuterList {"
            + "  repeated group items (LIST) {"
            + "    repeated group list {"
            + "      required int32 element;"
            + "    }"
            + "  }"
            + "}",
        "message NonRepeatedMiddleList {"
            + "  optional group items (LIST) {"
            + "    optional group list {"
            + "      required int32 element;"
            + "    }"
            + "  }"
            + "}",
        "message RepeatedListElement {"
            + "  optional group items (LIST) {"
            + "    repeated group list {"
            + "      repeated int32 element;"
            + "    }"
            + "  }"
            + "}")) {
      assertListStructureIsNotAutoDetected(MessageTypeParser.parseMessageType(schema));
    }
  }

  @Test
  public void testAutoDetectTwoLevelListWithArrayGroupName() throws Exception {
    // A 2-level list where the repeated group is named "array" (a standard
    // backward-compat name). The "array" name causes isElementType to always
    // treat the repeated group as the element, regardless of the list structure flag.
    Path test = writeDirect(
        "message TwoLevelListWithArrayGroup {"
            + "  optional group list (LIST) {"
            + "    repeated group array {"
            + "      required int32 str;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("list", 0);

          rc.startGroup();
          rc.startField("array", 0);

          rc.startGroup();
          rc.startField("str", 0);
          rc.addInteger(34);
          rc.endField("str", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("str", 0);
          rc.addInteger(35);
          rc.endField("str", 0);
          rc.endGroup();

          rc.endField("array", 0);
          rc.endGroup();

          rc.endField("list", 0);
          rc.endMessage();
        });

    // "array"-named group is always treated as the element type
    Schema elementRecord = record("array", field("str", primitive(Schema.Type.INT)));
    Schema expectedSchema = record("TwoLevelListWithArrayGroup", optionalField("list", array(elementRecord)));
    GenericRecord expectedRecord = instance(
        expectedSchema,
        "list",
        Arrays.asList(instance(elementRecord, "str", 34), instance(elementRecord, "str", 35)));

    // all three modes produce the same result
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(new AvroParquetReader<>(newAutoDetectConf(), test), expectedSchema, expectedRecord);
  }

  @Test
  public void testAutoDetectTwoLevelListWithArrayGroupAndElementChild() throws Exception {
    // A 2-level list where the repeated group is named "array" and its single
    // child is named "element". The "array" group name dominates: isElementType
    // treats the group as the element regardless of the child's name.
    Path test = writeDirect(
        "message TwoLevelListWithArrayGroupAndElementChild {"
            + "  optional group my_list (LIST) {"
            + "    repeated group array {"
            + "      required int32 element;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("my_list", 0);

          rc.startGroup();
          rc.startField("array", 0);

          rc.startGroup();
          rc.startField("element", 0);
          rc.addInteger(34);
          rc.endField("element", 0);
          rc.endGroup();

          rc.startGroup();
          rc.startField("element", 0);
          rc.addInteger(35);
          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("array", 0);
          rc.endGroup();

          rc.endField("my_list", 0);
          rc.endMessage();
        });

    // "array"-named group is always the element, even with child named "element"
    Schema elementRecord = record("array", field("element", primitive(Schema.Type.INT)));
    Schema expectedSchema =
        record("TwoLevelListWithArrayGroupAndElementChild", optionalField("my_list", array(elementRecord)));
    GenericRecord expectedRecord = instance(
        expectedSchema,
        "my_list",
        Arrays.asList(instance(elementRecord, "element", 34), instance(elementRecord, "element", 35)));

    // all three modes produce the same result
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(new AvroParquetReader<>(newAutoDetectConf(), test), expectedSchema, expectedRecord);
  }

  @Test
  public void testAutoDetectTwoLevelRepeatedPrimitive() throws Exception {
    // The most basic backward-compat rule: a repeated primitive inside a LIST
    // group is the element type directly, producing a required list of
    // non-nullable elements.
    Path test = writeDirect(
        "message TwoLevelRepeatedPrimitive {"
            + "  required group my_list (LIST) {"
            + "    repeated int32 element;"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("my_list", 0);

          rc.startGroup();
          rc.startField("element", 0);

          rc.addInteger(34);
          rc.addInteger(35);
          rc.addInteger(36);

          rc.endField("element", 0);
          rc.endGroup();

          rc.endField("my_list", 0);
          rc.endMessage();
        });

    Schema expectedSchema =
        record("TwoLevelRepeatedPrimitive", field("my_list", array(Schema.create(Schema.Type.INT))));
    GenericRecord expectedRecord = instance(expectedSchema, "my_list", Arrays.asList(34, 35, 36));

    // all three modes produce the same result for repeated primitives
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(new AvroParquetReader<>(newAutoDetectConf(), test), expectedSchema, expectedRecord);
  }

  @Test
  public void testProjectionReadsTwoLevelListOfPrimitives() throws Exception {
    // The projection is converted with the old list structure ("repeated binary array"), which
    // must follow the file's repeated field name to find the column.
    Path test = writeDirect(
        "message ProjectTwoLevelList {"
            + "  required int32 id;"
            + "  optional group tags (LIST) {"
            + "    repeated binary element (UTF8);"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("id", 0);
          rc.addInteger(1);
          rc.endField("id", 0);
          rc.startField("tags", 1);
          rc.startGroup();
          rc.startField("element", 0);
          rc.addBinary(Binary.fromString("a"));
          rc.addBinary(Binary.fromString("b"));
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("tags", 1);
          rc.endMessage();
        });

    Schema projection = record("ProjectTwoLevelList", optionalField("tags", array(primitive(Schema.Type.STRING))));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "tags", Arrays.asList(new Utf8("a"), new Utf8("b"))));
  }

  @Test
  public void testProjectionReadsSingleFieldGroupsWithOtherNames() throws Exception {
    // A synthetic group that is not named "list" or whose element is not named "element", such as
    // "bag" or the "list" / "item" names older PyArrow versions write
    for (String repeatedName : Arrays.asList("bag", "list")) {
      String elementName = repeatedName.equals("bag") ? "x" : "item";
      Path test = writeDirect(
          "message ProjectSingleFieldGroup {"
              + "  required int32 id;"
              + "  optional group tags (LIST) {"
              + "    repeated group " + repeatedName + " {"
              + "      optional binary " + elementName + " (UTF8);"
              + "    }"
              + "  }"
              + "}",
          rc -> {
            rc.startMessage();
            rc.startField("id", 0);
            rc.addInteger(1);
            rc.endField("id", 0);
            rc.startField("tags", 1);
            rc.startGroup();
            rc.startField(repeatedName, 0);
            rc.startGroup();
            rc.startField(elementName, 0);
            rc.addBinary(Binary.fromString("a"));
            rc.endField(elementName, 0);
            rc.endGroup();
            rc.startGroup(); // null element
            rc.endGroup();
            rc.endField(repeatedName, 0);
            rc.endGroup();
            rc.endField("tags", 1);
            rc.endMessage();
          });

      Schema projection = record(
          "ProjectSingleFieldGroup", optionalField("tags", array(optional(primitive(Schema.Type.STRING)))));

      assertReaderContains(
          projectionReader(test, projection),
          projection,
          instance(projection, "tags", Arrays.asList(new Utf8("a"), null)));
    }
  }

  @Test
  public void testProjectionReadsMixedListStructures() throws Exception {
    // A standard 3-level list next to a non-standard one disables auto-detection for the file
    Path test = writeDirect(
        "message ProjectMixedLists {"
            + "  optional group standard (LIST) {"
            + "    repeated group list {"
            + "      optional int32 element;"
            + "    }"
            + "  }"
            + "  optional group legacy (LIST) {"
            + "    repeated group list {"
            + "      optional int32 item;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("standard", 0);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("element", 0);
          rc.addInteger(1);
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("standard", 0);
          rc.startField("legacy", 1);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("item", 0);
          rc.addInteger(2);
          rc.endField("item", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("legacy", 1);
          rc.endMessage();
        });

    Schema ints = array(optional(primitive(Schema.Type.INT)));
    Schema projection = record("ProjectMixedLists", optionalField("standard", ints), optionalField("legacy", ints));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "standard", Arrays.asList(1), "legacy", Arrays.asList(2)));
  }

  @Test
  public void testProjectionPrunesRecordsInNonStandardList() throws Exception {
    Path test = writeDirect(
        "message ProjectRecordsInList {"
            + "  optional group points (LIST) {"
            + "    repeated group list {"
            + "      optional group item {"
            + "        required int32 x;"
            + "        required int32 y;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("points", 0);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("item", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addInteger(3);
          rc.endField("x", 0);
          rc.startField("y", 1);
          rc.addInteger(4);
          rc.endField("y", 1);
          rc.endGroup();
          rc.endField("item", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("points", 0);
          rc.endMessage();
        });

    Schema point = record("item", field("x", primitive(Schema.Type.INT)));
    Schema projection = record("ProjectRecordsInList", optionalField("points", array(optional(point))));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "points", Arrays.asList(instance(point, "x", 3))));
  }

  @Test
  public void testProjectionReadsMapWithOtherNames() throws Exception {
    Path test = writeDirect(
        "message ProjectMap {"
            + "  optional group counts (MAP_KEY_VALUE) {"
            + "    repeated group map {"
            + "      required binary key (UTF8);"
            + "      optional int32 value;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("counts", 0);
          rc.startGroup();
          rc.startField("map", 0);
          rc.startGroup();
          rc.startField("key", 0);
          rc.addBinary(Binary.fromString("a"));
          rc.endField("key", 0);
          rc.startField("value", 1);
          rc.addInteger(5);
          rc.endField("value", 1);
          rc.endGroup();
          rc.endField("map", 0);
          rc.endGroup();
          rc.endField("counts", 0);
          rc.endMessage();
        });

    Schema projection =
        record("ProjectMap", optionalField("counts", Schema.createMap(optional(primitive(Schema.Type.INT)))));
    Map<Utf8, Integer> counts = new HashMap<>();
    counts.put(new Utf8("a"), 5);

    assertReaderContains(projectionReader(test, projection), projection, instance(projection, "counts", counts));
  }

  @Test
  public void testProjectionStillChecksElementTypes() throws Exception {
    // The projected element keeps its own type, so a type that doesn't match the file still fails
    // the requested-schema check instead of reaching a converter.
    Path test = writeDirect(
        "message ProjectWrongElementType {"
            + "  optional group items (LIST) {"
            + "    repeated group list {"
            + "      optional int32 element;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("items", 0);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("element", 0);
          rc.addInteger(5);
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("items", 0);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectWrongElementType", optionalField("items", array(optional(primitive(Schema.Type.STRING)))));

    AvroParquetReader<GenericRecord> reader = projectionReader(test, projection);
    assertThatThrownBy(reader::read)
        .isInstanceOf(ParquetDecodingException.class)
        .cause()
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("incompatible types");
  }

  @Test
  public void testProjectionKeepsNonNullElementsRequired() throws Exception {
    // A non-null Avro element type is still requested as required, so optional elements in the file
    // fail the requested-schema check as they did before.
    Path test = writeDirect(
        "message ProjectRequiredElements {"
            + "  optional group items (LIST) {"
            + "    repeated group list {"
            + "      optional int32 item;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("items", 0);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("item", 0);
          rc.addInteger(5);
          rc.endField("item", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("items", 0);
          rc.endMessage();
        });

    Schema projection =
        record("ProjectRequiredElements", optionalField("items", array(primitive(Schema.Type.INT))));

    AvroParquetReader<GenericRecord> reader = projectionReader(test, projection);
    assertThatThrownBy(reader::read)
        .isInstanceOf(ParquetDecodingException.class)
        .cause()
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("incompatible types");
  }

  @Test
  public void testProjectionReadsNestedNonStandardLists() throws Exception {
    // A "list" / "item" list whose elements are 2-level lists
    Path test = writeDirect(
        "message ProjectNestedLists {"
            + "  optional group matrix (LIST) {"
            + "    repeated group list {"
            + "      optional group item (LIST) {"
            + "        repeated int32 element;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("matrix", 0);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("item", 0);
          rc.startGroup();
          rc.startField("element", 0);
          rc.addInteger(1);
          rc.addInteger(2);
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("item", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("matrix", 0);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectNestedLists", optionalField("matrix", array(optional(array(primitive(Schema.Type.INT))))));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "matrix", Arrays.asList(Arrays.asList(1, 2))));
  }

  @Test
  public void testProjectionReadsMapOfNonStandardLists() throws Exception {
    // A map whose values are "bag" lists
    Path test = writeDirect(
        "message ProjectMapOfLists {"
            + "  optional group lists (MAP) {"
            + "    repeated group key_value {"
            + "      required binary key (UTF8);"
            + "      optional group value (LIST) {"
            + "        repeated group bag {"
            + "          optional int32 x;"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("lists", 0);
          rc.startGroup();
          rc.startField("key_value", 0);
          rc.startGroup();
          rc.startField("key", 0);
          rc.addBinary(Binary.fromString("a"));
          rc.endField("key", 0);
          rc.startField("value", 1);
          rc.startGroup();
          rc.startField("bag", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addInteger(3);
          rc.endField("x", 0);
          rc.endGroup();
          rc.startGroup(); // null element
          rc.endGroup();
          rc.endField("bag", 0);
          rc.endGroup();
          rc.endField("value", 1);
          rc.endGroup();
          rc.endField("key_value", 0);
          rc.endGroup();
          rc.endField("lists", 0);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectMapOfLists",
        optionalField("lists", Schema.createMap(optional(array(optional(primitive(Schema.Type.INT)))))));
    Map<Utf8, Object> lists = new HashMap<>();
    lists.put(new Utf8("a"), Arrays.asList(3, null));

    assertReaderContains(projectionReader(test, projection), projection, instance(projection, "lists", lists));
  }

  @Test
  public void testProjectionOfTwoLevelRecordsFollowsTheReadSchema() throws Exception {
    // A 2-level list of records with one of their fields projected. The converters decide again
    // whether the pruned repeated group is the element, with the read schema.
    Path test = writeDirect(
        "message ProjectTwoLevelRecords {"
            + "  optional group points (LIST) {"
            + "    repeated group point {"
            + "      required int32 x;"
            + "      required int32 y;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("points", 0);
          rc.startGroup();
          rc.startField("point", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addInteger(1);
          rc.endField("x", 0);
          rc.startField("y", 1);
          rc.addInteger(2);
          rc.endField("y", 1);
          rc.endGroup();
          rc.endField("point", 0);
          rc.endGroup();
          rc.endField("points", 0);
          rc.endMessage();
        });

    Schema x = record("point", field("x", primitive(Schema.Type.INT)));
    Schema projection = record("ProjectTwoLevelRecords", optionalField("points", array(x)));

    // the read schema is the projection
    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "points", Arrays.asList(instance(x, "x", 1))));

    // the read schema adds a field with a default
    Schema.Field yWithDefault = new Schema.Field("y", primitive(Schema.Type.INT), null, 7);
    Schema xy = Schema.createRecord(
        "point", null, null, false, Arrays.asList(field("x", primitive(Schema.Type.INT)), yWithDefault));
    Schema readSchema = record("ProjectTwoLevelRecords", optionalField("points", array(xy)));
    assertReaderContains(
        projectionReader(test, projection, readSchema),
        readSchema,
        instance(readSchema, "points", Arrays.asList(instance(xy, "x", 1, "y", 7))));

    // the read schema requires a field the projection leaves out, so the pruned group would not be
    // read as the element: the list keeps the converted projection, as before the alignment
    Schema xyRequired =
        record("point", field("x", primitive(Schema.Type.INT)), field("y", primitive(Schema.Type.INT)));
    Schema requiredReadSchema = record("ProjectTwoLevelRecords", optionalField("points", array(xyRequired)));
    assertReaderContains(
        projectionReader(test, projection, requiredReadSchema),
        requiredReadSchema,
        instance(requiredReadSchema, "points", null));
  }

  @Test
  public void testProjectionIgnoresUnselectedColumnsAvroCannotRepresent() throws Exception {
    // A standard list whose element has a MAP with integer keys, which has no Avro schema; only x is selected
    Path test = writeDirect(
        "message ProjectWithIntKeyMap {"
            + "  optional group items (LIST) {"
            + "    repeated group list {"
            + "      optional group element {"
            + "        optional int32 x;"
            + "        optional group m (MAP) {"
            + "          repeated group key_value {"
            + "            required int32 key;"
            + "            optional int32 value;"
            + "          }"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("items", 0);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addInteger(1);
          rc.endField("x", 0);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("items", 0);
          rc.endMessage();
        });

    Schema element = record("element", optionalField("x", primitive(Schema.Type.INT)));
    Schema projection = record("ProjectWithIntKeyMap", optionalField("items", array(optional(element))));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "items", Arrays.asList(instance(element, "x", 1))));
  }

  @Test
  public void testProjectionStillChecksMapKeyTypes() throws Exception {
    Path test = writeDirect(
        "message ProjectFixedKeyMap {"
            + "  optional group counts (MAP) {"
            + "    repeated group map {"
            + "      required fixed_len_byte_array(1) key;"
            + "      optional int32 value;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("counts", 0);
          rc.startGroup();
          rc.startField("map", 0);
          rc.startGroup();
          rc.startField("key", 0);
          rc.addBinary(Binary.fromString("a"));
          rc.endField("key", 0);
          rc.startField("value", 1);
          rc.addInteger(5);
          rc.endField("value", 1);
          rc.endGroup();
          rc.endField("map", 0);
          rc.endGroup();
          rc.endField("counts", 0);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectFixedKeyMap", optionalField("counts", Schema.createMap(optional(primitive(Schema.Type.INT)))));

    AvroParquetReader<GenericRecord> reader = projectionReader(test, projection);
    assertThatThrownBy(reader::read)
        .isInstanceOf(ParquetDecodingException.class)
        .cause()
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("incompatible types");
  }

  @Test
  public void testProjectionWithoutAvroSchemaKeepsElementTypes() throws Exception {
    // No Avro schema in the file and no read schema: the records have the schema converted from the
    // projection, as before
    Path twoLevel = writeDirect(
        "message ProjectWithoutAvroSchema {"
            + "  optional group items (LIST) {"
            + "    repeated int32 array;"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("items", 0);
          rc.startGroup();
          rc.startField("array", 0);
          rc.addInteger(1);
          rc.addInteger(2);
          rc.endField("array", 0);
          rc.endGroup();
          rc.endField("items", 0);
          rc.endMessage();
        });
    Schema ints = record("ProjectWithoutAvroSchema", optionalField("items", array(primitive(Schema.Type.INT))));
    try (AvroParquetReader<GenericRecord> reader = requestedProjectionReader(twoLevel, ints)) {
      assertThat(reader.read().get("items")).isEqualTo(Arrays.asList(1, 2));
    }

    // A list of single-field groups, whose values a schema converted from the projection would read as
    // records
    Path legacy = writeDirect(
        "message ProjectWithoutAvroSchema {"
            + "  optional group items (LIST) {"
            + "    repeated group list {"
            + "      optional binary item (UTF8);"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("items", 0);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("item", 0);
          rc.addBinary(Binary.fromString("a"));
          rc.endField("item", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("items", 0);
          rc.endMessage();
        });
    Schema strings = record(
        "ProjectWithoutAvroSchema", optionalField("items", array(optional(primitive(Schema.Type.STRING)))));
    // The records have the schema converted from the projection, as before
    try (AvroParquetReader<GenericRecord> reader = requestedProjectionReader(legacy, strings)) {
      assertThat(String.valueOf(reader.read().get("items"))).isEqualTo("[a]");
    }
  }

  @Test
  public void testProjectionReadsListWrapperNamedLikeAnAvroType() throws Exception {
    // A wrapper may have any name, also one that isn't a valid name of an Avro record
    Path test = writeDirect(
        "message ProjectWrapperName {"
            + "  required int32 id;"
            + "  optional group tags (LIST) {"
            + "    repeated group string {"
            + "      optional binary element (UTF8);"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("id", 0);
          rc.addInteger(7);
          rc.endField("id", 0);
          rc.startField("tags", 1);
          rc.startGroup();
          rc.startField("string", 0);
          rc.startGroup();
          rc.startField("element", 0);
          rc.addBinary(Binary.fromString("a"));
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("string", 0);
          rc.endGroup();
          rc.endField("tags", 1);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectWrapperName",
        field("id", primitive(Schema.Type.INT)),
        optionalField("tags", array(optional(primitive(Schema.Type.STRING)))));

    try (AvroParquetReader<GenericRecord> reader = requestedProjectionReader(test, projection)) {
      GenericRecord record = reader.read();
      assertThat(record.get("id")).isEqualTo(7);
      assertThat(String.valueOf(record.get("tags"))).isEqualTo("[a]");
    }
  }

  @Test
  public void testProjectionReadsSingleFieldTupleAsElement() throws Exception {
    // The repeated group can't wrap the projected record, so it is the element itself. The converter
    // checks that with the record's name: the read schema's, or without one, the file's name of the
    // element, which the alignment gives the record converted from the projection.
    Path test = writeDirect(
        "message ProjectTuple {"
            + "  optional group points (LIST) {"
            + "    repeated group points_tuple {"
            + "      required int32 x;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("points", 0);
          rc.startGroup();
          rc.startField("points_tuple", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addInteger(1);
          rc.endField("x", 0);
          rc.endGroup();
          rc.endField("points_tuple", 0);
          rc.endGroup();
          rc.endField("points", 0);
          rc.endMessage();
        });

    Schema point = record("points_tuple", field("x", primitive(Schema.Type.INT)));
    Schema projection = record("ProjectTuple", optionalField("points", array(point)));
    GenericRecord expected = instance(projection, "points", Arrays.asList(instance(point, "x", 1)));

    assertReaderContains(projectionReader(test, projection), projection, expected);
    // Without a read schema, the record converted from the projection is the element too
    try (AvroParquetReader<GenericRecord> reader = requestedProjectionReader(test, projection)) {
      assertThat(String.valueOf(reader.read().get("points"))).isEqualTo("[{\"x\": 1}]");
    }
  }

  @Test
  public void testProjectionReadsSingleFieldTupleWithGroupField() throws Exception {
    // The repeated group is the element, and its one field is itself a group: aligning the group as a
    // wrapper of the element would add another x level
    Path test = writeDirect(
        "message ProjectTupleOfGroup {"
            + "  optional group points (LIST) {"
            + "    repeated group points_tuple {"
            + "      required group x {"
            + "        required int32 a;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("points", 0);
          rc.startGroup();
          rc.startField("points_tuple", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.startGroup();
          rc.startField("a", 0);
          rc.addInteger(1);
          rc.endField("a", 0);
          rc.endGroup();
          rc.endField("x", 0);
          rc.endGroup();
          rc.endField("points_tuple", 0);
          rc.endGroup();
          rc.endField("points", 0);
          rc.endMessage();
        });

    Schema x = record("x", field("a", primitive(Schema.Type.INT)));
    Schema point = record("points_tuple", field("x", x));
    Schema projection = record("ProjectTupleOfGroup", optionalField("points", array(point)));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "points", Arrays.asList(instance(point, "x", instance(x, "a", 1)))));
  }

  @Test
  public void testProjectionFollowsAliasesOfTheReadSchema() throws Exception {
    // The read schema renames the list, with the file's name as an alias, as the converters allow
    Path test = writeDirect(
        "message ProjectAlias {"
            + "  optional group items (LIST) {"
            + "    repeated group points_tuple {"
            + "      required int32 x;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("items", 0);
          rc.startGroup();
          rc.startField("points_tuple", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addInteger(1);
          rc.endField("x", 0);
          rc.endGroup();
          rc.endField("points_tuple", 0);
          rc.endGroup();
          rc.endField("items", 0);
          rc.endMessage();
        });

    Schema point = record("points_tuple", field("x", primitive(Schema.Type.INT)));
    Schema projection = record("ProjectAlias", optionalField("items", array(point)));
    Schema.Field renamed = optionalField("points", array(point));
    renamed.addAlias("items");
    Schema readSchema = record("ProjectAlias", renamed);

    assertReaderContains(
        projectionReader(test, projection, readSchema),
        readSchema,
        instance(readSchema, "points", Arrays.asList(instance(point, "x", 1))));
  }

  @Test
  public void testProjectionWithoutAvroSchemaKeepsTheConvertedSchema() throws Exception {
    // Without a read schema or an Avro schema in the file, the records have the schema converted from the
    // projection, as before: an ENUM column is a string, not an Avro enum
    Path test = writeDirect("message ProjectEnum {" + "  required binary kind (ENUM);" + "}", rc -> {
      rc.startMessage();
      rc.startField("kind", 0);
      rc.addBinary(Binary.fromString("A"));
      rc.endField("kind", 0);
      rc.endMessage();
    });

    Schema projection =
        record("ProjectEnum", field("kind", Schema.createEnum("Kind", null, null, Arrays.asList("A", "B"))));

    // The record converters, which read an Avro enum as one, unlike the compatible ones
    Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(conf, projection);
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(conf, test)) {
      Object kind = reader.read().get("kind");
      assertThat(kind).isNotInstanceOf(GenericEnumSymbol.class);
      assertThat(kind).isInstanceOf(CharSequence.class);
      assertThat(kind.toString()).isEqualTo("A");
    }
  }

  @Test
  public void testProjectionDoesNotReadListOfKeyValueRecordsAsMap() throws Exception {
    // A list of key-value records may repeat a key, which a map would lose, so a MAP projection doesn't
    // select it
    Path test = writeDirect(
        "message ProjectListAsMap {"
            + "  optional group counts (LIST) {"
            + "    repeated group pair {"
            + "      required binary key (UTF8);"
            + "      optional int32 value;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("counts", 0);
          rc.startGroup();
          rc.startField("pair", 0);
          for (int value : new int[] {1, 2}) {
            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("a"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(value);
            rc.endField("value", 1);
            rc.endGroup();
          }
          rc.endField("pair", 0);
          rc.endGroup();
          rc.endField("counts", 0);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectListAsMap", optionalField("counts", Schema.createMap(optional(primitive(Schema.Type.INT)))));

    try (AvroParquetReader<GenericRecord> reader = projectionReader(test, projection)) {
      assertThat(reader.read().get("counts")).isNull();
    }
  }

  @Test
  public void testProjectionReadsListWrapperNamedLikeAnAvroTypeForRecords() throws Exception {
    // A wrapper may have any name, also with a record element
    Path test = writeDirect(
        "message ProjectRecordWrapperName {"
            + "  required int32 id;"
            + "  optional group tags (LIST) {"
            + "    repeated group string {"
            + "      required group element {"
            + "        required int32 x;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("id", 0);
          rc.addInteger(7);
          rc.endField("id", 0);
          rc.startField("tags", 1);
          rc.startGroup();
          rc.startField("string", 0);
          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addInteger(1);
          rc.endField("x", 0);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("string", 0);
          rc.endGroup();
          rc.endField("tags", 1);
          rc.endMessage();
        });

    Schema element = record("element", field("x", primitive(Schema.Type.INT)));
    Schema projection = record(
        "ProjectRecordWrapperName",
        field("id", primitive(Schema.Type.INT)),
        optionalField("tags", array(element)));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "id", 7, "tags", Arrays.asList(instance(element, "x", 1))));
    try (AvroParquetReader<GenericRecord> reader = requestedProjectionReader(test, projection)) {
      GenericRecord record = reader.read();
      assertThat(record.get("id")).isEqualTo(7);
      assertThat(String.valueOf(record.get("tags"))).isEqualTo("[{\"x\": 1}]");
    }
  }

  @Test
  public void testProjectionDoesNotReadNestedListOfKeyValueRecordsAsMap() throws Exception {
    // The element of the outer list is a two-level LIST of key-value records named pair, whose paths
    // the MAP projection would match if it took the file's name, so it must not, as at the top level
    Path test = writeDirect(
        "message ProjectNestedListAsMap {"
            + "  optional group outer (LIST) {"
            + "    repeated group pair (LIST) {"
            + "      repeated group key_value {"
            + "        required binary key (UTF8);"
            + "        optional int32 value;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("outer", 0);
          rc.startGroup();
          rc.startField("pair", 0);
          rc.startGroup();
          rc.startField("key_value", 0);
          for (int value : new int[] {1, 2}) {
            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromString("a"));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(value);
            rc.endField("value", 1);
            rc.endGroup();
          }
          rc.endField("key_value", 0);
          rc.endGroup();
          rc.endField("pair", 0);
          rc.endGroup();
          rc.endField("outer", 0);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectNestedListAsMap",
        optionalField("outer", array(Schema.createMap(optional(primitive(Schema.Type.INT))))));

    try (AvroParquetReader<GenericRecord> reader = projectionReader(test, projection)) {
      Object outer = reader.read().get("outer");
      if (outer != null) {
        for (Object map : (Iterable<?>) outer) {
          assertThat(map)
              .as("Should not collapse the list into a map")
              .isNotInstanceOf(Map.class);
        }
      }
    }
  }

  @Test
  public void testProjectionReadsRepeatedListAsElementOfTwoLevelList() throws Exception {
    // A two-level list whose element is a repeated LIST, with an inner field name other than the projection's
    Path test = writeDirect(
        "message ProjectListOfRepeatedLists {"
            + "  optional group lists (LIST) {"
            + "    repeated group inner (LIST) {"
            + "      repeated int32 value;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("lists", 0);
          rc.startGroup();
          rc.startField("inner", 0);
          rc.startGroup();
          rc.startField("value", 0);
          rc.addInteger(1);
          rc.addInteger(2);
          rc.endField("value", 0);
          rc.endGroup();
          rc.startGroup();
          rc.startField("value", 0);
          rc.addInteger(3);
          rc.endField("value", 0);
          rc.endGroup();
          rc.endField("inner", 0);
          rc.endGroup();
          rc.endField("lists", 0);
          rc.endMessage();
        });

    Schema projection =
        record("ProjectListOfRepeatedLists", optionalField("lists", array(array(primitive(Schema.Type.INT)))));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "lists", Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3))));
    // Also without a read schema, with the schema converted from the projection
    assertReaderContains(
        requestedProjectionReader(test, projection),
        projection,
        instance(projection, "lists", Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3))));
  }

  @Test
  public void testProjectionReadsDeeplyNestedTwoLevelLists() throws Exception {
    // Each repeated LIST is the element of the list around it, which the alignment decides without
    // aligning it as a wrapper first, so the time it takes doesn't double with each level
    int depth = 25;
    StringBuilder parquetSchema = new StringBuilder("message ProjectNestedLists { optional group l0 (LIST) {");
    for (int level = 1; level <= depth; level++) {
      parquetSchema.append(" repeated group l").append(level).append(" (LIST) {");
    }
    parquetSchema.append(" repeated int32 value;");
    for (int level = 0; level <= depth; level++) {
      parquetSchema.append(" }");
    }
    parquetSchema.append(" }");
    Path test = writeDirect(parquetSchema.toString(), rc -> {
      rc.startMessage();
      rc.startField("l0", 0);
      for (int level = 1; level <= depth; level++) {
        rc.startGroup();
        rc.startField("l" + level, 0);
      }
      rc.startGroup();
      rc.startField("value", 0);
      rc.addInteger(1);
      rc.endField("value", 0);
      rc.endGroup();
      for (int level = depth; level >= 1; level--) {
        rc.endField("l" + level, 0);
        rc.endGroup();
      }
      rc.endField("l0", 0);
      rc.endMessage();
    });

    Schema element = primitive(Schema.Type.INT);
    Object value = 1;
    for (int level = 0; level <= depth; level++) {
      element = array(element);
      value = Arrays.asList(value);
    }
    Schema projection = record("ProjectNestedLists", optionalField("l0", element));
    Object expected = value;
    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      assertReaderContains(projectionReader(test, projection), projection, instance(projection, "l0", expected));
      assertReaderContains(
          requestedProjectionReader(test, projection), projection, instance(projection, "l0", expected));
    });
  }

  @Test
  public void testProjectionReadsStandardListOfRecordNamedLikeItsRepeatedGroup() throws Exception {
    // The element record is named list, like the repeated group, and its field element is named like
    // the group's field. The repeated group still wraps the element: its field element is a group, which
    // the record's int field can't select.
    Path test = writeDirect(
        "message ProjectRecordNamedList {"
            + "  optional group items (LIST) {"
            + "    repeated group list {"
            + "      required group element {"
            + "        required int32 element;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("items", 0);
          rc.startGroup();
          rc.startField("list", 0);
          rc.startGroup();
          rc.startField("element", 0);
          rc.startGroup();
          rc.startField("element", 0);
          rc.addInteger(1);
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("element", 0);
          rc.endGroup();
          rc.endField("list", 0);
          rc.endGroup();
          rc.endField("items", 0);
          rc.endMessage();
        });

    Schema element = record("list", field("element", primitive(Schema.Type.INT)));
    Schema projection = record("ProjectRecordNamedList", optionalField("items", array(element)));

    assertReaderContains(
        projectionReader(test, projection),
        projection,
        instance(projection, "items", Arrays.asList(instance(element, "element", 1))));
  }

  @Test
  public void testProjectionKeepsTwoLevelListThatTheReadElementRecordCanNotRead() throws Exception {
    // With write-old-list-structure=false, the schema converted from the string-array projection reads
    // each element as a record that wraps the string. The bag of strings in the file has no such record,
    // so the list keeps the converted projection, as before, and the other fields read.
    Path test = writeDirect(
        "message ProjectBagWithNewStructure {"
            + "  required int32 id;"
            + "  optional group tags (LIST) {"
            + "    repeated group bag {"
            + "      optional binary x (UTF8);"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("id", 0);
          rc.addInteger(7);
          rc.endField("id", 0);
          rc.startField("tags", 1);
          rc.startGroup();
          rc.startField("bag", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addBinary(Binary.fromString("a"));
          rc.endField("x", 0);
          rc.endGroup();
          rc.endField("bag", 0);
          rc.endGroup();
          rc.endField("tags", 1);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectBagWithNewStructure",
        field("id", primitive(Schema.Type.INT)),
        optionalField("tags", array(optional(primitive(Schema.Type.STRING)))));

    Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(conf, projection);
    conf.setBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, false);
    try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(conf, test)) {
      GenericRecord record = reader.read();
      assertThat(record.get("id")).isEqualTo(7);
      assertThat(record.get("tags")).isNull();
    }
  }

  @Test
  public void testProjectionWithoutAvroSchemaReadsRecordsWithTheSameFullName() throws Exception {
    // The schema converted from this projection has two records named foo2.foo: the root and the second
    // nested group named foo. Avro would write the second one as a reference to the root, so the read
    // must not go through the schema's JSON.
    Path test = writeDirect(
        "message foo2.foo {"
            + "  optional group a {"
            + "    optional group foo {"
            + "      optional int32 x;"
            + "    }"
            + "  }"
            + "  optional group b {"
            + "    optional group foo {"
            + "      optional int32 y;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("a", 0);
          rc.startGroup();
          rc.startField("foo", 0);
          rc.startGroup();
          rc.startField("x", 0);
          rc.addInteger(1);
          rc.endField("x", 0);
          rc.endGroup();
          rc.endField("foo", 0);
          rc.endGroup();
          rc.endField("a", 0);
          rc.startField("b", 1);
          rc.startGroup();
          rc.startField("foo", 0);
          rc.startGroup();
          rc.startField("y", 0);
          rc.addInteger(2);
          rc.endField("y", 0);
          rc.endGroup();
          rc.endField("foo", 0);
          rc.endGroup();
          rc.endField("b", 1);
          rc.endMessage();
        });

    Schema a = record("A", optionalField("foo", record("FooX", optionalField("x", primitive(Schema.Type.INT)))));
    Schema b = record("B", optionalField("foo", record("FooY", optionalField("y", primitive(Schema.Type.INT)))));
    Schema projection = Schema.createRecord(
        "foo", null, "foo2", false, Arrays.asList(optionalField("a", a), optionalField("b", b)));

    try (AvroParquetReader<GenericRecord> reader = requestedProjectionReader(test, projection)) {
      GenericRecord record = reader.read();
      assertThat(String.valueOf(record.get("a"))).isEqualTo("{\"foo\": {\"x\": 1}}");
      assertThat(String.valueOf(record.get("b"))).isEqualTo("{\"foo\": {\"y\": 2}}");
    }
  }

  @Test
  public void testProjectionDoesNotReadMapWithUnannotatedBinaryKeys() throws Exception {
    // A map's keys are strings in Avro. Two different byte keys that aren't UTF-8 would decode to the same
    // string, and one entry would overwrite the other, so a MAP projection doesn't select a legacy map
    // whose keys aren't annotated as strings.
    Path test = writeDirect(
        "message ProjectByteKeys {"
            + "  optional group counts (MAP) {"
            + "    repeated group map (MAP_KEY_VALUE) {"
            + "      required binary key;"
            + "      optional int32 value;"
            + "    }"
            + "  }"
            + "}",
        rc -> {
          rc.startMessage();
          rc.startField("counts", 0);
          rc.startGroup();
          rc.startField("map", 0);
          byte value = 1;
          for (byte key : new byte[] {(byte) 0xfe, (byte) 0xff}) {
            rc.startGroup();
            rc.startField("key", 0);
            rc.addBinary(Binary.fromConstantByteArray(new byte[] {key}));
            rc.endField("key", 0);
            rc.startField("value", 1);
            rc.addInteger(value++);
            rc.endField("value", 1);
            rc.endGroup();
          }
          rc.endField("map", 0);
          rc.endGroup();
          rc.endField("counts", 0);
          rc.endMessage();
        });

    Schema projection = record(
        "ProjectByteKeys", optionalField("counts", Schema.createMap(optional(primitive(Schema.Type.INT)))));

    try (AvroParquetReader<GenericRecord> reader = projectionReader(test, projection)) {
      assertThat(reader.read().get("counts")).isNull();
    }
    try (AvroParquetReader<GenericRecord> reader = requestedProjectionReader(test, projection)) {
      assertThat(reader.read().get("counts")).isNull();
    }
  }

  private <T extends IndexedRecord> AvroParquetReader<T> projectionReader(Path path, Schema projection)
      throws IOException {
    Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(conf, projection);
    AvroReadSupport.setAvroReadSchema(conf, projection);
    return new AvroParquetReader<>(conf, path);
  }

  private <T extends IndexedRecord> AvroParquetReader<T> requestedProjectionReader(Path path, Schema projection)
      throws IOException {
    Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(conf, projection);
    return new AvroParquetReader<>(conf, path);
  }

  private <T extends IndexedRecord> AvroParquetReader<T> projectionReader(
      Path path, Schema projection, Schema readSchema) throws IOException {
    Configuration conf = new Configuration();
    AvroReadSupport.setRequestedProjection(conf, projection);
    AvroReadSupport.setAvroReadSchema(conf, readSchema);
    return new AvroParquetReader<>(conf, path);
  }

  private static Configuration newAutoDetectConf() {
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AUTO_DETECT_LIST_STRUCTURE, true);
    return conf;
  }

  private static void assertListStructureIsNotAutoDetected(MessageType fileSchema) {
    Schema projection = record("Projection", field("items", array(primitive(Schema.Type.INT))));
    MessageType expectedProjection = new AvroSchemaConverter().convert(projection);
    Configuration conf = newAutoDetectConf();
    AvroReadSupport.setRequestedProjection(conf, projection);

    assertThat(new AvroReadSupport<>()
            .init(conf, new HashMap<>(), fileSchema)
            .getRequestedSchema())
        .as("Should not infer the new list structure for %s", fileSchema.getName())
        .isEqualTo(expectedProjection);
    assertThat(conf.get(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE)).isNull();
    assertThat(conf.get(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS)).isNull();
  }

  public <T extends IndexedRecord> AvroParquetReader<T> autoDetectReader(Path path) throws IOException {
    return new AvroParquetReader<T>(AUTO_DETECT_CONF, path);
  }

  public <T extends IndexedRecord> AvroParquetReader<T> oldBehaviorReader(Path path) throws IOException {
    return new AvroParquetReader<T>(OLD_BEHAVIOR_CONF, path);
  }

  public <T extends IndexedRecord> AvroParquetReader<T> oldBehaviorReader(Path path, Schema expectedSchema)
      throws IOException {
    Configuration conf = new Configuration(OLD_BEHAVIOR_CONF);
    AvroReadSupport.setAvroReadSchema(conf, expectedSchema);
    return new AvroParquetReader<T>(conf, path);
  }

  public <T extends IndexedRecord> AvroParquetReader<T> newBehaviorReader(Path path) throws IOException {
    return new AvroParquetReader<T>(NEW_BEHAVIOR_CONF, path);
  }

  public <T extends IndexedRecord> AvroParquetReader<T> newBehaviorReader(Path path, Schema expectedSchema)
      throws IOException {
    Configuration conf = new Configuration(NEW_BEHAVIOR_CONF);
    AvroReadSupport.setAvroReadSchema(conf, expectedSchema);
    return new AvroParquetReader<T>(conf, path);
  }

  public <T extends IndexedRecord> void assertReaderContains(
      AvroParquetReader<T> reader, Schema expectedSchema, T... expectedRecords) throws IOException {
    for (T expectedRecord : expectedRecords) {
      T actualRecord = reader.read();
      assertThat(actualRecord.getSchema())
          .as("Should match expected schema")
          .isEqualTo(expectedSchema);
      assertThat(actualRecord).as("Should match the expected record").isEqualTo(expectedRecord);
    }
    assertThat(reader.read())
        .as("Should only contain " + expectedRecords.length + " record"
            + (expectedRecords.length == 1 ? "" : "s"))
        .isNull();
  }
}
