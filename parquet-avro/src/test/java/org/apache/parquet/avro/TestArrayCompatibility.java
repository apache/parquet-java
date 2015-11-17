/**
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import static org.apache.parquet.avro.AvroTestUtil.array;
import static org.apache.parquet.avro.AvroTestUtil.field;
import static org.apache.parquet.avro.AvroTestUtil.instance;
import static org.apache.parquet.avro.AvroTestUtil.optional;
import static org.apache.parquet.avro.AvroTestUtil.optionalField;
import static org.apache.parquet.avro.AvroTestUtil.primitive;
import static org.apache.parquet.avro.AvroTestUtil.record;

public class TestArrayCompatibility {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  public static final Configuration NEW_BEHAVIOR_CONF = new Configuration();

  @BeforeClass
  public static void setupNewBehaviorConfiguration() {
    NEW_BEHAVIOR_CONF.setBoolean(
        AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);
  }

  @Test
  @Ignore(value="Not yet supported")
  public void testUnannotatedListOfPrimitives() throws Exception {
    Path test = writeDirect(
        "message UnannotatedListOfPrimitives {" +
            "  repeated int32 list_of_ints;" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.startField("list_of_ints", 0);

            rc.addInteger(34);
            rc.addInteger(35);
            rc.addInteger(36);

            rc.endField("list_of_ints", 0);
            rc.endMessage();
          }
        });

    Schema expectedSchema = record("OldPrimitiveInList",
        field("list_of_ints", array(primitive(Schema.Type.INT))));

    GenericRecord expectedRecord = instance(expectedSchema,
        "list_of_ints", Arrays.asList(34, 35, 36));

    // both should behave the same way
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  @Ignore(value="Not yet supported")
  public void testUnannotatedListOfGroups() throws Exception {
    Path test = writeDirect(
        "message UnannotatedListOfGroups {" +
            "  repeated group list_of_points {" +
            "    required float x;" +
            "    required float y;" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema point = record("?",
        field("x", primitive(Schema.Type.FLOAT)),
        field("y", primitive(Schema.Type.FLOAT)));
    Schema expectedSchema = record("OldPrimitiveInList",
        field("list_of_points", array(point)));

    GenericRecord expectedRecord = instance(expectedSchema,
        "list_of_points", Arrays.asList(
            instance(point, "x", 1.0f, "y", 1.0f),
            instance(point, "x", 2.0f, "y", 2.0f)));

    // both should behave the same way
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  public void testRepeatedPrimitiveInList() throws Exception {
    Path test = writeDirect(
        "message RepeatedPrimitiveInList {" +
            "  required group list_of_ints (LIST) {" +
            "    repeated int32 array;" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema expectedSchema = record("RepeatedPrimitiveInList",
        field("list_of_ints", array(Schema.create(Schema.Type.INT))));

    GenericRecord expectedRecord = instance(expectedSchema,
        "list_of_ints", Arrays.asList(34, 35, 36));

    // both should behave the same way
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  public void testMultiFieldGroupInList() throws Exception {
    // tests the missing element layer, detected by a multi-field group
    Path test = writeDirect(
        "message MultiFieldGroupInList {" +
            "  optional group locations (LIST) {" +
            "    repeated group element {" +
            "      required double latitude;" +
            "      required double longitude;" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));
    Schema expectedSchema = record("MultiFieldGroupInList",
        optionalField("locations", array(location)));

    GenericRecord expectedRecord = instance(expectedSchema,
        "locations", Arrays.asList(
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
        "message SingleFieldGroupInList {" +
            "  optional group single_element_groups (LIST) {" +
            "    repeated group single_element_group {" +
            "      required int64 count;" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    // can't tell from storage whether this should be a list of single-field
    // records or if the single_field_group layer is synthetic.

    // old behavior - assume that the repeated type is the element type
    Schema singleElementGroupSchema = record("single_element_group",
        field("count", primitive(Schema.Type.LONG)));
    Schema oldSchema = record("SingleFieldGroupInList",
        optionalField("single_element_groups", array(singleElementGroupSchema)));
    GenericRecord oldRecord = instance(oldSchema,
        "single_element_groups", Arrays.asList(
            instance(singleElementGroupSchema, "count", 1234L),
            instance(singleElementGroupSchema, "count", 2345L)));

    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    // new behavior - assume that single_element_group is synthetic (in spec)
    Schema newSchema = record("SingleFieldGroupInList",
        optionalField("single_element_groups", array(primitive(Schema.Type.LONG))));
    GenericRecord newRecord = instance(newSchema,
        "single_element_groups", Arrays.asList(1234L, 2345L));

    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);
  }

  @Test
  public void testSingleFieldGroupInListWithSchema() throws Exception {
    // this tests the case where older data has an ambiguous structure, but the
    // correct interpretation can be determined from the avro schema

    Schema singleElementRecord = record("single_element_group",
        field("count", primitive(Schema.Type.LONG)));

    Schema expectedSchema = record("SingleFieldGroupInList",
        optionalField("single_element_groups",
            array(singleElementRecord)));

    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(AvroWriteSupport.AVRO_SCHEMA, expectedSchema.toString());

    Path test = writeDirect(
        "message SingleFieldGroupInList {" +
            "  optional group single_element_groups (LIST) {" +
            "    repeated group single_element_group {" +
            "      required int64 count;" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        },
        metadata);

    GenericRecord expectedRecord = instance(expectedSchema,
        "single_element_groups", Arrays.asList(
            instance(singleElementRecord, "count", 1234L),
            instance(singleElementRecord, "count", 2345L)));

    // both should behave the same way because the schema is present
    assertReaderContains(oldBehaviorReader(test), expectedSchema, expectedRecord);
    assertReaderContains(newBehaviorReader(test), expectedSchema, expectedRecord);
  }

  @Test
  public void testNewOptionalGroupInList() throws Exception {
    Path test = writeDirect(
        "message NewOptionalGroupInList {" +
            "  optional group locations (LIST) {" +
            "    repeated group list {" +
            "      optional group element {" +
            "        required double latitude;" +
            "        required double longitude;" +
            "      }" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("list", optionalField("element", location));
    Schema oldSchema = record("NewOptionalGroupInList",
        optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(oldSchema,
        "locations", Arrays.asList(
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 0.0)),
            instance(elementRecord),
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 180.0))));

    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    // new behavior - assume that single_element_group is synthetic (in spec)
    Schema newSchema = record("NewOptionalGroupInList",
        optionalField("locations", array(optional(location))));
    GenericRecord newRecord = instance(newSchema,
        "locations", Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 0.0),
            null,
            instance(location, "latitude", 0.0, "longitude", 180.0)));

    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);
  }

  @Test
  public void testNewRequiredGroupInList() throws Exception {
    Path test = writeDirect(
        "message NewRequiredGroupInList {" +
            "  optional group locations (LIST) {" +
            "    repeated group list {" +
            "      required group element {" +
            "        required double latitude;" +
            "        required double longitude;" +
            "      }" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("list", field("element", location));
    Schema oldSchema = record("NewRequiredGroupInList",
        optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(oldSchema,
        "locations", Arrays.asList(
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 0.0))));

    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    // new behavior - assume that single_element_group is synthetic (in spec)
    Schema newSchema = record("NewRequiredGroupInList",
        optionalField("locations", array(location)));
    GenericRecord newRecord = instance(newSchema,
        "locations", Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);
  }

  @Test
  public void testAvroCompatOptionalGroupInList() throws Exception {
    Path test = writeDirect(
        "message AvroCompatOptionalGroupInList {" +
            "  optional group locations (LIST) {" +
            "    repeated group array {" +
            "      optional group element {" +
            "        required double latitude;" +
            "        required double longitude;" +
            "      }" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("array", optionalField("element", location));
    Schema oldSchema = record("AvroCompatOptionalGroupInList",
        optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(oldSchema,
        "locations", Arrays.asList(
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 0.0))));

    // both should detect the "array" name
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);
    assertReaderContains(newBehaviorReader(test), oldSchema, oldRecord);
  }

  @Test
  public void testAvroCompatOptionalGroupInListWithSchema() throws Exception {
    Path test = writeDirect(
        "message AvroCompatOptionalGroupInListWithSchema {" +
            "  optional group locations (LIST) {" +
            "    repeated group array {" +
            "      optional group element {" +
            "        required double latitude;" +
            "        required double longitude;" +
            "      }" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    Schema newSchema = record("AvroCompatOptionalGroupInListWithSchema",
        optionalField("locations", array(optional(location))));
    GenericRecord newRecord = instance(newSchema,
        "locations", Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    Configuration oldConfWithSchema = new Configuration();
    AvroReadSupport.setAvroReadSchema(oldConfWithSchema, newSchema);

    // both should use the schema structure that is provided
    assertReaderContains(
        new AvroParquetReader<GenericRecord>(oldConfWithSchema, test),
        newSchema, newRecord);

    Configuration newConfWithSchema = new Configuration(NEW_BEHAVIOR_CONF);
    AvroReadSupport.setAvroReadSchema(newConfWithSchema, newSchema);

    assertReaderContains(
        new AvroParquetReader<GenericRecord>(newConfWithSchema, test),
        newSchema, newRecord);
  }

  @Test
  public void testAvroCompatListInList() throws Exception {
    Path test = writeDirect(
        "message AvroCompatListInList {" +
            "  optional group listOfLists (LIST) {" +
            "    repeated group array (LIST) {" +
            "      repeated int32 array;" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema listOfLists = array(array(primitive(Schema.Type.INT)));
    Schema oldSchema = record("AvroCompatListInList",
        optionalField("listOfLists", listOfLists));

    GenericRecord oldRecord = instance(oldSchema,
        "listOfLists", Arrays.asList(
            Arrays.asList(34, 35, 36),
            Arrays.asList(),
            Arrays.asList(32, 33, 34)));

    // both should detect the "array" name
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);
    assertReaderContains(newBehaviorReader(test), oldSchema, oldRecord);
  }

  @Test
  public void testThriftCompatListInList() throws Exception {
    Path test = writeDirect(
        "message ThriftCompatListInList {" +
            "  optional group listOfLists (LIST) {" +
            "    repeated group listOfLists_tuple (LIST) {" +
            "      repeated int32 listOfLists_tuple_tuple;" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema listOfLists = array(array(primitive(Schema.Type.INT)));
    Schema oldSchema = record("ThriftCompatListInList",
        optionalField("listOfLists", listOfLists));

    GenericRecord oldRecord = instance(oldSchema,
        "listOfLists", Arrays.asList(
            Arrays.asList(34, 35, 36),
            Arrays.asList(),
            Arrays.asList(32, 33, 34)));

    // both should detect the "_tuple" names
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);
    assertReaderContains(newBehaviorReader(test), oldSchema, oldRecord);
  }

  @Test
  public void testThriftCompatRequiredGroupInList() throws Exception {
    Path test = writeDirect(
        "message ThriftCompatRequiredGroupInList {" +
            "  optional group locations (LIST) {" +
            "    repeated group locations_tuple {" +
            "      optional group element {" +
            "        required double latitude;" +
            "        required double longitude;" +
            "      }" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("locations_tuple", optionalField("element", location));
    Schema oldSchema = record("ThriftCompatRequiredGroupInList",
        optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(oldSchema,
        "locations", Arrays.asList(
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 0.0))));

    // both should detect the "array" name
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);
    assertReaderContains(newBehaviorReader(test), oldSchema, oldRecord);
  }

  @Test
  public void testHiveCompatOptionalGroupInList() throws Exception {
    Path test = writeDirect(
        "message HiveCompatOptionalGroupInList {" +
            "  optional group locations (LIST) {" +
            "    repeated group bag {" +
            "      optional group element {" +
            "        required double latitude;" +
            "        required double longitude;" +
            "      }" +
            "    }" +
            "  }" +
            "}",
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
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
          }
        });

    Schema location = record("element",
        field("latitude", primitive(Schema.Type.DOUBLE)),
        field("longitude", primitive(Schema.Type.DOUBLE)));

    // old behavior - assume that the repeated type is the element type
    Schema elementRecord = record("bag", optionalField("element", location));
    Schema oldSchema = record("HiveCompatOptionalGroupInList",
        optionalField("locations", array(elementRecord)));
    GenericRecord oldRecord = instance(oldSchema,
        "locations", Arrays.asList(
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 180.0)),
            instance(elementRecord, "element",
                instance(location, "latitude", 0.0, "longitude", 0.0))));

    // both should detect the "array" name
    assertReaderContains(oldBehaviorReader(test), oldSchema, oldRecord);

    Schema newSchema = record("HiveCompatOptionalGroupInList",
        optionalField("locations", array(optional(location))));
    GenericRecord newRecord = instance(newSchema,
        "locations", Arrays.asList(
            instance(location, "latitude", 0.0, "longitude", 180.0),
            instance(location, "latitude", 0.0, "longitude", 0.0)));

    assertReaderContains(newBehaviorReader(test), newSchema, newRecord);
  }

  private interface DirectWriter {
    public void write(RecordConsumer consumer);
  }

  private static class DirectWriteSupport extends WriteSupport<Void> {
    private RecordConsumer recordConsumer;
    private final MessageType type;
    private final DirectWriter writer;
    private final Map<String, String> metadata;

    private DirectWriteSupport(MessageType type, DirectWriter writer,
                               Map<String, String> metadata) {
      this.type = type;
      this.writer = writer;
      this.metadata = metadata;
    }

    @Override
    public WriteContext init(Configuration configuration) {
      return new WriteContext(type, metadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Void record) {
      writer.write(recordConsumer);
    }
  }

  private Path writeDirect(String type, DirectWriter writer) throws IOException {
    return writeDirect(MessageTypeParser.parseMessageType(type), writer);
  }

  private Path writeDirect(String type, DirectWriter writer,
                           Map<String, String> metadata) throws IOException {
    return writeDirect(MessageTypeParser.parseMessageType(type), writer, metadata);
  }

  private Path writeDirect(MessageType type, DirectWriter writer) throws IOException {
    return writeDirect(type, writer, new HashMap<String, String>());
  }

  private Path writeDirect(MessageType type, DirectWriter writer,
                           Map<String, String> metadata) throws IOException {
    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();

    Path path = new Path(temp.getPath());

    ParquetWriter<Void> parquetWriter = new ParquetWriter<Void>(
        path, new DirectWriteSupport(type, writer, metadata));
    parquetWriter.write(null);
    parquetWriter.close();

    return path;
  }

  public <T extends IndexedRecord> AvroParquetReader<T> oldBehaviorReader(
      Path path) throws IOException {
    return new AvroParquetReader<T>(path);
  }

  public <T extends IndexedRecord> AvroParquetReader<T> newBehaviorReader(
      Path path) throws IOException {
    return new AvroParquetReader<T>(NEW_BEHAVIOR_CONF, path);
  }

  public <T extends IndexedRecord> void assertReaderContains(
      AvroParquetReader<T> reader, Schema expectedSchema, T... expectedRecords)
      throws IOException {
    for (T expectedRecord : expectedRecords) {
      T actualRecord = reader.read();
      Assert.assertEquals("Should match expected schema",
          expectedSchema, actualRecord.getSchema());
      Assert.assertEquals("Should match the expected record",
          expectedRecord, actualRecord);
    }
    Assert.assertNull("Should only contain " + expectedRecords.length +
            " record" + (expectedRecords.length == 1 ? "" : "s"),
        reader.read());
  }

}
