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
package org.apache.parquet.hadoop.thrift;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.DirectWriterTest;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.thrift.ThriftParquetReader;
import org.apache.parquet.thrift.ThriftRecordConverter;
import org.apache.parquet.thrift.test.compat.ListOfCounts;
import org.apache.parquet.thrift.test.compat.ListOfInts;
import org.apache.parquet.thrift.test.compat.ListOfLists;
import org.apache.parquet.thrift.test.compat.ListOfLocations;
import org.apache.parquet.thrift.test.compat.ListOfSingleElementGroups;
import org.apache.parquet.thrift.test.compat.Location;
import org.apache.parquet.thrift.test.compat.SingleElementGroup;
import org.apache.thrift.TBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestArrayCompatibility extends DirectWriterTest {

  @Test
  @Ignore("Not yet supported")
  public void testUnannotatedListOfPrimitives() throws Exception {
    Path test = writeDirect(
        "message UnannotatedListOfPrimitives {" + "  repeated int32 list_of_ints;" + "}", new DirectWriter() {
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
  }

  @Test
  @Ignore("Not yet supported")
  public void testUnannotatedListOfGroups() throws Exception {
    Path test = writeDirect(
        "message UnannotatedListOfGroups {" + "  repeated group list_of_points {"
            + "    required float x;"
            + "    required float y;"
            + "  }"
            + "}",
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
  }

  @Test
  public void testRepeatedPrimitiveInList() throws Exception {
    Path test = writeDirect(
        "message RepeatedPrimitiveInList {" + "  required group list_of_ints (LIST) {"
            + "    repeated int32 array;"
            + "  }"
            + "}",
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

    ListOfInts expected = new ListOfInts(Lists.newArrayList(34, 35, 36));
    ListOfInts actual = reader(test, ListOfInts.class).read();
    Assert.assertEquals("Should read record correctly", expected, actual);
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

    ListOfLocations expected = new ListOfLocations();
    expected.addToLocations(new Location(0.0, 0.0));
    expected.addToLocations(new Location(0.0, 180.0));

    assertReaderContains(reader(test, ListOfLocations.class), expected);
  }

  @Test
  public void testSingleFieldGroupInList() throws Exception {
    // this tests the case where older data has an ambiguous structure, but the
    // correct interpretation can be determined from the thrift class

    Path test = writeDirect(
        "message SingleFieldGroupInList {" + "  optional group single_element_groups (LIST) {"
            + "    repeated group single_element_group {"
            + "      required int64 count;"
            + "    }"
            + "  }"
            + "}",
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

    // the behavior in this case depends on the thrift class used to read

    // test a class with the extra single_element_group level
    ListOfSingleElementGroups expectedOldBehavior = new ListOfSingleElementGroups();
    expectedOldBehavior.addToSingle_element_groups(new SingleElementGroup(1234L));
    expectedOldBehavior.addToSingle_element_groups(new SingleElementGroup(2345L));

    assertReaderContains(reader(test, ListOfSingleElementGroups.class), expectedOldBehavior);

    // test a class without the extra level
    ListOfCounts expectedNewBehavior = new ListOfCounts();
    expectedNewBehavior.addToSingle_element_groups(1234L);
    expectedNewBehavior.addToSingle_element_groups(2345L);

    assertReaderContains(reader(test, ListOfCounts.class), expectedNewBehavior);
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

    ListOfLocations expected = new ListOfLocations();
    expected.addToLocations(new Location(0.0, 0.0));
    // null is not included because thrift does not allow null in lists
    // expected.addToLocations(null);
    expected.addToLocations(new Location(0.0, 180.0));

    try {
      assertReaderContains(reader(test, ListOfLocations.class), expected);
      fail("Should fail: locations are optional and not ignored");
    } catch (RuntimeException e) {
      // e is a RuntimeException wrapping the decoding exception
      assertTrue(e.getCause().getCause().getMessage().contains("locations"));
    }

    assertReaderContains(readerIgnoreNulls(test, ListOfLocations.class), expected);
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

    ListOfLocations expected = new ListOfLocations();
    expected.addToLocations(new Location(0.0, 180.0));
    expected.addToLocations(new Location(0.0, 0.0));

    assertReaderContains(reader(test, ListOfLocations.class), expected);
  }

  @Test
  public void testAvroCompatRequiredGroupInList() throws Exception {
    Path test = writeDirect(
        "message AvroCompatRequiredGroupInList {" + "  optional group locations (LIST) {"
            + "    repeated group array {"
            + "      required group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
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
            rc.addDouble(90.0);
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
            rc.addDouble(-90.0);
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

    ListOfLocations expected = new ListOfLocations();
    expected.addToLocations(new Location(90.0, 180.0));
    expected.addToLocations(new Location(-90.0, 0.0));

    assertReaderContains(reader(test, ListOfLocations.class), expected);
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

    ListOfLists expected = new ListOfLists();
    expected.addToListOfLists(List.of(34, 35, 36));
    expected.addToListOfLists(List.of());
    expected.addToListOfLists(List.of(32, 33, 34));

    // should detect the "array" name
    assertReaderContains(reader(test, ListOfLists.class), expected);
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

    ListOfLists expected = new ListOfLists();
    expected.addToListOfLists(List.of(34, 35, 36));
    expected.addToListOfLists(List.of());
    expected.addToListOfLists(List.of(32, 33, 34));

    // should detect the "_tuple" names
    assertReaderContains(reader(test, ListOfLists.class), expected);
  }

  @Test
  public void testOldThriftCompatRequiredGroupInList() throws Exception {
    Path test = writeDirect(
        "message OldThriftCompatRequiredGroupInList {" + "  optional group locations (LIST) {"
            + "    repeated group locations_tuple {"
            + "      required group element {"
            + "        required double latitude;"
            + "        required double longitude;"
            + "      }"
            + "    }"
            + "  }"
            + "}",
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

    ListOfLocations expected = new ListOfLocations();
    expected.addToLocations(new Location(0.0, 180.0));
    expected.addToLocations(new Location(0.0, 0.0));

    assertReaderContains(reader(test, ListOfLocations.class), expected);
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

    ListOfLocations expected = new ListOfLocations();
    expected.addToLocations(new Location(0.0, 180.0));
    expected.addToLocations(new Location(0.0, 0.0));

    try {
      assertReaderContains(reader(test, ListOfLocations.class), expected);
      fail("Should fail: locations are optional and not ignored");
    } catch (RuntimeException e) {
      // e is a RuntimeException wrapping the decoding exception
      assertTrue(e.getCause().getCause().getMessage().contains("locations"));
    }

    assertReaderContains(readerIgnoreNulls(test, ListOfLocations.class), expected);
  }

  public <T extends TBase<?, ?>> ParquetReader<T> reader(Path file, Class<T> thriftClass) throws IOException {
    return ThriftParquetReader.<T>build(file).withThriftClass(thriftClass).build();
  }

  public <T extends TBase<?, ?>> ParquetReader<T> readerIgnoreNulls(Path file, Class<T> thriftClass)
      throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ThriftRecordConverter.IGNORE_NULL_LIST_ELEMENTS, true);
    return ThriftParquetReader.<T>build(file)
        .withThriftClass(thriftClass)
        .withConf(conf)
        .build();
  }

  public <T> void assertReaderContains(ParquetReader<T> reader, T... expected) throws IOException {
    T record;
    List<T> actual = Lists.newArrayList();
    while ((record = reader.read()) != null) {
      actual.add(record);
    }
    Assert.assertEquals("Should match exepected records", Lists.newArrayList(expected), actual);
  }
}
