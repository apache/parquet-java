/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.hadoop.util.HiddenColumnSchemaUtil.removeColumnsInSchema;
import static org.apache.parquet.hadoop.util.HiddenColumnSchemaUtil.restoreToList;
import static org.apache.parquet.hadoop.util.HiddenColumnSchemaUtil.stringfyColumnPaths;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class TestHiddenColumnSchemaUtil {

  @Test
  public void testRemoveColumnsInSchema() {
    MessageType schema = createTestSchema();
    Set<ColumnPath> paths = new HashSet<>();
    paths.add(ColumnPath.fromDotString("Name"));
    paths.add(ColumnPath.fromDotString("Links.Backward"));

    MessageType newSchema = removeColumnsInSchema(schema, paths, null);

    String[] docId = {"DocId"};
    Assert.assertTrue(newSchema.containsPath(docId));
    String[] gender = {"Gender"};
    Assert.assertTrue(newSchema.containsPath(gender));
    String[] linkForward = {"Links", "Forward"};
    Assert.assertTrue(newSchema.containsPath(linkForward));
    String[] name = {"Name"};
    Assert.assertFalse(newSchema.containsPath(name));
    String[] linkBackward = {"Links", "Backward"};
    Assert.assertFalse(newSchema.containsPath(linkBackward));
  }

  @Test
  public void testRemoveNestedComplexColumnsInSchema() throws IOException {
    Path path = Paths.get("src/test/java/org/apache/parquet/hadoop/util/schemafiles/schema1.txt");
    String[] dotStrings = {"msg.FreeShapeDeliveryZoneInfo.MultipleGeofence.array.HexagonIDs.array",
      "msg.Latitude",
      "msg.Longitude",
      "msg.FulfillmentZoneInfo.DeliveryZoneInfoMap.map.value.FreeShapeDeliveryZoneInfo.HexagonIDs.array",
      "msg.FreeShapeDeliveryZoneInfo.HexagonIDs.array",
      "msg.FulfillmentZoneInfo.DeliveryZoneInfoList.array.FreeShapeDeliveryZoneInfo.HexagonIDs.array",
      "msg.FulfillmentZoneInfo.DeliveryZoneInfoMap.map.value.FreeShapeDeliveryZoneInfo.MultipleGeofence.array.HexagonIDs.array",
      "msg.FulfillmentZoneInfo.DeliveryZoneInfoList.array.FreeShapeDeliveryZoneInfo.MultipleGeofence.array.HexagonIDs.array"
    };
    testSchemaFilesWithHiddenColumns(path, dotStrings);
  }

  @Test
  public void testRemoveInHiveSchema() throws IOException {
    Path path = Paths.get("src/test/java/org/apache/parquet/hadoop/util/schemafiles/schema2.txt");
    String[] dotStrings = {  "pickup_completed.supply_location.lat",
      "scheduled.pickup_hexagon_9",
      "created.pickup_location.hexagon_9",
      "scheduled.dropoff_lat",
      "pickup_completed.supply_location.lng",
      "created.pickup_location.lat",
      "scheduled.dropoff_lng",
      "pickup_completed.supply_location.hexagon_9",
      "scheduled.pickup_lng",
      "scheduled.dropoff_hexagon_9",
      "scheduled.pickup_lat",
      "created.pickup_location.lng",
    };
    testSchemaFilesWithHiddenColumns(path, dotStrings);
  }

  @Test
  public void testRemoveArrayColumnsInSchema() throws IOException {
    Path path = Paths.get("src/test/java/org/apache/parquet/hadoop/util/schemafiles/schema3.txt");
    String[] dotStrings = {  "msg.waypoints.array.latitude",
      "msg.waypoints.array.longitude"
    };
    testSchemaFilesWithHiddenColumns(path, dotStrings);
  }

  @Test
  public void testRemoveFlatColumnsInSchema() throws IOException {
    Path path = Paths.get("src/test/java/org/apache/parquet/hadoop/util/schemafiles/schema4.txt");
    String[] dotStrings = {"msg.rs_geohash_level_5",
      "msg.rs_geohash_level_6",
      "msg.dropoffLocation.longitude",
      "msg.dropoffLocation.latitude"
    };
    testSchemaFilesWithHiddenColumns(path, dotStrings);
  }

  @Test
  public void testRemovedGroupNodes() {
    MessageType schema = createTestSchema();
    Set<ColumnPath> paths = new HashSet<>();
    paths.add(ColumnPath.fromDotString("Links.Forward"));
    paths.add(ColumnPath.fromDotString("Links.Backward"));
    Set<String> removedGroupNodes = new HashSet<>();
    MessageType newSchema = removeColumnsInSchema(schema, paths, removedGroupNodes);
    Assert.assertEquals(removedGroupNodes.size(), 1);
    Assert.assertTrue(removedGroupNodes.contains("Links"));
  }

  @Test
  public void testRemoveMultipleLevelNestedComplexColumnsInSchema() {
    MessageType schema = new MessageType("schema",
      new GroupType(OPTIONAL, "group",
        new GroupType(REPEATED, "subgroup",
          new GroupType(REPEATED, "group",
            new PrimitiveType(REQUIRED, INT64, "col1")),
          new PrimitiveType(REQUIRED, INT64,  "col2"))));

    Set<ColumnPath> paths = new HashSet<>();
    paths.add(ColumnPath.fromDotString("group.subgroup.col2"));

    MessageType newSchema = removeColumnsInSchema(schema, paths, null);

    String[] groupColumn = {"group", "subgroup",  "group", "col1"};
    Assert.assertTrue(newSchema.containsPath(groupColumn));
    String[] singleColumn = {"group", "subgroup", "col2"};
    Assert.assertFalse(newSchema.containsPath(singleColumn));
  }

  @Test
  public void testStringfyColumnPaths() {
    Set<ColumnPath> paths = new HashSet<>();
    paths.add(ColumnPath.fromDotString("DocId"));
    paths.add(ColumnPath.fromDotString("Gender"));
    paths.add(ColumnPath.fromDotString("Links.Forward"));

    String strColumns = stringfyColumnPaths(paths);
    Assert.assertTrue(strColumns != null && strColumns.length() > 0);
    Assert.assertTrue(strColumns.contains("DocId"));
    Assert.assertTrue(strColumns.contains("Gender"));
    Assert.assertTrue(strColumns.contains("Links.Forward"));

    List<String[]> hiddenColumns = restoreToList(strColumns);
    Assert.assertEquals(hiddenColumns.size(), paths.size());
    for (String[] column : hiddenColumns) {
      Assert.assertTrue(paths.contains(ColumnPath.get(column)));
    }
  }

  @Test
  public void testRestoreFromConf() {
    Configuration conf = new Configuration();
    String strColumns = conf.get(HiddenColumnSchemaUtil.HIDDEN_COLUMN);
    List<String[]> hiddenColumns = restoreToList(strColumns);
    Assert.assertEquals(hiddenColumns.size(), 0);

    Set<ColumnPath> paths = new HashSet<>();
    paths.add(ColumnPath.fromDotString("a.b"));
    paths.add(ColumnPath.fromDotString("c"));
    conf.set(HiddenColumnSchemaUtil.HIDDEN_COLUMN, stringfyColumnPaths(paths));
    strColumns = conf.get(HiddenColumnSchemaUtil.HIDDEN_COLUMN);
    hiddenColumns = restoreToList(strColumns);
    Assert.assertEquals(hiddenColumns.size(), 2);
  }

  private static MessageType createTestSchema() {
    return new MessageType("schema",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "Name"),
      new PrimitiveType(REQUIRED, BINARY, "Gender"),
      new GroupType(OPTIONAL, "Links",
        new PrimitiveType(REPEATED, INT64, "Backward"),
        new PrimitiveType(REPEATED, INT64, "Forward")));
  }

  private void testSchemaFilesWithHiddenColumns(Path path, String[] dotStrings) throws IOException {
    MessageType schema = createSchemaFromFile(path);
    Set<ColumnPath> maskedColumns = new HashSet<>();
    for (String dotString : dotStrings) {
      maskedColumns.add(ColumnPath.fromDotString(dotString));
    }
    MessageType newSchema = removeColumnsInSchema(schema, maskedColumns, null);
    validateSchemaDiff(schema, newSchema, maskedColumns);
  }

  private void validateSchemaDiff(MessageType oldSchema, MessageType newSchema, Set<ColumnPath> maskedColumns) {
    Set<ColumnPath> leafNodesBeforeRemoval = flatSchema(oldSchema);
    Set<ColumnPath> leafNodesAfterRemoval = flatSchema(newSchema);
    for (ColumnPath node : leafNodesAfterRemoval) {
      if (!leafNodesBeforeRemoval.remove(node)) {
        Assert.fail("node doesn't exist in original schema");
      }
    }
    Assert.assertEquals(leafNodesBeforeRemoval, maskedColumns);
  }

  private MessageType createSchemaFromFile(Path path) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader reader = Files.newBufferedReader(path)) {
      String str;
      while ((str = reader.readLine()) != null) {
        if (str.length() > 0 && !str.substring(0, 1).equals("#")) {
          sb.append(str).append(" ");
        }
      }
    }
    return parseMessageType(sb.toString());
  }

  private Set<ColumnPath> flatSchema(MessageType schema) {
    Set<ColumnPath> leafNodes = new HashSet<>();
    List<String> currentPath = new ArrayList<>();
    List<Type> fields = schema.getFields();
    for (Type field : fields) {
      addNode(field, currentPath, leafNodes);
    }
    if (currentPath.size() != 0) {
      throw new RuntimeException("currentPath is not cleaned up");
    }
    return leafNodes;
  }

  private void addNode(Type field, List<String> currentPath, Set<ColumnPath> leafNodes) {
    currentPath.add(field.getName());
    if (field.isPrimitive()) {
      leafNodes.add(ColumnPath.get(currentPath.toArray(new String[0])));
    } else {
      List<Type> childFields = field.asGroupType().getFields();
      for (Type child : childFields) {
        addNode(child, currentPath, leafNodes);
      }
    }
    currentPath.remove(currentPath.size() - 1);
  }
}
