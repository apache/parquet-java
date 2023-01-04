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

import static org.apache.parquet.hadoop.util.DataMaskingUtil.removeColumnsInSchema;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class TestDataMaskingUtil
{

  @Test
  public void testRemoveColumnsInSchema() {
    MessageType schema = createTestSchema();
    String removedColumns = "Name,Links.Backward";

    MessageType newSchema = removeColumnsInSchema(schema, removedColumns);

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
    String[] removedColumns = {
      "msg.FreeShapeDeliveryZoneInfo.MultipleGeofence.array.HexagonIDs.array",
      "msg.Latitude",
      "msg.Longitude",
      "msg.FulfillmentZoneInfo.DeliveryZoneInfoMap.map.value.FreeShapeDeliveryZoneInfo.HexagonIDs.array",
      "msg.FreeShapeDeliveryZoneInfo.HexagonIDs.array",
      "msg.FulfillmentZoneInfo.DeliveryZoneInfoList.array.FreeShapeDeliveryZoneInfo.HexagonIDs.array",
      "msg.FulfillmentZoneInfo.DeliveryZoneInfoMap.map.value.FreeShapeDeliveryZoneInfo.MultipleGeofence.array.HexagonIDs.array",
      "msg.FulfillmentZoneInfo.DeliveryZoneInfoList.array.FreeShapeDeliveryZoneInfo.MultipleGeofence.array.HexagonIDs.array"
    };
    testSchemaFilesWithRemovedColumns(path, removedColumns);
  }

  @Test
  public void testRemoveInHiveSchema() throws IOException {
    Path path = Paths.get("src/test/java/org/apache/parquet/hadoop/util/schemafiles/schema2.txt");
    String[] removedColumns = {
      "pickup_completed.supply_location.lat",
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
    testSchemaFilesWithRemovedColumns(path, removedColumns);
  }

  @Test
  public void testRemoveArrayColumnsInSchema() throws IOException {
    Path path = Paths.get("src/test/java/org/apache/parquet/hadoop/util/schemafiles/schema3.txt");
    String[] removedColumns = {
      "msg.waypoints.array.latitude",
      "msg.waypoints.array.longitude"
    };
    testSchemaFilesWithRemovedColumns(path, removedColumns);
  }

  @Test
  public void testRemoveFlatColumnsInSchema() throws IOException {
    Path path = Paths.get("src/test/java/org/apache/parquet/hadoop/util/schemafiles/schema4.txt");
    String[] removedColumns = {
      "msg.rs_geohash_level_5",
      "msg.rs_geohash_level_6",
      "msg.dropoffLocation.longitude",
      "msg.dropoffLocation.latitude"
    };
    testSchemaFilesWithRemovedColumns(path, removedColumns);
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

  private void testSchemaFilesWithRemovedColumns(Path path, String[] removedColumns) throws IOException {
    MessageType schema = createSchemaFromFile(path);

    MessageType newSchema = removeColumnsInSchema(schema, String.join(DataMaskingUtil.DELIMITER, removedColumns));
    Set<ColumnPath> removedColumnPaths = new HashSet<>();
    for (String removedColumn : removedColumns) {
      removedColumnPaths.add(ColumnPath.fromDotString(removedColumn));
    }
    validateSchemaDiff(schema, newSchema, removedColumnPaths);
  }

  private void validateSchemaDiff(MessageType oldSchema, MessageType newSchema, Set<ColumnPath> removedColumns) {
    Set<ColumnPath> leafNodesBeforeRemoval = flatSchema(oldSchema);
    Set<ColumnPath> leafNodesAfterRemoval = flatSchema(newSchema);
    for (ColumnPath node : leafNodesAfterRemoval) {
      if (!leafNodesBeforeRemoval.remove(node)) {
        Assert.fail("node doesn't exist in original schema");
      }
    }
    Assert.assertEquals(leafNodesBeforeRemoval, removedColumns);
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
