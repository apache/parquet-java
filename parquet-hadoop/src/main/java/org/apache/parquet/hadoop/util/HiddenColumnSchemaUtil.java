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

import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility methods to support data masking when column key is unauthorized.
 */
public class HiddenColumnSchemaUtil {
  public final static String HIDDEN_COLUMN = "parquet.hidden.columns";
  public final static String DELIMITER = ",";
  public final static String REMOVED_GROUP_NODES = "parquet.removed.group_nodes";

  public static Set<ColumnPath> extractHiddenColumns(List<BlockMetaData> blocks)
  {
    Set<ColumnPath> columnPaths = new HashSet<>();
    for (BlockMetaData block : blocks) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.isNullMaskedColumn()) {
          columnPaths.add(column.getPath());
        }
      }
    }
    return columnPaths;
  }

  public static MessageType removeColumnsInSchema(MessageType schema, Set<ColumnPath> paths, Set<String> removedGroupNodes)
  {
    List<String> currentPath = new ArrayList<>();
    List<Type> prunedFields = removeColumnsInFields(schema.getFields(), currentPath, paths, removedGroupNodes);
    return new MessageType(schema.getName(), prunedFields);
  }

  public static String stringfyColumnPaths(Set<ColumnPath> paths) {
    StringBuilder strBuilder = new StringBuilder();
    boolean first = true;
    for (ColumnPath path : paths) {
      if (first) {
        strBuilder.append(path.toDotString());
        first = false;
      } else {
        strBuilder.append(DELIMITER).append(path.toDotString());
      }
    }
    return strBuilder.toString();
  }

  public static List<String[]> restoreToList(String str) {
    List<String[]> paths = new ArrayList<>();
    if (str == null || str.length() == 0) {
      return paths;
    }

    String[] splits = str.split(DELIMITER);
    for (String split : splits) {
      paths.add(ColumnPath.fromDotString(split).toArray());
    }
    return paths;
  }

  private static List<Type> removeColumnsInFields(List<Type> fields, List<String> currentPath, Set<ColumnPath> paths, Set<String> removedGroupNodes)
  {
    List<Type> prunedFields = new ArrayList<>();
    for (org.apache.parquet.schema.Type childField : fields) {
      org.apache.parquet.schema.Type prunedChildField = removeColumnsInField(childField, currentPath, paths, removedGroupNodes);
      if (prunedChildField != null) {
        prunedFields.add(prunedChildField);
      }
    }
    return prunedFields;
  }

  private static Type removeColumnsInField(Type field, List<String> currentPath, Set<ColumnPath> paths, Set<String> removedGroupNodes)
  {
    String fieldName = field.getName();
    currentPath.add(fieldName);
    ColumnPath path = ColumnPath.get(currentPath.toArray(new String[0]));
    org.apache.parquet.schema.Type prunedField = null;
    if (!paths.contains(path)) {
      if (field.isPrimitive()) {
        prunedField = field;
      }
      else {
        List<Type> childFields = ((GroupType) field).getFields();
        List<Type> prunedFields = removeColumnsInFields(childFields, currentPath, paths, removedGroupNodes);
        if (prunedFields.size() > 0) {
          prunedField = ((GroupType) field).withNewFields(prunedFields);
        } else {
          if (removedGroupNodes != null) {
            removedGroupNodes.add(field.getName());
          }
        }
      }
    }

    Preconditions.checkState(currentPath.size() > 0,
      "The variable currentPath is empty but trying to remove element in it");
    Preconditions.checkState(currentPath.get(currentPath.size() - 1) != null,
      "The last element of currentPath is null");
    Preconditions.checkState(currentPath.get(currentPath.size() - 1).equals(fieldName),
      "The last element of currentPath " + currentPath.get(currentPath.size() - 1) + " not equal to " + fieldName);

    currentPath.remove(currentPath.size() - 1);
    return prunedField;
  }
}
