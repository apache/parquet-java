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
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DataMaskingUtil
{
  public final static String DATA_MASKING_COLUMNS = "parquet.data.masking.columns";
  public final static String DELIMITER = ",";

  public static MessageType removeColumnsInSchema(MessageType schema, String removeColumns)
  {
    if (removeColumns == null || removeColumns.isEmpty()) {
      return schema;
    }

    Set<ColumnPath> paths = getColumnPaths(removeColumns);
    List<String> currentPath = new ArrayList<>();
    List<Type> prunedFields = removeColumnsInFields(schema.getFields(), currentPath, paths);
    return new MessageType(schema.getName(), prunedFields);
  }

  private static Set<ColumnPath> getColumnPaths(String str) {
    Set<ColumnPath> columnPaths = new HashSet<>();
    if (str == null || str.length() == 0) {
      return columnPaths;
    }

    String[] splits = str.split(DELIMITER);
    for (String split : splits) {
      columnPaths.add(ColumnPath.fromDotString(split));
    }
    return columnPaths;
  }

  private static List<Type> removeColumnsInFields(List<Type> fields, List<String> currentPath, Set<ColumnPath> paths)
  {
    List<Type> prunedFields = new ArrayList<>();
    for (org.apache.parquet.schema.Type childField : fields) {
      org.apache.parquet.schema.Type prunedChildField = removeColumnsInField(childField, currentPath, paths);
      if (prunedChildField != null) {
        prunedFields.add(prunedChildField);
      }
    }
    return prunedFields;
  }

  private static Type removeColumnsInField(Type field, List<String> currentPath, Set<ColumnPath> paths)
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
        List<Type> prunedFields = removeColumnsInFields(childFields, currentPath, paths);
        if (prunedFields.size() > 0) {
          prunedField = ((GroupType) field).withNewFields(prunedFields);
        }
      }
    }

    currentPath.remove(currentPath.size() - 1);
    return prunedField;
  }
}
