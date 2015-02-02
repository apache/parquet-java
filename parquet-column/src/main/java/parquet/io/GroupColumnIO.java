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
package parquet.io;

import static parquet.schema.Type.Repetition.REPEATED;
import static parquet.schema.Type.Repetition.REQUIRED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.Log;
import parquet.schema.GroupType;

/**
 * Group level of the IO structure
 *
 *
 * @author Julien Le Dem
 *
 */
public class GroupColumnIO extends ColumnIO {
  private static final Log LOG = Log.getLog(GroupColumnIO.class);

  private final Map<String, ColumnIO> childrenByName = new HashMap<String, ColumnIO>();
  private final List<ColumnIO> children = new ArrayList<ColumnIO>();
  private int childrenSize = 0;

  GroupColumnIO(GroupType groupType, GroupColumnIO parent, int index) {
    super(groupType, parent, index);
  }

  void add(ColumnIO child) {
    children.add(child);
    childrenByName.put(child.getType().getName(), child);
    ++ childrenSize;
  }

  @Override
  void setLevels(int r, int d, String[] fieldPath, int[] indexFieldPath, List<ColumnIO> repetition, List<ColumnIO> path) {
    super.setLevels(r, d, fieldPath, indexFieldPath, repetition, path);
    for (ColumnIO child : this.children) {
      String[] newFieldPath = Arrays.copyOf(fieldPath, fieldPath.length + 1);
      int[] newIndexFieldPath = Arrays.copyOf(indexFieldPath, indexFieldPath.length + 1);
      newFieldPath[fieldPath.length] = child.getType().getName();
      newIndexFieldPath[indexFieldPath.length] = child.getIndex();
      List<ColumnIO> newRepetition;
      if (child.getType().isRepetition(REPEATED)) {
        newRepetition = new ArrayList<ColumnIO>(repetition);
        newRepetition.add(child);
      } else {
        newRepetition = repetition;
      }
      List<ColumnIO> newPath = new ArrayList<ColumnIO>(path);
      newPath.add(child);
      child.setLevels(
          // the type repetition level increases whenever there's a possible repetition
          child.getType().isRepetition(REPEATED) ? r + 1 : r,
          // the type definition level increases whenever a field can be missing (not required)
          !child.getType().isRepetition(REQUIRED) ? d + 1 : d,
          newFieldPath,
          newIndexFieldPath,
          newRepetition,
          newPath
          );

    }
  }

  @Override
  List<String[]> getColumnNames() {
    ArrayList<String[]> result = new ArrayList<String[]>();
    for (ColumnIO c : children) {
      result.addAll(c.getColumnNames());
    }
    return result;
  }

  PrimitiveColumnIO getLast() {
    return children.get(children.size()-1).getLast();
  }

  PrimitiveColumnIO getFirst() {
    return children.get(0).getFirst();
  }

  public ColumnIO getChild(String name) {
    return childrenByName.get(name);
  }

  public ColumnIO getChild(int fieldIndex) {
    try {
      return children.get(fieldIndex);
    } catch (IndexOutOfBoundsException e) {
      throw new InvalidRecordException("could not get child " + fieldIndex + " from " + children, e);
    }
  }

  public int getChildrenCount() {
    return childrenSize;

  }

}
