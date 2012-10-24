/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redelm.io;

import static redelm.schema.Type.Repetition.REPEATED;
import static redelm.schema.Type.Repetition.REQUIRED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redelm.Log;
import redelm.column.ColumnsStore;
import redelm.schema.GroupType;

public class GroupColumnIO extends ColumnIO {
  private static final Log logger = Log.getLog(GroupColumnIO.class);

  private final Map<String, ColumnIO> childrenByName = new HashMap<String, ColumnIO>();
  private final List<ColumnIO> children = new ArrayList<ColumnIO>();
  private int childrenSize = 0;

  GroupColumnIO(GroupType groupType, GroupColumnIO parent) {
    super(groupType, parent);
  }

  void add(ColumnIO child) {
    children.add(child);
    childrenByName.put(child.getType().getName(), child);
    ++ childrenSize;
  }

  @Override
  void setLevels(int r, int d, String[] fieldPath, int[] indexFieldPath, List<ColumnIO> repetition, List<ColumnIO> path, ColumnsStore columns) {
    super.setLevels(r, d, fieldPath, indexFieldPath, repetition, path, columns);
    for (ColumnIO child : this.children) {
      String[] newFieldPath = Arrays.copyOf(fieldPath, fieldPath.length + 1);
      int[] newIndexFieldPath = Arrays.copyOf(indexFieldPath, indexFieldPath.length + 1);
      newFieldPath[fieldPath.length] =  child.getType().getName();
      newIndexFieldPath[indexFieldPath.length] =  this.getType().asGroupType().getFieldIndex(child.getType().getName());
      List<ColumnIO> newRepetition;
      if (child.getType().getRepetition() == REPEATED) {
        newRepetition = new ArrayList<ColumnIO>(repetition);
        newRepetition.add(child);
      } else {
        newRepetition = repetition;
      }
      List<ColumnIO> newPath = new ArrayList<ColumnIO>(path);
      newPath.add(child);
      child.setLevels(
          // the type repetition level increases whenever there's a possible repetition
          child.getType().getRepetition() == REPEATED ? r + 1 : r,
          // the type definition level increases whenever a field can be missing (not required)
          child.getType().getRepetition() != REQUIRED ? d + 1 : d,
          newFieldPath,
          newIndexFieldPath,
          newRepetition,
          newPath,
          columns
          );

    }
  }

  @Override
  void writeNull(int r, int d) {
    for (ColumnIO child : this.children) {
      child.writeNull(r, d);
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
      throw new RuntimeException("could not get child " + fieldIndex + " from " + children, e);
    }
  }

  public int getChildrenCount() {
    return childrenSize;

  }

}
