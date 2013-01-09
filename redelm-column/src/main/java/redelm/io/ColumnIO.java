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


import java.util.Arrays;
import java.util.List;

import redelm.Log;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

abstract public class ColumnIO {

  static final boolean DEBUG = Log.DEBUG;

  private final GroupColumnIO parent;
  private final Type type;
  private int repetitionLevel;
  private int definitionLevel;
  private String[] fieldPath;
  private int[] indexFieldPath;

  ColumnIO(Type type, GroupColumnIO parent) {
    this.type = type;
    this.parent = parent;
  }

  String[] getFieldPath() {
    return fieldPath;
  }

  public int[] getIndexFieldPath() {
    return indexFieldPath;
  }

  int getRepetitionLevel() {
    return repetitionLevel;
  }

  int getDefinitionLevel() {
    return definitionLevel;
  }

  void setRepetitionLevel(int repetitionLevel) {
    this.repetitionLevel = repetitionLevel;
  }

  void setDefinitionLevel(int definitionLevel) {
    this.definitionLevel = definitionLevel;
  }

  void setFieldPath(String[] fieldPath, int[] indexFieldPath) {
    this.fieldPath = fieldPath;
    this.indexFieldPath = indexFieldPath;
  }

  Type getType() {
    return type;
  }

  void setLevels(int r, int d, String[] fieldPath, int[] indexFieldPath, List<ColumnIO> repetition, List<ColumnIO> path) {
    setRepetitionLevel(r);
    setDefinitionLevel(d);
    setFieldPath(fieldPath, indexFieldPath);
  }

  abstract List<String[]> getColumnNames();

  public GroupColumnIO getParent() {
    return parent;
  }

  abstract PrimitiveColumnIO getLast();
  abstract PrimitiveColumnIO getFirst();

  ColumnIO getParent(int r) {
    if (getRepetitionLevel() == r && getType().getRepetition() == Repetition.REPEATED) {
      return this;
    } else  if (getParent()!=null && getParent().getDefinitionLevel()>=r) {
      return getParent().getParent(r);
    } else {
      throw new RuntimeException("no parent("+r+") for "+Arrays.toString(this.getFieldPath()));
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName()+" "+type.getName()
        +" r:"+repetitionLevel
        +" d:"+definitionLevel
        +" "+Arrays.toString(fieldPath);
  }

}
