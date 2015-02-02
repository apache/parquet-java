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


import java.util.Arrays;
import java.util.List;

import parquet.Log;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

/**
 * a structure used to serialize deserialize records
 *
 * @author Julien Le Dem
 *
 */
abstract public class ColumnIO {

  static final boolean DEBUG = Log.DEBUG;

  private final GroupColumnIO parent;
  private final Type type;
  private final String name;
  private final int index;
  private int repetitionLevel;
  private int definitionLevel;
  private String[] fieldPath;
  private int[] indexFieldPath;


  ColumnIO(Type type, GroupColumnIO parent, int index) {
    this.type = type;
    this.parent = parent;
    this.index = index;
    this.name = type.getName();
  }

  String[] getFieldPath() {
    return fieldPath;
  }

  public String getFieldPath(int level) {
    return fieldPath[level];
  }

  public int[] getIndexFieldPath() {
    return indexFieldPath;
  }

  public int getIndexFieldPath(int level) {
    return indexFieldPath[level];
  }

  public int getIndex() {
    return this.index;
  }

  public String getName() {
    return name;
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

  public Type getType() {
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
    if (getRepetitionLevel() == r && getType().isRepetition(Repetition.REPEATED)) {
      return this;
    } else  if (getParent()!=null && getParent().getDefinitionLevel()>=r) {
      return getParent().getParent(r);
    } else {
      throw new InvalidRecordException("no parent("+r+") for "+Arrays.toString(this.getFieldPath()));
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
