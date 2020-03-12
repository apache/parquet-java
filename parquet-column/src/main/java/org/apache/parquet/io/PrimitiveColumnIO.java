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
package org.apache.parquet.io;


import java.util.Arrays;
import java.util.List;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;


/**
 * Primitive level of the IO structure
 */
public class PrimitiveColumnIO extends ColumnIO {
//  private static final Logger logger = Logger.getLogger(PrimitiveColumnIO.class.getName());

  private ColumnIO[] path;
  private ColumnDescriptor columnDescriptor;
  private final int id;

  PrimitiveColumnIO(Type type, GroupColumnIO parent, int index, int id) {
    super(type, parent, index);
    this.id = id;
  }

  @Override
  void setLevels(int r, int d, String[] fieldPath, int[] fieldIndexPath, List<ColumnIO> repetition, List<ColumnIO> path) {
    super.setLevels(r, d, fieldPath, fieldIndexPath, repetition, path);
    PrimitiveType type = getType().asPrimitiveType();
    this.columnDescriptor = new ColumnDescriptor(
        fieldPath,
        type,
        getRepetitionLevel(),
        getDefinitionLevel());
    this.path = path.toArray(new ColumnIO[0]);
  }

  @Override
  List<String[]> getColumnNames() {
    return Arrays.asList(new String[][] { getFieldPath() });
  }

  public ColumnDescriptor getColumnDescriptor() {
    return columnDescriptor;
  }

  public ColumnIO[] getPath() {
    return path;
  }

  public boolean isLast(int r) {
    return getLast(r) == this;
  }

  private PrimitiveColumnIO getLast(int r) {
    ColumnIO parent = getParent(r);

    PrimitiveColumnIO last = parent.getLast();
    return last;
  }

  @Override
  PrimitiveColumnIO getLast() {
    return this;
  }

  @Override
  PrimitiveColumnIO getFirst() {
    return this;
  }
  public boolean isFirst(int r) {
    return getFirst(r) == this;
  }

  private PrimitiveColumnIO getFirst(int r) {
    ColumnIO parent = getParent(r);
    return parent.getFirst();
  }

  public PrimitiveTypeName getPrimitive() {
    return getType().asPrimitiveType().getPrimitiveTypeName();
  }

  public int getId() {
    return id;
  }

}
