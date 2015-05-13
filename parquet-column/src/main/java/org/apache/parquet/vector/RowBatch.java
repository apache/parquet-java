/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.vector;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static org.apache.parquet.Preconditions.checkNotNull;

public class RowBatch {

  private int size;
  private ColumnVector[] columns;

  public int size() {
    //TODO: see the FIXME in setColumns()
    throw new NotImplementedException();
//    return size;
  }

  public ColumnVector[] getColumns() {
    return columns;
  }

  public void setColumns(ColumnVector[] columns) {
    checkNotNull(columns, "columns");
    this.columns = columns;

    //FIXME given column vectors had different sizes in my tests, may be a bug somewhere
//    size = -1;
//    for(ColumnVector cv: columns) {
//      if (size == -1) {
//        size = cv.size();
//      } else {
//        if (size != cv.size()) {
//          throw new RuntimeException("columns have different sizes");
//        }
//      }
//    }
  }
}
