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
package org.apache.parquet.column.page;

import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.parquet.column.ColumnDescriptor;

/**
 * contains all the readers for all the columns of the corresponding row group
 *
 * TODO: rename to RowGroup?
 */
public interface PageReadStore {

  /**
   *
   * @param descriptor
   *          the descriptor of the column
   * @return the page reader for that column
   */
  PageReader getPageReader(ColumnDescriptor descriptor);

  /**
   *
   * @return the total number of rows in that row group
   */
  long getRowCount();

  /**
   *
   * @return the row index offset of this row group.
   */
  default Optional<Long> getRowIndexOffset() {
    return Optional.empty();
  }

  /**
   * Returns the indexes of the rows to be read/built if the related data is available. All the rows which index is not
   * returned shall be skipped.
   *
   * @return the optional of the incremental iterator of the row indexes or an empty optional if the related data is not
   *         available
   */
  default Optional<PrimitiveIterator.OfLong> getRowIndexes() {
    return Optional.empty();
  }
}
