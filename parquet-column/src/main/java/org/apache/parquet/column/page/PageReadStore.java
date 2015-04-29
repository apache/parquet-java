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

import org.apache.parquet.column.ColumnDescriptor;

/**
 * contains all the readers for all the columns of the corresponding row group
 *
 * TODO: rename to RowGroup?
 * 
 * @author Julien Le Dem
 *
 */
public interface PageReadStore {

  /**
   *
   * @param descriptor the descriptor of the column
   * @return the page reader for that column
   */
  PageReader getPageReader(ColumnDescriptor descriptor);

  /**
   *
   * @return the total number of rows in that row group
   */
  long getRowCount();

}
