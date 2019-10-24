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
package org.apache.parquet.filter;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.Set;

/**
 * Builder for a record filter. Idea is that each filter provides a create function
 * which returns an unbound filter. This only becomes a filter when it is bound to the actual
 * columns.
 */
public interface UnboundRecordFilter {

  /**
   * Call to bind to actual columns and create filter.
   *
   * @param readers an iterable of readers to bind this filter to
   * @return this unbound filter as a filter bound to the readers
   */
  RecordFilter bind(Iterable<ColumnReader> readers);

  /**
   * Call to collect column paths into the set
   */
  void collectColumnPaths(Set<ColumnPath> columnPathSet);
}