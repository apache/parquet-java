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
package parquet.filter;

import parquet.Preconditions;
import parquet.column.ColumnReader;

/**
 * Provides ability to negate the result of a filter.
 *
 * @author Frank Austin Nothaft
 */
public final class NotRecordFilter implements RecordFilter {

  private final RecordFilter boundFilter;

  /**
   * Returns builder for creating an and filter.
   * @param filter The filter to invert.
   */
  public static final UnboundRecordFilter not( final UnboundRecordFilter filter) {
    Preconditions.checkNotNull( filter, "filter" );
    return new UnboundRecordFilter() {
      @Override
      public RecordFilter bind(Iterable<ColumnReader> readers) {
        return new NotRecordFilter( filter.bind(readers) );
      }
    };
  }

  /**
   * Private constructor, use NotRecordFilter.not() instead.
   */
  private NotRecordFilter( RecordFilter boundFilter) {
    this.boundFilter = boundFilter;
  }

  @Override
  public boolean isMatch() {
    return !(boundFilter.isMatch());
  }
}
