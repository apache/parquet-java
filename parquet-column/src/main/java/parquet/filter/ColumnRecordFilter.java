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

import parquet.column.ColumnReader;
import java.util.Arrays;
import static parquet.Preconditions.checkNotNull;

/**
 * Record filter which applies the supplied predicate to the specified column.
 */
public final class ColumnRecordFilter implements RecordFilter {

  private final ColumnReader filterOnColumn;
  private final ColumnPredicates.Predicate filterPredicate;

  /**
   * Factory method for record filter which applies the supplied predicate to the specified column.
   * Note that if searching for a repeated sub-attribute it will only ever match against the
   * first instance of it in the object.
   *
   * @param columnPath Dot separated path specifier, e.g. "engine.capacity"
   * @param predicate Should call getBinary etc. and check the value
   */
  public static final UnboundRecordFilter column(final String columnPath,
                                                 final ColumnPredicates.Predicate predicate) {
    checkNotNull(columnPath, "columnPath");
    checkNotNull(predicate,  "predicate");
    return new UnboundRecordFilter() {
      final String[] filterPath = columnPath.split("\\.");
      @Override
      public RecordFilter bind(Iterable<ColumnReader> readers) {
        for (ColumnReader reader : readers) {
          if ( Arrays.equals( reader.getDescriptor().getPath(), filterPath)) {
            return new ColumnRecordFilter(reader, predicate);
          }
        }
        throw new IllegalArgumentException( "Column " + columnPath + " does not exist.");
      }
    };
  }

  /**
   * Private constructor. Use column() instead.
   */
  private ColumnRecordFilter(ColumnReader filterOnColumn, ColumnPredicates.Predicate filterPredicate) {
    this.filterOnColumn  = filterOnColumn;
    this.filterPredicate = filterPredicate;
  }

  /**
   * @return true if the current value for the column reader matches the predicate.
   */
  @Override
  public boolean isMatch() {
    return filterPredicate.apply(filterOnColumn);
  }

}
