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
package org.apache.parquet.internal.filter2.columnindex;

import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

/**
 * Provides the {@link ColumnIndex} and {@link OffsetIndex} objects for a row-group.
 */
public interface ColumnIndexStore {

  /**
   * Exception thrown in case of an offset index is missing for any of the columns.
   */
  public static class MissingOffsetIndexException extends ParquetRuntimeException {
    public MissingOffsetIndexException(ColumnPath path) {
      super("No offset index for column " + path.toDotString() + " is available; Unable to do filtering");
    }
  }

  /**
   * @param column the path of the column
   * @return the column index for the column-chunk in the row-group or {@code null} if no column index is available
   */
  ColumnIndex getColumnIndex(ColumnPath column);

  /**
   * @param column the path of the column
   * @return the offset index for the column-chunk in the row-group
   * @throws MissingOffsetIndexException if the related offset index is missing
   */
  OffsetIndex getOffsetIndex(ColumnPath column) throws MissingOffsetIndexException;
}
