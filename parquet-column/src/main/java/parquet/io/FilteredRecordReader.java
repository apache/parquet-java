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

import parquet.column.ColumnReader;
import parquet.column.impl.ColumnReadStoreImpl;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;
import parquet.io.api.RecordMaterializer;

/**
 * Extends the
 * @author Jacob Metcalf
 *
 */
class FilteredRecordReader<T> extends RecordReaderImplementation<T> {

  private final RecordFilter recordFilter;
  private final long recordCount;
  private long recordsRead = 0;

  /**
   * @param root          the root of the schema
   * @param validating
   * @param columnStore
   * @param unboundFilter Filter records, pass in NULL_FILTER to leave unfiltered.
   */
  public FilteredRecordReader(MessageColumnIO root, RecordMaterializer<T> recordMaterializer, boolean validating,
                              ColumnReadStoreImpl columnStore, UnboundRecordFilter unboundFilter, long recordCount) {
    super(root, recordMaterializer, validating, columnStore);
    this.recordCount = recordCount;
    if ( unboundFilter != null ) {
      recordFilter = unboundFilter.bind(getColumnReaders());
    } else {
      recordFilter = null;
    }
  }

  /**
   * Override read() method to provide skip.
   */
  @Override
  public T read() {
    skipToMatch();
    if (recordsRead == recordCount) {
      return null;
    }
    ++ recordsRead;
    return super.read();
  }

  // FilteredRecordReader skips forwards itself, it never asks the layer above to do the skipping for it.
  // This is different from how filtering is handled in the filter2 API
  @Override
  public boolean shouldSkipCurrentRecord() {
    return false;
  }

  /**
   * Skips forwards until the filter finds the first match. Returns false
   * if none found.
   */
  private void skipToMatch() {
    while (recordsRead < recordCount && !recordFilter.isMatch()) {
      State currentState = getState(0);
      do {
        ColumnReader columnReader = currentState.column;

        // currentLevel = depth + 1 at this point
        // set the current value
        if (columnReader.getCurrentDefinitionLevel() >= currentState.maxDefinitionLevel) {
          columnReader.skip();
        }
        columnReader.consume();

        // Based on repetition level work out next state to go to
        int nextR = currentState.maxRepetitionLevel == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
        currentState = currentState.getNextState(nextR);
      } while (currentState != null);
      ++ recordsRead;
    }
  }
}
