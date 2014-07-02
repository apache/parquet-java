/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.io;

import parquet.Preconditions;
import parquet.column.ColumnReader;
import parquet.column.impl.ColumnReadStoreImpl;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.FilterPredicate;
import parquet.filter2.RecordPredicate;
import parquet.filter2.RecordPredicateBuilder;
import parquet.io.api.RecordMaterializer;

/**
 * Extends the
 * @author Jacob Metcalf
 *
 */
class FilteredRecordReader<T> extends RecordReaderImplementation<T> {

  private final RecordFilter boundRecordFilter;
  private final RecordPredicate recordPredicate;

  private final long recordCount;
  private long recordsRead = 0;

  /**
   * @param root          the root of the schema
   * @param validating
   * @param columnStore
   * @param unboundFilter Filter records, pass in NULL_FILTER to leave unfiltered.
   */
  public FilteredRecordReader(MessageColumnIO root,
                              RecordMaterializer<T> recordMaterializer,
                              boolean validating,
                              ColumnReadStoreImpl columnStore,
                              UnboundRecordFilter unboundFilter,
                              FilterPredicate filterPredicate,
                              long recordCount) {
    super(root, recordMaterializer, validating, columnStore);

    this.recordCount = recordCount;

    Preconditions.checkArgument(unboundFilter != null || filterPredicate !=null,
        "FilteredRecordReader requires either an unboundFilter or a filterPredicate, they cannot both be null");

    Preconditions.checkArgument(unboundFilter == null || filterPredicate == null,
        "FilteredRecordReader requires either an unboundFilter or a filterPredicate, you cannot provide both.");

    if (unboundFilter != null) {
      boundRecordFilter = unboundFilter.bind(getColumnReaders());
    } else {
      boundRecordFilter = null;
    }

    if (filterPredicate != null) {
      this.recordPredicate = RecordPredicateBuilder.build(filterPredicate, getColumnReaders());
    } else {
      this.recordPredicate = null;
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

  private boolean isMatch() {
    if (boundRecordFilter != null) {
      return boundRecordFilter.isMatch();
    }

    if (recordPredicate != null) {
      return recordPredicate.isMatch();
    }

    return true;
  }

  /**
   * Skips forwards until the filter finds the first match. Returns false
   * if none found.
   */
  private void skipToMatch() {
    while (recordsRead < recordCount && !isMatch()) {
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
