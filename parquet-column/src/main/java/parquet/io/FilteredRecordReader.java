package parquet.io;

import parquet.column.ColumnReader;
import parquet.column.impl.ColumnReadStoreImpl;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;
import parquet.io.api.RecordMaterializer;

import java.util.Arrays;

/**
 * Extends the
 * @author Jacob Metcalf
 *
 */
class FilteredRecordReader<T> extends RecordReaderImplementation<T> {

  private final RecordFilter recordFilter;

  /**
   * @param root          the root of the schema
   * @param validating
   * @param columnStore
   * @param unboundFilter Filter records, pass in NULL_FILTER to leave unfiltered.
   */
  public FilteredRecordReader(MessageColumnIO root, RecordMaterializer<T> recordMaterializer, boolean validating,
                              ColumnReadStoreImpl columnStore, UnboundRecordFilter unboundFilter) {
    super(root, recordMaterializer, validating, columnStore);

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
    if ( skipToMatch()) {
      return super.read();
    } else {
      return null;
    }
  }


  /**
   * Skips forwards until the filter finds the first match. Returns false
   * if none found.
   */
  private boolean skipToMatch() {
    while ( !recordFilter.isMatch()) {
      if ( recordFilter.isFullyConsumed()) {
        return false;
      }
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
    }
    return true;
  }
}
