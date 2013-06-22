package parquet.filter;

import parquet.column.ColumnReader;

/**
 * Null filter which will always let all records through.
 */
final class NullRecordFilter implements UnboundRecordFilter, RecordFilter {

  /**
   * Package level visibility so we can make an instance available in interface.
   */
  NullRecordFilter() {}

  @Override
  public RecordFilter bind(Iterable<ColumnReader> readers) {
    return this;
  }

  @Override
  public boolean isMatch() {
    return true;
  }

  @Override
  public boolean isFullyConsumed() {
    // Always false, we will leave to the record reader to decide when it has consumed everything
    return false;
  }
}
