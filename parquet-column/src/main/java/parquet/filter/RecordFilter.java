package parquet.filter;

import parquet.column.ColumnReader;

/**
 * Filter to be applied to a record to work out whether to skip it.
 *
 * @author Jacob Metcalf
 */
public interface RecordFilter {

  /**
   * Works out whether the current record can pass through the filter.
   */
  boolean isMatch();

  /**
   * Whether the filter values are fully consumed.
   */
  boolean isFullyConsumed();

  /**
   * Null filter to be used if no filtering needed.
   */
  public static final UnboundRecordFilter NULL_FILTER = new NullRecordFilter();
}
