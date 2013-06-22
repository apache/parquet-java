package parquet.filter;

import com.google.common.base.Predicate;
import parquet.column.ColumnReader;

/**
 * Builder for a record filter. Idea is that each filter provides a create function
 * which returns an unbound filter. This only becomes a filter when it is bound to the actual
 * columns.
 *
 * @author Jacob Metcalf
 */
public interface UnboundRecordFilter {

  /**
   * Call to bind to actual columns and create filter.
   */
  RecordFilter bind( Iterable<ColumnReader> readers);
}
