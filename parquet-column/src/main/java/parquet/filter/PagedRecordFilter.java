package parquet.filter;

import parquet.column.ColumnReader;

/**
 * Filter which will only materialize a page worth of results.
 */
public final class PagedRecordFilter implements RecordFilter {

  private final long startPos;
  private final long endPos;
  private long currentPos = 0;

  /**
   * Returns builder for creating a paged query.
   * @param startPos The record to start from, numbering starts at 1.
   * @param pageSize The size of the page.
   */
  public static final UnboundRecordFilter page( final long startPos, final long pageSize ) {
    return new UnboundRecordFilter() {
      @Override
      public RecordFilter bind(Iterable<ColumnReader> readers) {
        return new PagedRecordFilter( startPos, pageSize );
      }
    };
  }

  /**
   * Private constructor, use column() instead.
   */
  private PagedRecordFilter(long startPos, long pageSize) {
    this.startPos = startPos;
    this.endPos   = startPos + pageSize;
  }

  /**
   * Terminate early when we have got our page.
   */
  @Override
  public boolean isFullyConsumed() {
    return ( currentPos >= endPos );
  }

  /**
   * Keeps track of how many times it is called. Only returns matches when the
   * record number is in the range.
   */
  @Override
  public boolean isMatch() {
    currentPos++;
    return (( currentPos >= startPos ) && ( currentPos < endPos ));
  }

}
