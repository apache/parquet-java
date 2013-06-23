package parquet.filter;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import parquet.column.ColumnReader;
import parquet.io.api.Binary;

import java.util.Arrays;

import static com.google.common.collect.Iterables.toArray;
import static parquet.Preconditions.checkNotNull;

/**
 * Record filter which applies the supplied predicate to the specified column.
 */
public final class ColumnRecordFilter implements RecordFilter {

  private final ColumnReader filterOnColumn;
  private final Predicate<ColumnReader> filterPredicate;

  /**
   * Factory method for record filter which applies the supplied predicate to the specified column.
   * Note that if searching for a repeated sub-attribute it will only ever match against the
   * first instance of it in the object.
   *
   * @param columnPath Dot separated path specifier, e.g. "engine.capacity"
   * @param predicate Should call getBinary etc. and check the value
   */
  public static final UnboundRecordFilter column(final String columnPath, final Predicate<ColumnReader> predicate) {
    checkNotNull(columnPath, "columnPath");
    checkNotNull(predicate,  "predicate");
    return new UnboundRecordFilter() {
      final String[] filterPath = toArray(Splitter.on('.').split(columnPath), String.class);
      @Override
      public RecordFilter bind(Iterable<ColumnReader> readers) {
        for ( ColumnReader reader : readers ) {
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
  private ColumnRecordFilter(ColumnReader filterOnColumn, Predicate<ColumnReader> filterPredicate) {
    this.filterOnColumn  = filterOnColumn;
    this.filterPredicate = filterPredicate;
  }

  /**
   * @return true if the current value for the column reader matches the predicate.
   */
  @Override
  public boolean isMatch() {

    return ( filterOnColumn.isFullyConsumed()) ? false : filterPredicate.apply( filterOnColumn );
  }

  /**
   * @return true if the column we are filtering on has no more values.
   */
  @Override
  public boolean isFullyConsumed() {
    return filterOnColumn.isFullyConsumed();
  }

  /**
   * Predicate for string equality
   */
  public static final Predicate<ColumnReader> equalTo( final String value ) {
    final Binary valueAsBinary = Binary.fromString( value );
    return new Predicate<ColumnReader> () {
      @Override
      public boolean apply(ColumnReader input) {
        return Objects.equal( input.getBinary(), valueAsBinary );
      }
    };
  }

  /**
   * Predicate for INT64 / long equality
   */
  public static final Predicate<ColumnReader> equalTo( final long value ) {
    return new Predicate<ColumnReader> () {
      @Override
      public boolean apply(ColumnReader input) {
        return input.getLong() == value;
      }
    };
  }
}
