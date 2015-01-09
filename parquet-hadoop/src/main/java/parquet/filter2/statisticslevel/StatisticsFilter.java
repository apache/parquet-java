package parquet.filter2.statisticslevel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.column.statistics.Statistics;
import parquet.common.schema.ColumnPath;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators.And;
import parquet.filter2.predicate.Operators.Column;
import parquet.filter2.predicate.Operators.Eq;
import parquet.filter2.predicate.Operators.Gt;
import parquet.filter2.predicate.Operators.GtEq;
import parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import parquet.filter2.predicate.Operators.Lt;
import parquet.filter2.predicate.Operators.LtEq;
import parquet.filter2.predicate.Operators.Not;
import parquet.filter2.predicate.Operators.NotEq;
import parquet.filter2.predicate.Operators.Or;
import parquet.filter2.predicate.Operators.UserDefined;
import parquet.filter2.predicate.UserDefinedPredicate;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import static parquet.Preconditions.checkArgument;
import static parquet.Preconditions.checkNotNull;

/**
 * Applies a {@link parquet.filter2.predicate.FilterPredicate} to statistics about a group of
 * records.
 *
 * Note: the supplied predicate must not contain any instances of the not() operator as this is not
 * supported by this filter.
 *
 * the supplied predicate should first be run through {@link parquet.filter2.predicate.LogicalInverseRewriter} to rewrite it
 * in a form that doesn't make use of the not() operator.
 *
 * the supplied predicate should also have already been run through
 * {@link parquet.filter2.predicate.SchemaCompatibilityValidator}
 * to make sure it is compatible with the schema of this file.
 *
 * Returns true if all the records represented by the statistics in the provided column metadata can be dropped.
 *         false otherwise (including when it is not known, which is often the case).
 */
// TODO: this belongs in the parquet-column project, but some of the classes here need to be moved too
// TODO: (https://issues.apache.org/jira/browse/PARQUET-38)
public class StatisticsFilter implements FilterPredicate.Visitor<Boolean> {

  public static boolean canDrop(FilterPredicate pred, List<ColumnChunkMetaData> columns) {
    checkNotNull(pred, "pred");
    checkNotNull(columns, "columns");
    return pred.accept(new StatisticsFilter(columns));
  }

  private final Map<ColumnPath, ColumnChunkMetaData> columns = new HashMap<ColumnPath, ColumnChunkMetaData>();

  private StatisticsFilter(List<ColumnChunkMetaData> columnsList) {
    for (ColumnChunkMetaData chunk : columnsList) {
      columns.put(chunk.getPath(), chunk);
    }
  }

  private ColumnChunkMetaData getColumnChunk(ColumnPath columnPath) {
    ColumnChunkMetaData c = columns.get(columnPath);
    checkArgument(c != null, "Column " + columnPath.toDotString() + " not found in schema!");
    return c;
  }

  // is this column chunk composed entirely of nulls?
  // in case of all nulls, the stats object read from file metadata is empty
  private boolean isAllNulls(ColumnChunkMetaData column) {
    return (column.getStatistics().isEmpty()) || (column.getStatistics().getNumNulls() == column.getValueCount());
  }

  // are there any nulls in this column chunk?
  private boolean hasNulls(ColumnChunkMetaData column) {
    return column.getStatistics().getNumNulls() > 0;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
    Column<T> filterColumn = eq.getColumn();
    T value = eq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (value == null) {
      // we are looking for records where v eq(null)
      // so drop if there are no nulls in this chunk
      return !hasNulls(columnChunk);
    }

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v eq(someNonNull)
      // and this is a column of all nulls, so drop it
      return true;
    }

    Statistics<T> stats = columnChunk.getStatistics();

    // drop if value < min || value > max
    return value.compareTo(stats.genericGetMin()) < 0 || value.compareTo(stats.genericGetMax()) > 0;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
    Column<T> filterColumn = notEq.getColumn();
    T value = notEq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (value == null) {
      // we are looking for records where v notEq(null)
      // so, if this is a column of all nulls, we can drop it
      return isAllNulls(columnChunk);
    }

    if (hasNulls(columnChunk)) {
      // we are looking for records where v notEq(someNonNull)
      // but this chunk contains nulls, we cannot drop it
      return false;
    }

    Statistics<T> stats = columnChunk.getStatistics();

    // drop if this is a column where min = max = value
    return value.compareTo(stats.genericGetMin()) == 0 && value.compareTo(stats.genericGetMax()) == 0;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Lt<T> lt) {
    Column<T> filterColumn = lt.getColumn();
    T value = lt.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v < someValue
      // this chunk is all nulls, so we can drop it
      return true;
    }

    Statistics<T> stats = columnChunk.getStatistics();

    // drop if value <= min
    return  value.compareTo(stats.genericGetMin()) <= 0;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
    Column<T> filterColumn = ltEq.getColumn();
    T value = ltEq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v <= someValue
      // this chunk is all nulls, so we can drop it
      return true;
    }

    Statistics<T> stats = columnChunk.getStatistics();

    // drop if value < min
    return value.compareTo(stats.genericGetMin()) < 0;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Gt<T> gt) {
    Column<T> filterColumn = gt.getColumn();
    T value = gt.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v > someValue
      // this chunk is all nulls, so we can drop it
      return true;
    }

    Statistics<T> stats = columnChunk.getStatistics();

    // drop if value >= max
    return value.compareTo(stats.genericGetMax()) >= 0;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
    Column<T> filterColumn = gtEq.getColumn();
    T value = gtEq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v >= someValue
      // this chunk is all nulls, so we can drop it
      return true;
    }

    Statistics<T> stats = columnChunk.getStatistics();

    // drop if value >= max
    return value.compareTo(stats.genericGetMax()) > 0;
  }

  @Override
  public Boolean visit(And and) {
    return and.getLeft().accept(this) && and.getRight().accept(this);
  }

  @Override
  public Boolean visit(Or or) {
    // seems unintuitive to put an && not an || here
    // but we can only drop a chunk of records if we know that
    // both the left and right predicates agree that no matter what
    // we don't need this chunk.
    return or.getLeft().accept(this) && or.getRight().accept(this);
  }

  @Override
  public Boolean visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }

  private <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> ud, boolean inverted) {
    Column<T> filterColumn = ud.getColumn();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    U udp = ud.getUserDefinedPredicate();
    Statistics<T> stats = columnChunk.getStatistics();
    parquet.filter2.predicate.Statistics<T> udpStats =
        new parquet.filter2.predicate.Statistics<T>(stats.genericGetMin(), stats.genericGetMax());

    if (inverted) {
      return udp.inverseCanDrop(udpStats);
    } else {
      return udp.canDrop(udpStats);
    }
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> ud) {
    return visit(ud, false);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(LogicalNotUserDefined<T, U> lnud) {
    return visit(lnud.getUserDefined(), true);
  }

}
