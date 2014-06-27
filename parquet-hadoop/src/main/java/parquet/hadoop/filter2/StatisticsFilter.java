package parquet.hadoop.filter2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.Preconditions;
import parquet.column.statistics.Statistics;
import parquet.filter2.FilterPredicate;
import parquet.filter2.FilterPredicates.And;
import parquet.filter2.FilterPredicates.Column;
import parquet.filter2.FilterPredicates.Eq;
import parquet.filter2.FilterPredicates.Gt;
import parquet.filter2.FilterPredicates.GtEq;
import parquet.filter2.FilterPredicates.LogicalNotUserDefined;
import parquet.filter2.FilterPredicates.Lt;
import parquet.filter2.FilterPredicates.LtEq;
import parquet.filter2.FilterPredicates.Not;
import parquet.filter2.FilterPredicates.NotEq;
import parquet.filter2.FilterPredicates.Or;
import parquet.filter2.FilterPredicates.UserDefined;
import parquet.filter2.StatisticsUtil.MinMaxComparison;
import parquet.filter2.UserDefinedPredicates.UserDefinedPredicate;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.schema.ColumnPathUtil;

import static parquet.filter2.StatisticsUtil.applyUdpMinMax;
import static parquet.filter2.StatisticsUtil.compareMinMax;

public class StatisticsFilter implements FilterPredicate.Visitor<Boolean> {

  /**
   * Note: pred must not contain any instances of the not() operator as this is not
   * supported by this filter.
   *
   * pred should first be run through {@link parquet.filter2.CollapseLogicalNots} to rewrite it
   * in a form that doesn't make use of the not() operator.
   *
   * pred should also have already been run through {@link parquet.filter2.FilterValidator} to make sure
   * it is compatible with the schema of this file.
   *
   * TODO: we could just remove the not operator. see comments in CollapseLogicalNots
   */
  public static boolean canDrop(FilterPredicate pred, List<ColumnChunkMetaData> columns) {
    return pred.accept(new StatisticsFilter(columns));
  }

  private final Map<String, ColumnChunkMetaData> columns = new HashMap<String, ColumnChunkMetaData>();

  public StatisticsFilter(List<ColumnChunkMetaData> columnsList) {
    for (ColumnChunkMetaData chunk : columnsList) {
      String columnPath = ColumnPathUtil.toDotSeparatedString(chunk.getPath().toArray());
      columns.put(columnPath, chunk);
    }
  }

  private ColumnChunkMetaData getColumnChunk(String columnPath) {
    ColumnChunkMetaData c = columns.get(columnPath);
    Preconditions.checkArgument(c != null, "Column " + columnPath + " not found in schema!");
    return c;
  }

  private boolean isAllNulls(ColumnChunkMetaData column) {
    return column.getStatistics().getNumNulls() == column.getValueCount();
  }

  private boolean hasNulls(ColumnChunkMetaData column) {
    return column.getStatistics().getNumNulls() > 0;
  }

  @Override
  public <T> Boolean visit(Eq<T> eq) {
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

    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compareMinMax(filterColumn.getColumnType(), value, rawStats);

    // drop if value < min || value > max
    return cmp.getMinCmp() < 0 || cmp.getMaxCmp() > 0;
  }

  @Override
  public <T> Boolean visit(NotEq<T> notEq) {
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

    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compareMinMax(filterColumn.getColumnType(), value, rawStats);

    // drop if this is a column where min = max = value
    return cmp.getMinCmp() == 0 && cmp.getMaxCmp() == 0;
  }

  @Override
  public <T> Boolean visit(Lt<T> lt) {
    Column<T> filterColumn = lt.getColumn();
    T value = lt.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v < someValue
      // this chunk is all nulls, so we can drop it
      return true;
    }

    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compareMinMax(filterColumn.getColumnType(), value, rawStats);

    // drop if value <= min
    return cmp.getMinCmp() <= 0;
  }

  @Override
  public <T> Boolean visit(LtEq<T> ltEq) {
    Column<T> filterColumn = ltEq.getColumn();
    T value = ltEq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v <= someValue
      // this chunk is all nulls, so we can drop it
      return true;
    }

    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compareMinMax(filterColumn.getColumnType(), value, rawStats);

    // drop if value < min
    return cmp.getMinCmp() < 0;
  }

  @Override
  public <T> Boolean visit(Gt<T> gt) {
    Column<T> filterColumn = gt.getColumn();
    T value = gt.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v > someValue
      // this chunk is all nulls, so we can drop it
      return true;
    }

    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compareMinMax(filterColumn.getColumnType(), value, rawStats);

    // drop if value >= max
    return cmp.getMaxCmp() >= 0;
  }

  @Override
  public <T> Boolean visit(GtEq<T> gtEq) {
    Column<T> filterColumn = gtEq.getColumn();
    T value = gtEq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());

    if (isAllNulls(columnChunk)) {
      // we are looking for records where v >= someValue
      // this chunk is all nulls, so we can drop it
      return true;
    }

    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compareMinMax(filterColumn.getColumnType(), value, rawStats);

    // drop if value >= max
    return cmp.getMaxCmp() > 0;
  }

  @Override
  public Boolean visit(And and) {
    Boolean canDropLeft = and.getLeft().accept(this);
    Boolean canDropRight = and.getRight().accept(this);
    return canDropLeft && canDropRight;
  }

  @Override
  public Boolean visit(Or or) {
    Boolean canDropLeft = or.getLeft().accept(this);
    Boolean canDropRight = or.getRight().accept(this);

    // seems unintuitive to put an && not an || here
    // but we can only drop a chunk of records if we know that
    // both the left and right predicates agree that no matter what
    // we don't need this chunk.
    return canDropLeft && canDropRight;
  }

  @Override
  public Boolean visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through CollapseLogicalNots? " + not);
  }

  private <T, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> ud, boolean inverted) {
    Column<T> filterColumn = ud.getColumn();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    U udp = ud.getUserDefinedPredicate();
    Statistics rawStats = columnChunk.getStatistics();
    return applyUdpMinMax(filterColumn.getColumnType(), udp, rawStats, inverted);
  }

  @Override
  public <T, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> ud) {
    return visit(ud, false);
  }

  @Override
  public <T, U extends UserDefinedPredicate<T>> Boolean visit(LogicalNotUserDefined<T, U> lnud) {
    return visit(lnud.getUserDefined(), true);
  }

}
