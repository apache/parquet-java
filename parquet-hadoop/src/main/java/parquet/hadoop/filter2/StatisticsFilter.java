package parquet.hadoop.filter2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.Preconditions;
import parquet.bytes.BytesUtils;
import parquet.column.statistics.BinaryStatistics;
import parquet.column.statistics.BooleanStatistics;
import parquet.column.statistics.DoubleStatistics;
import parquet.column.statistics.FloatStatistics;
import parquet.column.statistics.IntStatistics;
import parquet.column.statistics.LongStatistics;
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
import parquet.filter2.UserDefinedPredicates.BinaryUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.DoubleUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.FloatUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.IntUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.LongUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.StringUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.UserDefinedPredicate;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.io.api.Binary;
import parquet.schema.ColumnPathUtil;

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

  private static final class MinMaxComparison {
    private final int minCmp;
    private final int maxCmp;

    private MinMaxComparison(int minCmp, int maxCmp) {
      this.minCmp = minCmp;
      this.maxCmp = maxCmp;
    }

    public int getMinCmp() {
      return minCmp;
    }

    public int getMaxCmp() {
      return maxCmp;
    }
  }

  private <T> MinMaxComparison compare(Class<T> clazz, T rawValue, Statistics rawStats) {

    if (clazz.equals(Integer.class)) {
      IntStatistics stats = (IntStatistics) rawStats;
      int value = (Integer) rawValue;
      return new MinMaxComparison(
          Integer.compare(value, stats.getMin()),
          Integer.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Long.class)) {
      LongStatistics stats = (LongStatistics) rawStats;
      long value = (Long) rawValue;
      return new MinMaxComparison(
          Long.compare(value, stats.getMin()),
          Long.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Float.class)) {
      FloatStatistics stats = (FloatStatistics) rawStats;
      float value = (Float) rawValue;
      return new MinMaxComparison(
          Float.compare(value, stats.getMin()),
          Float.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Double.class)) {
      DoubleStatistics stats = (DoubleStatistics) rawStats;
      double value = (Double) rawValue;
      return new MinMaxComparison(
          Double.compare(value, stats.getMin()),
          Double.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Boolean.class)) {
      BooleanStatistics stats = (BooleanStatistics) rawStats;
      boolean value = (Boolean) rawValue;
      return new MinMaxComparison(
          Boolean.compare(value, stats.getMin()),
          Boolean.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Binary.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      Binary value = (Binary) rawValue;

      int minCmp = value.compareTo(stats.getMin());
      int maxCmp = value.compareTo(stats.getMax());
      return new MinMaxComparison(minCmp, maxCmp);
    }

    if (clazz.equals(String.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      String strValue = (String) rawValue;
      Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));

      int minCmp = value.compareTo(stats.getMin());
      int maxCmp = value.compareTo(stats.getMax());
      return new MinMaxComparison(minCmp, maxCmp);
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz);
  }

  private <T> boolean applyUdp(Class<T> clazz, UserDefinedPredicate<T> rawUdp, Statistics rawStats, boolean inverted) {

    if (clazz.equals(Integer.class)) {
      IntStatistics stats = (IntStatistics) rawStats;
      IntUserDefinedPredicate udp = (IntUserDefinedPredicate) rawUdp;
      return udp.canDrop(stats.getMin(), stats.getMax(), inverted);
    }

    if (clazz.equals(Long.class)) {
      LongStatistics stats = (LongStatistics) rawStats;
      LongUserDefinedPredicate udp = (LongUserDefinedPredicate) rawUdp;
      return udp.canDrop(stats.getMin(), stats.getMax(), inverted);
    }

    if (clazz.equals(Float.class)) {
      FloatStatistics stats = (FloatStatistics) rawStats;
      FloatUserDefinedPredicate udp = (FloatUserDefinedPredicate) rawUdp;
      return udp.canDrop(stats.getMin(), stats.getMax(), inverted);
    }

    if (clazz.equals(Double.class)) {
      DoubleStatistics stats = (DoubleStatistics) rawStats;
      DoubleUserDefinedPredicate udp = (DoubleUserDefinedPredicate) rawUdp;
      return udp.canDrop(stats.getMin(), stats.getMax(), inverted);
    }

    if (clazz.equals(Binary.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      BinaryUserDefinedPredicate udp = (BinaryUserDefinedPredicate) rawUdp;
      return udp.canDrop(stats.getMin(), stats.getMax(), inverted);
    }

    if (clazz.equals(String.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      StringUserDefinedPredicate udp = (StringUserDefinedPredicate) rawUdp;
      return udp.canDrop(stats.getMin().toStringUsingUTF8(), stats.getMax().toStringUsingUTF8(), inverted);
    }

    throw new IllegalArgumentException("Encountered unknown filter column type for user defined predicate " + clazz);
  }

  @Override
  public <T> Boolean visit(Eq<T> eq) {
    Column<T> filterColumn = eq.getColumn();
    T value = eq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    Statistics rawStats = columnChunk.getStatistics();

    if (value == null) {
      // drop if there are no nulls
      return rawStats.getNumNulls() == 0;
    }

    MinMaxComparison cmp = compare(filterColumn.getColumnType(), value, rawStats);

    // drop if value < min || value > max
    return cmp.getMinCmp() < 0 || cmp.getMaxCmp() > 0;
  }

  @Override
  public <T> Boolean visit(NotEq<T> notEq) {
    Column<T> filterColumn = notEq.getColumn();
    T value = notEq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    Statistics rawStats = columnChunk.getStatistics();

    if (value == null) {
      // drop if this is a column of all nulls
      return rawStats.getNumNulls() == columnChunk.getValueCount();
    }

    MinMaxComparison cmp = compare(filterColumn.getColumnType(), value, rawStats);

    // drop if this is a column where min = max = value
    return cmp.getMinCmp() == 0 && cmp.getMaxCmp() == 0;
  }

  @Override
  public <T> Boolean visit(Lt<T> lt) {
    Column<T> filterColumn = lt.getColumn();
    T value = lt.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compare(filterColumn.getColumnType(), value, rawStats);

    // drop if value <= min
    return cmp.getMinCmp() <= 0;
  }

  @Override
  public <T> Boolean visit(LtEq<T> ltEq) {
    Column<T> filterColumn = ltEq.getColumn();
    T value = ltEq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compare(filterColumn.getColumnType(), value, rawStats);

    // drop if value < min
    return cmp.getMinCmp() < 0;
  }

  @Override
  public <T> Boolean visit(Gt<T> gt) {
    Column<T> filterColumn = gt.getColumn();
    T value = gt.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compare(filterColumn.getColumnType(), value, rawStats);

    // drop if value >= max
    return cmp.getMaxCmp() >= 0;
  }

  @Override
  public <T> Boolean visit(GtEq<T> gtEq) {
    Column<T> filterColumn = gtEq.getColumn();
    T value = gtEq.getValue();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    Statistics rawStats = columnChunk.getStatistics();

    MinMaxComparison cmp = compare(filterColumn.getColumnType(), value, rawStats);

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
    return applyUdp(filterColumn.getColumnType(), udp, rawStats, inverted);
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
