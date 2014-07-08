package parquet.hadoop.filter2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import parquet.column.Encoding;
import parquet.column.statistics.DoubleStatistics;
import parquet.column.statistics.IntStatistics;
import parquet.filter2.CollapseLogicalNots;
import parquet.filter2.FilterPredicate;
import parquet.filter2.FilterPredicateOperators.Column;
import parquet.filter2.UserDefinedPredicate;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static parquet.filter2.Filter.and;
import static parquet.filter2.Filter.doubleColumn;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.gt;
import static parquet.filter2.Filter.gtEq;
import static parquet.filter2.Filter.intColumn;
import static parquet.filter2.Filter.lt;
import static parquet.filter2.Filter.ltEq;
import static parquet.filter2.Filter.not;
import static parquet.filter2.Filter.notEq;
import static parquet.filter2.Filter.or;
import static parquet.filter2.Filter.userDefined;
import static parquet.hadoop.filter2.StatisticsFilter.canDrop;

public class TestStatisticsFilter {

  private static ColumnChunkMetaData getIntColumnMeta(IntStatistics stats, long valueCount) {
    return ColumnChunkMetaData.get(ColumnPath.get("int", "column"),
        PrimitiveTypeName.INT32,
        CompressionCodecName.GZIP,
        new HashSet<Encoding>(Arrays.asList(Encoding.PLAIN)),
        stats,
        0L, 0L, valueCount, 0L, 0L);
  }

  private static ColumnChunkMetaData getDoubleColumnMeta(DoubleStatistics stats, long valueCount) {
    return ColumnChunkMetaData.get(ColumnPath.get("double", "column"),
        PrimitiveTypeName.DOUBLE,
        CompressionCodecName.GZIP,
        new HashSet<Encoding>(Arrays.asList(Encoding.PLAIN)),
        stats,
        0L, 0L, valueCount, 0L, 0L);
  }

  private static final Column<Integer> intColumn = intColumn("int.column");
  private static final Column<Double> doubleColumn = doubleColumn("double.column");

  private static final IntStatistics intStats = new IntStatistics();
  private static final IntStatistics nullIntStats = new IntStatistics();
  private static final DoubleStatistics doubleStats = new DoubleStatistics();

  static {
    intStats.setMinMax(10, 100);
    doubleStats.setMinMax(10, 100);

    nullIntStats.setMinMax(0, 0);
    nullIntStats.setNumNulls(177);
  }

  private static final List<ColumnChunkMetaData> columnMetas = Arrays.asList(
      getIntColumnMeta(intStats, 177L),
      getDoubleColumnMeta(doubleStats, 177L));

  private static final List<ColumnChunkMetaData> nullColumnMetas = Arrays.asList(
      getIntColumnMeta(nullIntStats, 177L), // column of all nulls
      getDoubleColumnMeta(doubleStats, 177L));


  @Test
  public void testEqNonNull() {
    assertTrue(canDrop(eq(intColumn, 9), columnMetas));
    assertFalse(canDrop(eq(intColumn, 10), columnMetas));
    assertFalse(canDrop(eq(intColumn, 100), columnMetas));
    assertTrue(canDrop(eq(intColumn, 101), columnMetas));

    // drop columns of all nulls when looking for non-null value
    assertTrue(canDrop(eq(intColumn, 0), nullColumnMetas));
  }

  @Test
  public void testEqNull() {
    IntStatistics statsNoNulls = new IntStatistics();
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics();
    statsSomeNulls.setMinMax(10, 100);
    statsSomeNulls.setNumNulls(3);

    assertTrue(canDrop(eq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsNoNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(eq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsSomeNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

  }

  @Test
  public void testNotEqNonNull() {
    assertFalse(canDrop(notEq(intColumn, 9), columnMetas));
    assertFalse(canDrop(notEq(intColumn, 10), columnMetas));
    assertFalse(canDrop(notEq(intColumn, 100), columnMetas));
    assertFalse(canDrop(notEq(intColumn, 101), columnMetas));

    IntStatistics allSevens = new IntStatistics();
    allSevens.setMinMax(7, 7);
    assertTrue(canDrop(notEq(intColumn, 7), Arrays.asList(
        getIntColumnMeta(allSevens, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

  }

  @Test
  public void testNotEqNull() {
    IntStatistics statsNoNulls = new IntStatistics();
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics();
    statsSomeNulls.setMinMax(10, 100);
    statsSomeNulls.setNumNulls(3);

    IntStatistics statsAllNulls = new IntStatistics();
    statsAllNulls.setMinMax(0, 0);
    statsAllNulls.setNumNulls(177);

    assertFalse(canDrop(notEq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsNoNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(notEq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsSomeNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(notEq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsAllNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));
  }

  @Test
  public void testLt() {
    assertTrue(canDrop(lt(intColumn, 9), columnMetas));
    assertTrue(canDrop(lt(intColumn, 10), columnMetas));
    assertFalse(canDrop(lt(intColumn, 100), columnMetas));
    assertFalse(canDrop(lt(intColumn, 101), columnMetas));

    assertTrue(canDrop(lt(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(lt(intColumn, 7), nullColumnMetas));
  }

  @Test
  public void testLtEq() {
    assertTrue(canDrop(ltEq(intColumn, 9), columnMetas));
    assertFalse(canDrop(ltEq(intColumn, 10), columnMetas));
    assertFalse(canDrop(ltEq(intColumn, 100), columnMetas));
    assertFalse(canDrop(ltEq(intColumn, 101), columnMetas));

    assertTrue(canDrop(ltEq(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(ltEq(intColumn, 7), nullColumnMetas));
  }

  @Test
  public void testGt() {
    assertFalse(canDrop(gt(intColumn, 9), columnMetas));
    assertFalse(canDrop(gt(intColumn, 10), columnMetas));
    assertTrue(canDrop(gt(intColumn, 100), columnMetas));
    assertTrue(canDrop(gt(intColumn, 101), columnMetas));

    assertTrue(canDrop(gt(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(gt(intColumn, 7), nullColumnMetas));
  }

  @Test
  public void testGtEq() {
    assertFalse(canDrop(gtEq(intColumn, 9), columnMetas));
    assertFalse(canDrop(gtEq(intColumn, 10), columnMetas));
    assertFalse(canDrop(gtEq(intColumn, 100), columnMetas));
    assertTrue(canDrop(gtEq(intColumn, 101), columnMetas));

    assertTrue(canDrop(gtEq(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(gtEq(intColumn, 7), nullColumnMetas));
  }

  @Test
  public void testAnd() {
    FilterPredicate yes = eq(intColumn, 9);
    FilterPredicate no = eq(doubleColumn, 50D);
    assertTrue(canDrop(and(yes, yes), columnMetas));
    assertFalse(canDrop(and(yes, no), columnMetas));
    assertFalse(canDrop(and(no, yes), columnMetas));
    assertFalse(canDrop(and(no, no), columnMetas));
  }

  @Test
  public void testOr() {
    FilterPredicate yes = eq(intColumn, 9);
    FilterPredicate no = eq(doubleColumn, 50D);
    assertTrue(canDrop(or(yes, yes), columnMetas));
    assertFalse(canDrop(or(yes, no), columnMetas));
    assertFalse(canDrop(or(no, yes), columnMetas));
    assertFalse(canDrop(or(no, no), columnMetas));
  }

  public static class SevensAndEightsUdp extends UserDefinedPredicate<Integer> {

    @Override
    public boolean keep(Integer value) {
      throw new RuntimeException("this method should not be called");
    }

    @Override
    public boolean canDrop(Integer min, Integer max) {
      return min == 7 && max == 7;
    }

    @Override
    public boolean inverseCanDrop(Integer min, Integer max) {
      return min == 8 && max == 8;
    }
  }

  @Test
  public void testUdp() {
    FilterPredicate pred = userDefined(intColumn, SevensAndEightsUdp.class);
    FilterPredicate invPred = CollapseLogicalNots.collapse(not(userDefined(intColumn, SevensAndEightsUdp.class)));

    IntStatistics seven = new IntStatistics();
    seven.setMinMax(7, 7);

    IntStatistics eight = new IntStatistics();
    eight.setMinMax(8, 8);

    IntStatistics neither = new IntStatistics();
    neither.setMinMax(1 , 2);

    assertTrue(canDrop(pred, Arrays.asList(
        getIntColumnMeta(seven, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(pred, Arrays.asList(
        getIntColumnMeta(eight, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(pred, Arrays.asList(
        getIntColumnMeta(neither, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(invPred, Arrays.asList(
        getIntColumnMeta(seven, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(invPred, Arrays.asList(
        getIntColumnMeta(eight, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(invPred, Arrays.asList(
        getIntColumnMeta(neither, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));
  }

  @Test
  public void testClearExceptionForNots() {
    List<ColumnChunkMetaData> columnMetas = Arrays.asList(
        getIntColumnMeta(new IntStatistics(), 0L),
        getDoubleColumnMeta(new DoubleStatistics(), 0L));

    FilterPredicate pred = and(eq(intColumn, 17), not(eq(doubleColumn, 12.0)));

    try {
      canDrop(pred, columnMetas);
      fail("This should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("This predicate contains a not! Did you forget to run this predicate through CollapseLogicalNots?"
          + " not(eq(double.column, 12.0))", e.getMessage());
    }
  }

  @Test
  public void testMissingColumn() {
    List<ColumnChunkMetaData> columnMetas = Arrays.asList(getIntColumnMeta(new IntStatistics(), 0L));
    try {
      canDrop(and(eq(intColumn, 17), eq(doubleColumn, 12.0)), columnMetas);
      fail("This should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("Column double.column not found in schema!", e.getMessage());
    }
  }

}
