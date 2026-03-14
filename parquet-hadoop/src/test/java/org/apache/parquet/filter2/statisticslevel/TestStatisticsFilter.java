/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.filter2.statisticslevel;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.contains;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notIn;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.statisticslevel.StatisticsFilter.canDrop;
import static org.apache.parquet.io.api.Binary.fromString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

public class TestStatisticsFilter {

  private static ColumnChunkMetaData getIntColumnMeta(
      org.apache.parquet.column.statistics.Statistics<?> stats, long valueCount) {
    return ColumnChunkMetaData.get(
        ColumnPath.get("int", "column"),
        PrimitiveTypeName.INT32,
        CompressionCodecName.GZIP,
        new HashSet<Encoding>(List.of(Encoding.PLAIN)),
        stats,
        0L,
        0L,
        valueCount,
        0L,
        0L);
  }

  private static ColumnChunkMetaData getDoubleColumnMeta(
      org.apache.parquet.column.statistics.Statistics<?> stats, long valueCount) {
    return ColumnChunkMetaData.get(
        ColumnPath.get("double", "column"),
        PrimitiveTypeName.DOUBLE,
        CompressionCodecName.GZIP,
        new HashSet<Encoding>(List.of(Encoding.PLAIN)),
        stats,
        0L,
        0L,
        valueCount,
        0L,
        0L);
  }

  private static final IntColumn intColumn = intColumn("int.column");
  private static final DoubleColumn doubleColumn = doubleColumn("double.column");
  private static final BinaryColumn missingColumn = binaryColumn("missing");
  private static final IntColumn missingColumn2 = intColumn("missing.int");

  private static final IntStatistics intStats = new IntStatistics();
  private static final IntStatistics nullIntStats = new IntStatistics();
  private static final org.apache.parquet.column.statistics.Statistics<?> emptyIntStats =
      org.apache.parquet.column.statistics.Statistics.getBuilderForReading(
              Types.required(PrimitiveTypeName.INT32).named("test_int32"))
          .build();
  private static final DoubleStatistics doubleStats = new DoubleStatistics();
  private static final org.apache.parquet.column.statistics.Statistics<?> missingMinMaxDoubleStats =
      org.apache.parquet.column.statistics.Statistics.getBuilderForReading(
              Types.required(PrimitiveTypeName.DOUBLE).named("test_double"))
          .withNumNulls(100)
          .build();

  static {
    intStats.setMinMax(10, 100);
    doubleStats.setMinMax(10, 100);

    nullIntStats.setNumNulls(177);
  }

  private static final List<ColumnChunkMetaData> columnMetas =
      List.of(getIntColumnMeta(intStats, 177L), getDoubleColumnMeta(doubleStats, 177L));

  private static final List<ColumnChunkMetaData> nullColumnMetas = List.of(
      getIntColumnMeta(nullIntStats, 177L), // column of all nulls
      getDoubleColumnMeta(doubleStats, 177L));

  private static final List<ColumnChunkMetaData> missingMinMaxColumnMetas = List.of(
      getIntColumnMeta(emptyIntStats, 177L), // missing min/max values and numNulls => stats is empty
      getDoubleColumnMeta(missingMinMaxDoubleStats, 177L)); // missing min/max, some null values

  @Test
  public void testEqNonNull() {
    assertTrue(canDrop(eq(intColumn, 9), columnMetas));
    assertFalse(canDrop(eq(intColumn, 10), columnMetas));
    assertFalse(canDrop(eq(intColumn, 100), columnMetas));
    assertTrue(canDrop(eq(intColumn, 101), columnMetas));

    // drop columns of all nulls when looking for non-null value
    assertTrue(canDrop(eq(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(eq(missingColumn, fromString("any")), columnMetas));

    assertFalse(canDrop(eq(intColumn, 50), missingMinMaxColumnMetas));
    assertFalse(canDrop(eq(doubleColumn, 50.0), missingMinMaxColumnMetas));
  }

  @Test
  public void testEqNull() {
    IntStatistics statsNoNulls = new IntStatistics();
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics();
    statsSomeNulls.setMinMax(10, 100);
    statsSomeNulls.setNumNulls(3);

    assertTrue(canDrop(
        eq(intColumn, null),
        List.of(getIntColumnMeta(statsNoNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        eq(intColumn, null),
        List.of(getIntColumnMeta(statsSomeNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(eq(missingColumn, null), columnMetas));

    assertFalse(canDrop(eq(intColumn, null), missingMinMaxColumnMetas));
    assertFalse(canDrop(eq(doubleColumn, null), missingMinMaxColumnMetas));
  }

  @Test
  public void testNotEqNonNull() {
    assertFalse(canDrop(notEq(intColumn, 9), columnMetas));
    assertFalse(canDrop(notEq(intColumn, 10), columnMetas));
    assertFalse(canDrop(notEq(intColumn, 100), columnMetas));
    assertFalse(canDrop(notEq(intColumn, 101), columnMetas));

    IntStatistics allSevens = new IntStatistics();
    allSevens.setMinMax(7, 7);
    assertTrue(canDrop(
        notEq(intColumn, 7),
        List.of(getIntColumnMeta(allSevens, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    allSevens.setNumNulls(100L);
    assertFalse(canDrop(
        notEq(intColumn, 7),
        List.of(getIntColumnMeta(allSevens, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    allSevens.setNumNulls(177L);
    assertFalse(canDrop(
        notEq(intColumn, 7),
        List.of(getIntColumnMeta(allSevens, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(notEq(missingColumn, fromString("any")), columnMetas));

    assertFalse(canDrop(notEq(intColumn, 50), missingMinMaxColumnMetas));
    assertFalse(canDrop(notEq(doubleColumn, 50.0), missingMinMaxColumnMetas));
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

    assertFalse(canDrop(
        notEq(intColumn, null),
        List.of(getIntColumnMeta(statsNoNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        notEq(intColumn, null),
        List.of(getIntColumnMeta(statsSomeNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(
        notEq(intColumn, null),
        List.of(getIntColumnMeta(statsAllNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(notEq(missingColumn, null), columnMetas));

    assertFalse(canDrop(notEq(intColumn, null), missingMinMaxColumnMetas));
    assertFalse(canDrop(notEq(doubleColumn, null), missingMinMaxColumnMetas));
  }

  @Test
  public void testLt() {
    assertTrue(canDrop(lt(intColumn, 9), columnMetas));
    assertTrue(canDrop(lt(intColumn, 10), columnMetas));
    assertFalse(canDrop(lt(intColumn, 100), columnMetas));
    assertFalse(canDrop(lt(intColumn, 101), columnMetas));

    assertTrue(canDrop(lt(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(lt(intColumn, 7), nullColumnMetas));

    assertTrue(canDrop(lt(missingColumn, fromString("any")), columnMetas));

    assertFalse(canDrop(lt(intColumn, 0), missingMinMaxColumnMetas));
    assertFalse(canDrop(lt(doubleColumn, 0.0), missingMinMaxColumnMetas));
  }

  @Test
  public void testLtEq() {
    assertTrue(canDrop(ltEq(intColumn, 9), columnMetas));
    assertFalse(canDrop(ltEq(intColumn, 10), columnMetas));
    assertFalse(canDrop(ltEq(intColumn, 100), columnMetas));
    assertFalse(canDrop(ltEq(intColumn, 101), columnMetas));

    assertTrue(canDrop(ltEq(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(ltEq(intColumn, 7), nullColumnMetas));

    assertTrue(canDrop(ltEq(missingColumn, fromString("any")), columnMetas));

    assertFalse(canDrop(ltEq(intColumn, -1), missingMinMaxColumnMetas));
    assertFalse(canDrop(ltEq(doubleColumn, -0.1), missingMinMaxColumnMetas));
  }

  @Test
  public void testGt() {
    assertFalse(canDrop(gt(intColumn, 9), columnMetas));
    assertFalse(canDrop(gt(intColumn, 10), columnMetas));
    assertTrue(canDrop(gt(intColumn, 100), columnMetas));
    assertTrue(canDrop(gt(intColumn, 101), columnMetas));

    assertTrue(canDrop(gt(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(gt(intColumn, 7), nullColumnMetas));

    assertTrue(canDrop(gt(missingColumn, fromString("any")), columnMetas));

    assertFalse(canDrop(gt(intColumn, 0), missingMinMaxColumnMetas));
    assertFalse(canDrop(gt(doubleColumn, 0.0), missingMinMaxColumnMetas));
  }

  @Test
  public void testGtEq() {
    assertFalse(canDrop(gtEq(intColumn, 9), columnMetas));
    assertFalse(canDrop(gtEq(intColumn, 10), columnMetas));
    assertFalse(canDrop(gtEq(intColumn, 100), columnMetas));
    assertTrue(canDrop(gtEq(intColumn, 101), columnMetas));

    assertTrue(canDrop(gtEq(intColumn, 0), nullColumnMetas));
    assertTrue(canDrop(gtEq(intColumn, 7), nullColumnMetas));

    assertTrue(canDrop(gtEq(missingColumn, fromString("any")), columnMetas));

    assertFalse(canDrop(gtEq(intColumn, 1), missingMinMaxColumnMetas));
    assertFalse(canDrop(gtEq(doubleColumn, 0.1), missingMinMaxColumnMetas));
  }

  @Test
  public void testInNotIn() {
    Set<Integer> values1 = new HashSet<>();
    values1.add(10);
    values1.add(12);
    values1.add(15);
    values1.add(17);
    values1.add(19);
    assertFalse(canDrop(in(intColumn, values1), columnMetas));
    assertFalse(canDrop(notIn(intColumn, values1), columnMetas));

    Set<Integer> values2 = new HashSet<>();
    values2.add(109);
    values2.add(2);
    values2.add(5);
    values2.add(117);
    values2.add(101);
    assertFalse(canDrop(in(intColumn, values2), columnMetas));
    assertFalse(canDrop(notIn(intColumn, values2), columnMetas));

    Set<Integer> values3 = new HashSet<>();
    values3.add(1);
    values3.add(2);
    values3.add(5);
    values3.add(7);
    values3.add(10);
    assertFalse(canDrop(in(intColumn, values3), columnMetas));
    assertFalse(canDrop(notIn(intColumn, values3), columnMetas));

    Set<Integer> values4 = new HashSet<>();
    values4.add(50);
    values4.add(60);
    assertFalse(canDrop(in(intColumn, values4), missingMinMaxColumnMetas));
    assertFalse(canDrop(notIn(intColumn, values4), missingMinMaxColumnMetas));

    Set<Double> values5 = new HashSet<>();
    values5.add(1.0);
    values5.add(2.0);
    values5.add(95.0);
    values5.add(107.0);
    values5.add(99.0);
    assertFalse(canDrop(in(doubleColumn, values5), columnMetas));
    assertFalse(canDrop(notIn(doubleColumn, values5), columnMetas));

    Set<Binary> values6 = new HashSet<>();
    values6.add(Binary.fromString("test1"));
    values6.add(Binary.fromString("test2"));
    assertTrue(canDrop(in(missingColumn, values6), columnMetas));
    assertFalse(canDrop(notIn(missingColumn, values6), columnMetas));

    Set<Integer> values7 = new HashSet<>();
    values7.add(null);
    assertFalse(canDrop(in(intColumn, values7), nullColumnMetas));
    assertFalse(canDrop(notIn(intColumn, values7), nullColumnMetas));

    Set<Binary> values8 = new HashSet<>();
    values8.add(null);
    assertFalse(canDrop(in(missingColumn, values8), columnMetas));
    assertFalse(canDrop(notIn(missingColumn, values8), columnMetas));

    IntStatistics statsNoNulls = new IntStatistics();
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics();
    statsSomeNulls.setMinMax(10, 100);
    statsSomeNulls.setNumNulls(3);

    Set<Integer> values9 = new HashSet<>();
    values9.add(null);
    assertTrue(canDrop(
        in(intColumn, values9),
        List.of(getIntColumnMeta(statsNoNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        notIn(intColumn, values9),
        List.of(getIntColumnMeta(statsNoNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        in(intColumn, values9),
        List.of(getIntColumnMeta(statsSomeNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        notIn(intColumn, values9),
        List.of(getIntColumnMeta(statsSomeNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))));
  }

  @Test
  public void testContainsEqNonNull() {
    assertTrue(canDrop(contains(eq(intColumn, 9)), columnMetas));
    assertFalse(canDrop(contains(eq(intColumn, 10)), columnMetas));
    assertFalse(canDrop(contains(eq(intColumn, 100)), columnMetas));
    assertTrue(canDrop(contains(eq(intColumn, 101)), columnMetas));

    // drop columns of all nulls when looking for non-null value
    assertTrue(canDrop(contains(eq(intColumn, 0)), nullColumnMetas));
    assertFalse(canDrop(contains(eq(intColumn, 50)), missingMinMaxColumnMetas));
  }

  @Test
  public void testContainsAnd() {
    Operators.Contains<Integer> yes = contains(eq(intColumn, 9));
    Operators.Contains<Double> no = contains(eq(doubleColumn, 50D));
    assertTrue(canDrop(and(yes, yes), columnMetas));
    assertTrue(canDrop(and(yes, no), columnMetas));
    assertTrue(canDrop(and(no, yes), columnMetas));
    assertFalse(canDrop(and(no, no), columnMetas));
  }

  @Test
  public void testContainsOr() {
    Operators.Contains<Integer> yes = contains(eq(intColumn, 9));
    Operators.Contains<Double> no = contains(eq(doubleColumn, 50D));
    assertTrue(canDrop(or(yes, yes), columnMetas));
    assertFalse(canDrop(or(yes, no), columnMetas));
    assertFalse(canDrop(or(no, yes), columnMetas));
    assertFalse(canDrop(or(no, no), columnMetas));
  }

  @Test
  public void testAnd() {
    FilterPredicate yes = eq(intColumn, 9);
    FilterPredicate no = eq(doubleColumn, 50D);
    assertTrue(canDrop(and(yes, yes), columnMetas));
    assertTrue(canDrop(and(yes, no), columnMetas));
    assertTrue(canDrop(and(no, yes), columnMetas));
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
      if (value == null) {
        return true;
      }
      throw new RuntimeException("this method should not be called with value != null");
    }

    @Override
    public boolean canDrop(Statistics<Integer> statistics) {
      return statistics.getMin() == 7 && statistics.getMax() == 7;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Integer> statistics) {
      return statistics.getMin() == 8 && statistics.getMax() == 8;
    }
  }

  public static class DropNullUdp extends SevensAndEightsUdp {
    @Override
    public boolean keep(Integer value) {
      if (value == null) {
        return false;
      }
      throw new RuntimeException("this method should not be called with value != null");
    }
  }

  public static class AllPositiveUdp extends UserDefinedPredicate<Double> {
    @Override
    public boolean keep(Double value) {
      if (value == null) {
        return true;
      }
      throw new RuntimeException("this method should not be called with value != null");
    }

    @Override
    public boolean canDrop(Statistics<Double> statistics) {
      return statistics.getMin() <= 0.0;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Double> statistics) {
      return statistics.getMin() > 0.0;
    }
  }

  @Test
  public void testUdp() {
    FilterPredicate pred = userDefined(intColumn, SevensAndEightsUdp.class);
    FilterPredicate invPred = LogicalInverseRewriter.rewrite(not(userDefined(intColumn, SevensAndEightsUdp.class)));

    FilterPredicate udpDropMissingColumn = userDefined(missingColumn2, DropNullUdp.class);
    FilterPredicate invUdpDropMissingColumn =
        LogicalInverseRewriter.rewrite(not(userDefined(missingColumn2, DropNullUdp.class)));

    FilterPredicate udpKeepMissingColumn = userDefined(missingColumn2, SevensAndEightsUdp.class);
    FilterPredicate invUdpKeepMissingColumn =
        LogicalInverseRewriter.rewrite(not(userDefined(missingColumn2, SevensAndEightsUdp.class)));

    FilterPredicate allPositivePred = userDefined(doubleColumn, AllPositiveUdp.class);

    IntStatistics seven = new IntStatistics();
    seven.setMinMax(7, 7);

    IntStatistics eight = new IntStatistics();
    eight.setMinMax(8, 8);

    IntStatistics neither = new IntStatistics();
    neither.setMinMax(1, 2);

    assertTrue(canDrop(pred, List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(pred, List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(pred, List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(invPred, List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(invPred, List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(invPred, List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    // udpDropMissingColumn drops null column.
    assertTrue(canDrop(
        udpDropMissingColumn, List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(
        udpDropMissingColumn, List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(
        udpDropMissingColumn,
        List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    // invUdpDropMissingColumn (i.e., not(udpDropMissingColumn)) keeps null column.
    assertFalse(canDrop(
        invUdpDropMissingColumn,
        List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        invUdpDropMissingColumn,
        List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        invUdpDropMissingColumn,
        List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    // udpKeepMissingColumn keeps null column.
    assertFalse(canDrop(
        udpKeepMissingColumn, List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        udpKeepMissingColumn, List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(
        udpKeepMissingColumn,
        List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    // invUdpKeepMissingColumn (i.e., not(udpKeepMissingColumn)) drops null column.
    assertTrue(canDrop(
        invUdpKeepMissingColumn,
        List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(
        invUdpKeepMissingColumn,
        List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(
        invUdpKeepMissingColumn,
        List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(allPositivePred, missingMinMaxColumnMetas));
  }

  @Test
  public void testClearExceptionForNots() {
    List<ColumnChunkMetaData> columnMetas =
        List.of(getDoubleColumnMeta(new DoubleStatistics(), 0L), getIntColumnMeta(new IntStatistics(), 0L));

    FilterPredicate pred = and(not(eq(doubleColumn, 12.0)), eq(intColumn, 17));

    try {
      canDrop(pred, columnMetas);
      fail("This should throw");
    } catch (IllegalArgumentException e) {
      assertEquals(
          "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter?"
              + " not(eq(double.column, 12.0))",
          e.getMessage());
    }
  }

  private static final FloatColumn floatCol = floatColumn("float.column");

  private static ColumnChunkMetaData getFloatColumnMeta(
      org.apache.parquet.column.statistics.Statistics<?> stats, long valueCount) {
    return ColumnChunkMetaData.get(
        ColumnPath.get("float", "column"),
        PrimitiveTypeName.FLOAT,
        CompressionCodecName.GZIP,
        new HashSet<>(List.of(Encoding.PLAIN)),
        stats,
        0L,
        0L,
        valueCount,
        0L,
        0L);
  }

  private static ColumnChunkMetaData getDoubleColumnMetaWithType(
      PrimitiveType type, org.apache.parquet.column.statistics.Statistics<?> stats, long valueCount) {
    return ColumnChunkMetaData.get(
        ColumnPath.get("double", "column"),
        type,
        CompressionCodecName.GZIP,
        null,
        new HashSet<>(List.of(Encoding.PLAIN)),
        stats,
        0L,
        0L,
        valueCount,
        0L,
        0L);
  }

  // ========================= Double NaN Tests =========================

  @Test
  public void testNaNDoubleAllNaN() {
    // All non-null values are NaN, TYPE_DEFINED_ORDER (no min/max set)
    DoubleStatistics allNanStats = new DoubleStatistics();
    allNanStats.setNumNulls(0);
    allNanStats.incrementNanCount(177);

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(intStats, 177L), getDoubleColumnMeta(allNanStats, 177L));

    assertTrue(canDrop(eq(doubleColumn, 5.0), metas));
    assertFalse(canDrop(notEq(doubleColumn, 5.0), metas));
    assertFalse(canDrop(lt(doubleColumn, 5.0), metas));
    assertFalse(canDrop(ltEq(doubleColumn, 5.0), metas));
    assertFalse(canDrop(gt(doubleColumn, 5.0), metas));
    assertFalse(canDrop(gtEq(doubleColumn, 5.0), metas));
    assertTrue(canDrop(in(doubleColumn, new HashSet<>(List.of(5.0))), metas));

    assertFalse(canDrop(eq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(notEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(lt(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(ltEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(gt(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(gtEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(in(doubleColumn, new HashSet<>(List.of(Double.NaN))), metas));
  }

  @Test
  public void testNaNDoubleMixed() {
    // Mixed: nan_count=50, null_count=0, min=10, max=100, valueCount=177
    DoubleStatistics mixedStats = new DoubleStatistics();
    mixedStats.setMinMax(10, 100);
    mixedStats.setNumNulls(0);
    mixedStats.incrementNanCount(50);

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(intStats, 177L), getDoubleColumnMeta(mixedStats, 177L));

    // Non-NaN literal within range: cannot drop
    assertFalse(canDrop(eq(doubleColumn, 50.0), metas));
    assertFalse(canDrop(notEq(doubleColumn, 50.0), metas));
    assertFalse(canDrop(lt(doubleColumn, 50.0), metas));
    assertFalse(canDrop(ltEq(doubleColumn, 50.0), metas));
    assertFalse(canDrop(gt(doubleColumn, 50.0), metas));
    assertFalse(canDrop(gtEq(doubleColumn, 50.0), metas));
    assertFalse(canDrop(in(doubleColumn, new HashSet<>(List.of(50.0))), metas));

    // NaN literal: NaN values are present so cannot drop
    assertFalse(canDrop(eq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(notEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(lt(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(ltEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(gt(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(gtEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(in(doubleColumn, new HashSet<>(List.of(Double.NaN))), metas));
  }

  @Test
  public void testNaNDoubleZeroNaNCount() {
    // Explicit zero NaN count with valid min/max
    DoubleStatistics zeroNanStats = new DoubleStatistics();
    zeroNanStats.setMinMax(10, 100);
    zeroNanStats.setNumNulls(0);
    zeroNanStats.incrementNanCount(0);

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(intStats, 177L), getDoubleColumnMeta(zeroNanStats, 177L));

    assertFalse(canDrop(eq(doubleColumn, 50.0), metas));
    assertFalse(canDrop(notEq(doubleColumn, 50.0), metas));
    assertFalse(canDrop(lt(doubleColumn, 50.0), metas));
    assertFalse(canDrop(ltEq(doubleColumn, 50.0), metas));
    assertFalse(canDrop(gt(doubleColumn, 50.0), metas));
    assertFalse(canDrop(gtEq(doubleColumn, 50.0), metas));
    assertFalse(canDrop(in(doubleColumn, new HashSet<>(List.of(50.0))), metas));

    assertTrue(canDrop(eq(doubleColumn, Double.NaN), metas));
    assertTrue(canDrop(notEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(lt(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(ltEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(gt(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(gtEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(in(doubleColumn, new HashSet<>(List.of(Double.NaN))), metas));
  }

  @Test
  public void testNaNDoubleIeee754TotalOrder() {
    // All-NaN with IEEE_754_TOTAL_ORDER — stats built via builder (no min/max)
    PrimitiveType ieee754Type = Types.required(PrimitiveTypeName.DOUBLE)
        .columnOrder(ColumnOrder.ieee754TotalOrder())
        .named("test_double");
    org.apache.parquet.column.statistics.Statistics<?> allNanStats =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(ieee754Type)
            .withNumNulls(0)
            .withNanCount(177)
            .build();

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(intStats, 177L), getDoubleColumnMetaWithType(ieee754Type, allNanStats, 177L));

    assertTrue(canDrop(eq(doubleColumn, 5.0), metas));
    assertFalse(canDrop(notEq(doubleColumn, 5.0), metas));
    assertFalse(canDrop(lt(doubleColumn, 5.0), metas));
    assertFalse(canDrop(ltEq(doubleColumn, 5.0), metas));
    assertFalse(canDrop(gt(doubleColumn, 5.0), metas));
    assertFalse(canDrop(gtEq(doubleColumn, 5.0), metas));
    assertTrue(canDrop(in(doubleColumn, new HashSet<>(List.of(5.0))), metas));

    assertFalse(canDrop(eq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(notEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(lt(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(ltEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(gt(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(gtEq(doubleColumn, Double.NaN), metas));
    assertFalse(canDrop(in(doubleColumn, new HashSet<>(List.of(Double.NaN))), metas));
  }

  // ========================= Float NaN Tests =========================

  @Test
  public void testNaNFloatAllNaN() {
    // All non-null values are NaN
    FloatStatistics allNanStats = new FloatStatistics();
    allNanStats.setNumNulls(0);
    allNanStats.incrementNanCount(177);

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(intStats, 177L), getFloatColumnMeta(allNanStats, 177L));

    assertTrue(canDrop(eq(floatCol, 5.0f), metas));
    assertFalse(canDrop(notEq(floatCol, 5.0f), metas));
    assertFalse(canDrop(lt(floatCol, 5.0f), metas));
    assertFalse(canDrop(ltEq(floatCol, 5.0f), metas));
    assertFalse(canDrop(gt(floatCol, 5.0f), metas));
    assertFalse(canDrop(gtEq(floatCol, 5.0f), metas));
    assertTrue(canDrop(in(floatCol, new HashSet<>(List.of(5.0f))), metas));

    assertFalse(canDrop(eq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(notEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(lt(floatCol, Float.NaN), metas));
    assertFalse(canDrop(ltEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(gt(floatCol, Float.NaN), metas));
    assertFalse(canDrop(gtEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(in(floatCol, new HashSet<>(List.of(Float.NaN))), metas));
  }

  @Test
  public void testNaNFloatMixed() {
    // Mixed: nan_count=50, min=10, max=100
    FloatStatistics mixedStats = new FloatStatistics();
    mixedStats.setMinMax(10.0f, 100.0f);
    mixedStats.setNumNulls(0);
    mixedStats.incrementNanCount(50);

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(intStats, 177L), getFloatColumnMeta(mixedStats, 177L));

    assertFalse(canDrop(eq(floatCol, 50.0f), metas));
    assertFalse(canDrop(notEq(floatCol, 50.0f), metas));
    assertFalse(canDrop(lt(floatCol, 50.0f), metas));
    assertFalse(canDrop(ltEq(floatCol, 50.0f), metas));
    assertFalse(canDrop(gt(floatCol, 50.0f), metas));
    assertFalse(canDrop(gtEq(floatCol, 50.0f), metas));
    assertFalse(canDrop(in(floatCol, new HashSet<>(List.of(50.0f))), metas));

    assertFalse(canDrop(eq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(notEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(lt(floatCol, Float.NaN), metas));
    assertFalse(canDrop(ltEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(gt(floatCol, Float.NaN), metas));
    assertFalse(canDrop(gtEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(in(floatCol, new HashSet<>(List.of(Float.NaN))), metas));
  }

  @Test
  public void testNaNFloatZeroNaNCount() {
    // Zero NaN count with valid min/max
    FloatStatistics zeroNanStats = new FloatStatistics();
    zeroNanStats.setMinMax(10.0f, 100.0f);
    zeroNanStats.setNumNulls(0);
    zeroNanStats.incrementNanCount(0);

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(intStats, 177L), getFloatColumnMeta(zeroNanStats, 177L));

    assertFalse(canDrop(eq(floatCol, 50.0f), metas));
    assertFalse(canDrop(notEq(floatCol, 50.0f), metas));
    assertFalse(canDrop(lt(floatCol, 50.0f), metas));
    assertFalse(canDrop(ltEq(floatCol, 50.0f), metas));
    assertFalse(canDrop(gt(floatCol, 50.0f), metas));
    assertFalse(canDrop(gtEq(floatCol, 50.0f), metas));
    assertFalse(canDrop(in(floatCol, new HashSet<>(List.of(50.0f))), metas));

    assertTrue(canDrop(eq(floatCol, Float.NaN), metas));
    assertTrue(canDrop(notEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(lt(floatCol, Float.NaN), metas));
    assertFalse(canDrop(ltEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(gt(floatCol, Float.NaN), metas));
    assertFalse(canDrop(gtEq(floatCol, Float.NaN), metas));
    assertFalse(canDrop(in(floatCol, new HashSet<>(List.of(Float.NaN))), metas));
  }

  // ========================= Float16 NaN Tests =========================

  private static final PrimitiveType FLOAT16_TYPE = Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
      .length(2)
      .as(LogicalTypeAnnotation.float16Type())
      .named("test_float16");

  private static final Binary FLOAT16_NAN = Binary.fromConstantByteArray(new byte[] {0x00, 0x7e});
  private static final Binary FLOAT16_ONE = Binary.fromConstantByteArray(new byte[] {0x00, 0x3c});
  private static final Binary FLOAT16_TEN = Binary.fromConstantByteArray(new byte[] {0x00, 0x49});
  private static final Binary FLOAT16_HUNDRED = Binary.fromConstantByteArray(new byte[] {0x40, 0x56});
  private static final Binary FLOAT16_FIFTY = Binary.fromConstantByteArray(new byte[] {0x40, 0x52});

  private static final Operators.BinaryColumn float16Column = binaryColumn("float16.column");

  private static ColumnChunkMetaData getFloat16ColumnMeta(
      org.apache.parquet.column.statistics.Statistics<?> stats, long valueCount) {
    return ColumnChunkMetaData.get(
        ColumnPath.get("float16", "column"),
        FLOAT16_TYPE,
        CompressionCodecName.GZIP,
        null,
        new HashSet<>(List.of(Encoding.PLAIN)),
        stats,
        0L,
        0L,
        valueCount,
        0L,
        0L);
  }

  @Test
  public void testNaNFloat16AllNaN() {
    // All non-null values are NaN
    org.apache.parquet.column.statistics.Statistics<?> allNanStats =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(FLOAT16_TYPE)
            .withNumNulls(0)
            .withNanCount(177)
            .build();

    ColumnChunkMetaData float16Meta = getFloat16ColumnMeta(allNanStats, 177L);
    List<ColumnChunkMetaData> metas = List.of(getIntColumnMeta(intStats, 177L), float16Meta);

    assertTrue(canDrop(eq(float16Column, FLOAT16_ONE), metas));
    assertFalse(canDrop(notEq(float16Column, FLOAT16_ONE), metas));
    assertFalse(canDrop(lt(float16Column, FLOAT16_ONE), metas));
    assertFalse(canDrop(ltEq(float16Column, FLOAT16_ONE), metas));
    assertFalse(canDrop(gt(float16Column, FLOAT16_ONE), metas));
    assertFalse(canDrop(gtEq(float16Column, FLOAT16_ONE), metas));
    assertTrue(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_ONE))), metas));

    assertFalse(canDrop(eq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(notEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(lt(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(ltEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(gt(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(gtEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_NAN))), metas));
  }

  @Test
  public void testNaNFloat16Mixed() {
    // Mixed: nan_count=50, min=10, max=100
    org.apache.parquet.column.statistics.Statistics<?> mixedStats =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(FLOAT16_TYPE)
            .withMin(FLOAT16_TEN.getBytes())
            .withMax(FLOAT16_HUNDRED.getBytes())
            .withNumNulls(0)
            .withNanCount(50)
            .build();

    ColumnChunkMetaData float16Meta = getFloat16ColumnMeta(mixedStats, 177L);
    List<ColumnChunkMetaData> metas = List.of(getIntColumnMeta(intStats, 177L), float16Meta);

    assertFalse(canDrop(eq(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(notEq(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(lt(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(ltEq(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(gt(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(gtEq(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_FIFTY))), metas));

    assertFalse(canDrop(eq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(notEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(lt(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(ltEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(gt(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(gtEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_NAN))), metas));
  }

  @Test
  public void testNaNFloat16ZeroNaNCount() {
    // Zero NaN count with valid min/max
    org.apache.parquet.column.statistics.Statistics<?> zeroNanStats =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(FLOAT16_TYPE)
            .withMin(FLOAT16_TEN.getBytes())
            .withMax(FLOAT16_HUNDRED.getBytes())
            .withNumNulls(0)
            .withNanCount(0)
            .build();

    ColumnChunkMetaData float16Meta = getFloat16ColumnMeta(zeroNanStats, 177L);
    List<ColumnChunkMetaData> metas = List.of(getIntColumnMeta(intStats, 177L), float16Meta);

    assertFalse(canDrop(eq(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(notEq(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(lt(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(ltEq(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(gt(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(gtEq(float16Column, FLOAT16_FIFTY), metas));
    assertFalse(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_FIFTY))), metas));

    assertTrue(canDrop(eq(float16Column, FLOAT16_NAN), metas));
    assertTrue(canDrop(notEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(lt(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(ltEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(gt(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(gtEq(float16Column, FLOAT16_NAN), metas));
    assertFalse(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_NAN))), metas));
  }
}
