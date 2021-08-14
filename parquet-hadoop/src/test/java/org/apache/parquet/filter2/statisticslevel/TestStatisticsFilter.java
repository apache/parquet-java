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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.io.api.Binary.fromString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
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

public class TestStatisticsFilter {

  private static ColumnChunkMetaData getIntColumnMeta(org.apache.parquet.column.statistics.Statistics<?> stats,
      long valueCount) {
    return ColumnChunkMetaData.get(ColumnPath.get("int", "column"),
        PrimitiveTypeName.INT32,
        CompressionCodecName.GZIP,
        new HashSet<Encoding>(Arrays.asList(Encoding.PLAIN)),
        stats,
        0L, 0L, valueCount, 0L, 0L);
  }

  private static ColumnChunkMetaData getDoubleColumnMeta(org.apache.parquet.column.statistics.Statistics<?> stats,
      long valueCount) {
    return ColumnChunkMetaData.get(ColumnPath.get("double", "column"),
        PrimitiveTypeName.DOUBLE,
        CompressionCodecName.GZIP,
        new HashSet<Encoding>(Arrays.asList(Encoding.PLAIN)),
        stats,
        0L, 0L, valueCount, 0L, 0L);
  }

  private static final IntColumn intColumn = intColumn("int.column");
  private static final DoubleColumn doubleColumn = doubleColumn("double.column");
  private static final BinaryColumn missingColumn = binaryColumn("missing");
  private static final IntColumn missingColumn2 = intColumn("missing.int");

  private static final IntStatistics intStats = new IntStatistics();
  private static final IntStatistics nullIntStats = new IntStatistics();
  private static final org.apache.parquet.column.statistics.Statistics<?> emptyIntStats = org.apache.parquet.column.statistics.Statistics
      .getBuilderForReading(Types.required(PrimitiveTypeName.INT32).named("test_int32")).build();
  private static final DoubleStatistics doubleStats = new DoubleStatistics();
  private static final org.apache.parquet.column.statistics.Statistics<?> missingMinMaxDoubleStats = org.apache.parquet.column.statistics.Statistics
      .getBuilderForReading(Types.required(PrimitiveTypeName.DOUBLE).named("test_double")).withNumNulls(100).build();

  static {
    intStats.setMinMax(10, 100);
    doubleStats.setMinMax(10, 100);

    nullIntStats.setNumNulls(177);
  }

  private static final List<ColumnChunkMetaData> columnMetas = Arrays.asList(
      getIntColumnMeta(intStats, 177L),
      getDoubleColumnMeta(doubleStats, 177L));

  private static final List<ColumnChunkMetaData> nullColumnMetas = Arrays.asList(
      getIntColumnMeta(nullIntStats, 177L), // column of all nulls
      getDoubleColumnMeta(doubleStats, 177L));

  private static final List<ColumnChunkMetaData> missingMinMaxColumnMetas = Arrays.asList(
      getIntColumnMeta(emptyIntStats, 177L),                // missing min/max values and numNulls => stats is empty
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

    assertTrue(canDrop(eq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsNoNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(eq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsSomeNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

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
    assertTrue(canDrop(notEq(intColumn, 7), Arrays.asList(
        getIntColumnMeta(allSevens, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    allSevens.setNumNulls(100L);
    assertFalse(canDrop(notEq(intColumn, 7), Arrays.asList(
        getIntColumnMeta(allSevens, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    allSevens.setNumNulls(177L);
    assertFalse(canDrop(notEq(intColumn, 7), Arrays.asList(
        getIntColumnMeta(allSevens, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

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

    assertFalse(canDrop(notEq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsNoNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(notEq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsSomeNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(notEq(intColumn, null), Arrays.asList(
        getIntColumnMeta(statsAllNulls, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

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
    assertTrue((canDrop(in(intColumn, values2), columnMetas)));
    assertFalse((canDrop(notIn(intColumn, values2), columnMetas)));

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
    assertFalse((canDrop(in(intColumn, values4), missingMinMaxColumnMetas)));
    assertFalse((canDrop(notIn(intColumn, values4), missingMinMaxColumnMetas)));

    Set<Double> values5 = new HashSet<>();
    values5.add(1.0);
    values5.add(2.0);
    values5.add(95.0);
    values5.add(107.0);
    values5.add(99.0);
    assertFalse((canDrop(in(doubleColumn, values5), columnMetas)));
    assertFalse((canDrop(notIn(doubleColumn, values5), columnMetas)));

    Set<Binary> values6 = new HashSet<>();
    values6.add(Binary.fromString("test1"));
    values6.add(Binary.fromString("test2"));
    assertTrue((canDrop(in(missingColumn, values6), columnMetas)));
    assertFalse((canDrop(notIn(missingColumn, values6), columnMetas)));

    Set<Integer> values7 = new HashSet<>();
    values7.add(null);
    assertFalse((canDrop(in(intColumn, values7), nullColumnMetas)));
    assertFalse((canDrop(notIn(intColumn, values7), nullColumnMetas)));

    Set<Binary> values8 = new HashSet<>();
    values8.add(null);
    assertFalse((canDrop(in(missingColumn, values8), columnMetas)));
    assertFalse((canDrop(notIn(missingColumn, values8), columnMetas)));

    IntStatistics statsNoNulls = new IntStatistics();
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics();
    statsSomeNulls.setMinMax(10, 100);
    statsSomeNulls.setNumNulls(3);

    Set<Integer> values9 = new HashSet<>();
    values9.add(null);
    assertTrue(canDrop(in(intColumn, values9), Arrays.asList(
      getIntColumnMeta(statsNoNulls, 177L),
      getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(notIn(intColumn, values9), Arrays.asList(
      getIntColumnMeta(statsNoNulls, 177L),
      getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(in(intColumn, values9), Arrays.asList(
      getIntColumnMeta(statsSomeNulls, 177L),
      getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(notIn(intColumn, values9), Arrays.asList(
      getIntColumnMeta(statsSomeNulls, 177L),
      getDoubleColumnMeta(doubleStats, 177L))));
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
    FilterPredicate invUdpDropMissingColumn = LogicalInverseRewriter.rewrite(not(userDefined(missingColumn2, DropNullUdp.class)));

    FilterPredicate udpKeepMissingColumn = userDefined(missingColumn2, SevensAndEightsUdp.class);
    FilterPredicate invUdpKeepMissingColumn = LogicalInverseRewriter.rewrite(not(userDefined(missingColumn2, SevensAndEightsUdp.class)));

    FilterPredicate allPositivePred = userDefined(doubleColumn, AllPositiveUdp.class);

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

    // udpDropMissingColumn drops null column.
    assertTrue(canDrop(udpDropMissingColumn, Arrays.asList(
        getIntColumnMeta(seven, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(udpDropMissingColumn, Arrays.asList(
        getIntColumnMeta(eight, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(udpDropMissingColumn, Arrays.asList(
        getIntColumnMeta(neither, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    // invUdpDropMissingColumn (i.e., not(udpDropMissingColumn)) keeps null column.
    assertFalse(canDrop(invUdpDropMissingColumn, Arrays.asList(
        getIntColumnMeta(seven, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(invUdpDropMissingColumn, Arrays.asList(
        getIntColumnMeta(eight, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(invUdpDropMissingColumn, Arrays.asList(
        getIntColumnMeta(neither, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    // udpKeepMissingColumn keeps null column.
    assertFalse(canDrop(udpKeepMissingColumn, Arrays.asList(
        getIntColumnMeta(seven, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(udpKeepMissingColumn, Arrays.asList(
        getIntColumnMeta(eight, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(udpKeepMissingColumn, Arrays.asList(
        getIntColumnMeta(neither, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    // invUdpKeepMissingColumn (i.e., not(udpKeepMissingColumn)) drops null column.
    assertTrue(canDrop(invUdpKeepMissingColumn, Arrays.asList(
        getIntColumnMeta(seven, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(invUdpKeepMissingColumn, Arrays.asList(
        getIntColumnMeta(eight, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertTrue(canDrop(invUdpKeepMissingColumn, Arrays.asList(
        getIntColumnMeta(neither, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

    assertFalse(canDrop(allPositivePred, missingMinMaxColumnMetas));
  }

  @Test
  public void testClearExceptionForNots() {
    List<ColumnChunkMetaData> columnMetas = Arrays.asList(
        getDoubleColumnMeta(new DoubleStatistics(), 0L),
        getIntColumnMeta(new IntStatistics(), 0L));

    FilterPredicate pred = and(not(eq(doubleColumn, 12.0)), eq(intColumn, 17));

    try {
      canDrop(pred, columnMetas);
      fail("This should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter?"
          + " not(eq(double.column, 12.0))", e.getMessage());
    }
  }

}
