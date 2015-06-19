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

import org.apache.parquet.column.statistics.StatisticsOpts;
import org.junit.Test;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.statisticslevel.StatisticsFilter.canDrop;

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

  private static final IntColumn intColumn = intColumn("int.column");
  private static final DoubleColumn doubleColumn = doubleColumn("double.column");

  private static final IntStatistics intStats = new IntStatistics(new StatisticsOpts(null));
  private static final IntStatistics nullIntStats = new IntStatistics(new StatisticsOpts(null));
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
    IntStatistics statsNoNulls = new IntStatistics(new StatisticsOpts(null));
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics(new StatisticsOpts(null));
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

    IntStatistics allSevens = new IntStatistics(new StatisticsOpts(null));
    allSevens.setMinMax(7, 7);
    assertTrue(canDrop(notEq(intColumn, 7), Arrays.asList(
        getIntColumnMeta(allSevens, 177L),
        getDoubleColumnMeta(doubleStats, 177L))));

  }

  @Test
  public void testNotEqNull() {
    IntStatistics statsNoNulls = new IntStatistics(new StatisticsOpts(null));
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics(new StatisticsOpts(null));
    statsSomeNulls.setMinMax(10, 100);
    statsSomeNulls.setNumNulls(3);

    IntStatistics statsAllNulls = new IntStatistics(new StatisticsOpts(null));
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
      throw new RuntimeException("this method should not be called");
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

  @Test
  public void testUdp() {
    FilterPredicate pred = userDefined(intColumn, SevensAndEightsUdp.class);
    FilterPredicate invPred = LogicalInverseRewriter.rewrite(not(userDefined(intColumn, SevensAndEightsUdp.class)));

    IntStatistics seven = new IntStatistics(new StatisticsOpts(null));
    seven.setMinMax(7, 7);

    IntStatistics eight = new IntStatistics(new StatisticsOpts(null));
    eight.setMinMax(8, 8);

    IntStatistics neither = new IntStatistics(new StatisticsOpts(null));
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
        getDoubleColumnMeta(new DoubleStatistics(), 0L),
        getIntColumnMeta(new IntStatistics(null), 0L));

    FilterPredicate pred = and(not(eq(doubleColumn, 12.0)), eq(intColumn, 17));

    try {
      canDrop(pred, columnMetas);
      fail("This should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter?"
          + " not(eq(double.column, 12.0))", e.getMessage());
    }
  }

  @Test
  public void testMissingColumn() {
    List<ColumnChunkMetaData> columnMetas =
        Arrays.asList(getIntColumnMeta(new IntStatistics(new StatisticsOpts(null)), 0L));
    try {
      canDrop(and(eq(doubleColumn, 12.0), eq(intColumn, 17)), columnMetas);
      fail("This should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("Column double.column not found in schema!", e.getMessage());
    }
  }

}
