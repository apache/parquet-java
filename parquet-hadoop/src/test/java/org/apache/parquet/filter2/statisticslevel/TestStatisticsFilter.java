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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.parquet.bytes.BytesUtils;
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
    assertThat(canDrop(eq(intColumn, 9), columnMetas)).isTrue();
    assertThat(canDrop(eq(intColumn, 10), columnMetas)).isFalse();
    assertThat(canDrop(eq(intColumn, 100), columnMetas)).isFalse();
    assertThat(canDrop(eq(intColumn, 101), columnMetas)).isTrue();

    // drop columns of all nulls when looking for non-null value
    assertThat(canDrop(eq(intColumn, 0), nullColumnMetas)).isTrue();
    assertThat(canDrop(eq(missingColumn, fromString("any")), columnMetas)).isTrue();

    assertThat(canDrop(eq(intColumn, 50), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(eq(doubleColumn, 50.0), missingMinMaxColumnMetas)).isFalse();
  }

  @Test
  public void testEqNull() {
    IntStatistics statsNoNulls = new IntStatistics();
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics();
    statsSomeNulls.setMinMax(10, 100);
    statsSomeNulls.setNumNulls(3);

    assertThat(canDrop(
            eq(intColumn, null),
            List.of(getIntColumnMeta(statsNoNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(
            eq(intColumn, null),
            List.of(getIntColumnMeta(statsSomeNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(eq(missingColumn, null), columnMetas)).isFalse();

    assertThat(canDrop(eq(intColumn, null), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(eq(doubleColumn, null), missingMinMaxColumnMetas)).isFalse();
  }

  @Test
  public void testNotEqNonNull() {
    assertThat(canDrop(notEq(intColumn, 9), columnMetas)).isFalse();
    assertThat(canDrop(notEq(intColumn, 10), columnMetas)).isFalse();
    assertThat(canDrop(notEq(intColumn, 100), columnMetas)).isFalse();
    assertThat(canDrop(notEq(intColumn, 101), columnMetas)).isFalse();

    IntStatistics allSevens = new IntStatistics();
    allSevens.setMinMax(7, 7);
    assertThat(canDrop(
            notEq(intColumn, 7),
            List.of(getIntColumnMeta(allSevens, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    allSevens.setNumNulls(100L);
    assertThat(canDrop(
            notEq(intColumn, 7),
            List.of(getIntColumnMeta(allSevens, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    allSevens.setNumNulls(177L);
    assertThat(canDrop(
            notEq(intColumn, 7),
            List.of(getIntColumnMeta(allSevens, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(notEq(missingColumn, fromString("any")), columnMetas))
        .isFalse();

    assertThat(canDrop(notEq(intColumn, 50), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(notEq(doubleColumn, 50.0), missingMinMaxColumnMetas)).isFalse();
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

    assertThat(canDrop(
            notEq(intColumn, null),
            List.of(getIntColumnMeta(statsNoNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(
            notEq(intColumn, null),
            List.of(getIntColumnMeta(statsSomeNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(
            notEq(intColumn, null),
            List.of(getIntColumnMeta(statsAllNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(notEq(missingColumn, null), columnMetas)).isTrue();

    assertThat(canDrop(notEq(intColumn, null), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(notEq(doubleColumn, null), missingMinMaxColumnMetas)).isFalse();
  }

  @Test
  public void testLt() {
    assertThat(canDrop(lt(intColumn, 9), columnMetas)).isTrue();
    assertThat(canDrop(lt(intColumn, 10), columnMetas)).isTrue();
    assertThat(canDrop(lt(intColumn, 100), columnMetas)).isFalse();
    assertThat(canDrop(lt(intColumn, 101), columnMetas)).isFalse();

    assertThat(canDrop(lt(intColumn, 0), nullColumnMetas)).isTrue();
    assertThat(canDrop(lt(intColumn, 7), nullColumnMetas)).isTrue();

    assertThat(canDrop(lt(missingColumn, fromString("any")), columnMetas)).isTrue();

    assertThat(canDrop(lt(intColumn, 0), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, 0.0), missingMinMaxColumnMetas)).isFalse();
  }

  @Test
  public void testLtEq() {
    assertThat(canDrop(ltEq(intColumn, 9), columnMetas)).isTrue();
    assertThat(canDrop(ltEq(intColumn, 10), columnMetas)).isFalse();
    assertThat(canDrop(ltEq(intColumn, 100), columnMetas)).isFalse();
    assertThat(canDrop(ltEq(intColumn, 101), columnMetas)).isFalse();

    assertThat(canDrop(ltEq(intColumn, 0), nullColumnMetas)).isTrue();
    assertThat(canDrop(ltEq(intColumn, 7), nullColumnMetas)).isTrue();

    assertThat(canDrop(ltEq(missingColumn, fromString("any")), columnMetas)).isTrue();

    assertThat(canDrop(ltEq(intColumn, -1), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, -0.1), missingMinMaxColumnMetas)).isFalse();
  }

  @Test
  public void testGt() {
    assertThat(canDrop(gt(intColumn, 9), columnMetas)).isFalse();
    assertThat(canDrop(gt(intColumn, 10), columnMetas)).isFalse();
    assertThat(canDrop(gt(intColumn, 100), columnMetas)).isTrue();
    assertThat(canDrop(gt(intColumn, 101), columnMetas)).isTrue();

    assertThat(canDrop(gt(intColumn, 0), nullColumnMetas)).isTrue();
    assertThat(canDrop(gt(intColumn, 7), nullColumnMetas)).isTrue();

    assertThat(canDrop(gt(missingColumn, fromString("any")), columnMetas)).isTrue();

    assertThat(canDrop(gt(intColumn, 0), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, 0.0), missingMinMaxColumnMetas)).isFalse();
  }

  @Test
  public void testGtEq() {
    assertThat(canDrop(gtEq(intColumn, 9), columnMetas)).isFalse();
    assertThat(canDrop(gtEq(intColumn, 10), columnMetas)).isFalse();
    assertThat(canDrop(gtEq(intColumn, 100), columnMetas)).isFalse();
    assertThat(canDrop(gtEq(intColumn, 101), columnMetas)).isTrue();

    assertThat(canDrop(gtEq(intColumn, 0), nullColumnMetas)).isTrue();
    assertThat(canDrop(gtEq(intColumn, 7), nullColumnMetas)).isTrue();

    assertThat(canDrop(gtEq(missingColumn, fromString("any")), columnMetas)).isTrue();

    assertThat(canDrop(gtEq(intColumn, 1), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, 0.1), missingMinMaxColumnMetas)).isFalse();
  }

  @Test
  public void testInNotIn() {
    Set<Integer> values1 = new HashSet<>();
    values1.add(10);
    values1.add(12);
    values1.add(15);
    values1.add(17);
    values1.add(19);
    assertThat(canDrop(in(intColumn, values1), columnMetas)).isFalse();
    assertThat(canDrop(notIn(intColumn, values1), columnMetas)).isFalse();

    Set<Integer> values2 = new HashSet<>();
    values2.add(109);
    values2.add(2);
    values2.add(5);
    values2.add(117);
    values2.add(101);
    assertThat(canDrop(in(intColumn, values2), columnMetas)).isFalse();
    assertThat(canDrop(notIn(intColumn, values2), columnMetas)).isFalse();

    Set<Integer> values3 = new HashSet<>();
    values3.add(1);
    values3.add(2);
    values3.add(5);
    values3.add(7);
    values3.add(10);
    assertThat(canDrop(in(intColumn, values3), columnMetas)).isFalse();
    assertThat(canDrop(notIn(intColumn, values3), columnMetas)).isFalse();

    Set<Integer> values4 = new HashSet<>();
    values4.add(50);
    values4.add(60);
    assertThat(canDrop(in(intColumn, values4), missingMinMaxColumnMetas)).isFalse();
    assertThat(canDrop(notIn(intColumn, values4), missingMinMaxColumnMetas)).isFalse();

    Set<Double> values5 = new HashSet<>();
    values5.add(1.0);
    values5.add(2.0);
    values5.add(95.0);
    values5.add(107.0);
    values5.add(99.0);
    assertThat(canDrop(in(doubleColumn, values5), columnMetas)).isFalse();
    assertThat(canDrop(notIn(doubleColumn, values5), columnMetas)).isFalse();

    Set<Binary> values6 = new HashSet<>();
    values6.add(Binary.fromString("test1"));
    values6.add(Binary.fromString("test2"));
    assertThat(canDrop(in(missingColumn, values6), columnMetas)).isTrue();
    assertThat(canDrop(notIn(missingColumn, values6), columnMetas)).isFalse();

    Set<Integer> values7 = new HashSet<>();
    values7.add(null);
    assertThat(canDrop(in(intColumn, values7), nullColumnMetas)).isFalse();
    assertThat(canDrop(notIn(intColumn, values7), nullColumnMetas)).isFalse();

    Set<Binary> values8 = new HashSet<>();
    values8.add(null);
    assertThat(canDrop(in(missingColumn, values8), columnMetas)).isFalse();
    assertThat(canDrop(notIn(missingColumn, values8), columnMetas)).isFalse();

    IntStatistics statsNoNulls = new IntStatistics();
    statsNoNulls.setMinMax(10, 100);
    statsNoNulls.setNumNulls(0);

    IntStatistics statsSomeNulls = new IntStatistics();
    statsSomeNulls.setMinMax(10, 100);
    statsSomeNulls.setNumNulls(3);

    Set<Integer> values9 = new HashSet<>();
    values9.add(null);
    assertThat(canDrop(
            in(intColumn, values9),
            List.of(getIntColumnMeta(statsNoNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(
            notIn(intColumn, values9),
            List.of(getIntColumnMeta(statsNoNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(
            in(intColumn, values9),
            List.of(getIntColumnMeta(statsSomeNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(
            notIn(intColumn, values9),
            List.of(getIntColumnMeta(statsSomeNulls, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();
  }

  @Test
  public void testInWithNullLiteralAndUnsetNumNulls() {
    // Reproduces the bug where StatisticsFilter drops a row group for IN (..., null) when num_nulls
    // is unset. min/max are present but the number of nulls is unknown, so we must not fall through
    // to the min/max-only check (which only considers the non-null literals) and drop a chunk that
    // may contain matching null rows
    org.apache.parquet.column.statistics.Statistics<?> statsUnsetNulls =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(
                Types.required(PrimitiveTypeName.INT32).named("test_int32"))
            .withMin(BytesUtils.intToBytes(10))
            .withMax(BytesUtils.intToBytes(100))
            .build();
    // min/max are available but num_nulls is not
    assertThat(statsUnsetNulls.hasNonNullValue()).isTrue();
    assertThat(statsUnsetNulls.isNumNullsSet()).isFalse();

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(statsUnsetNulls, 177L), getDoubleColumnMeta(doubleStats, 177L));

    // IN (200, null) where 200 is outside [10, 100]. The chunk might contain null rows matching the
    // null literal, so it must NOT be dropped
    Set<Integer> valuesNullAndOutOfRange = new HashSet<>();
    valuesNullAndOutOfRange.add(null);
    valuesNullAndOutOfRange.add(200);
    assertThat(canDrop(in(intColumn, valuesNullAndOutOfRange), metas)).isFalse();

    // IN (200) without a null literal can still be dropped based on min/max even if num_nulls is
    // unknown, confirming the fix does not over-broaden pruning.
    Set<Integer> valuesOutOfRange = new HashSet<>();
    valuesOutOfRange.add(200);
    assertThat(canDrop(in(intColumn, valuesOutOfRange), metas)).isTrue();

    // IN (50, null) where 50 is inside [10, 100] must also not be dropped.
    Set<Integer> valuesNullAndInRange = new HashSet<>();
    valuesNullAndInRange.add(null);
    valuesNullAndInRange.add(50);
    assertThat(canDrop(in(intColumn, valuesNullAndInRange), metas)).isFalse();
  }

  @Test
  public void testContainsEqNonNull() {
    assertThat(canDrop(contains(eq(intColumn, 9)), columnMetas)).isTrue();
    assertThat(canDrop(contains(eq(intColumn, 10)), columnMetas)).isFalse();
    assertThat(canDrop(contains(eq(intColumn, 100)), columnMetas)).isFalse();
    assertThat(canDrop(contains(eq(intColumn, 101)), columnMetas)).isTrue();

    // drop columns of all nulls when looking for non-null value
    assertThat(canDrop(contains(eq(intColumn, 0)), nullColumnMetas)).isTrue();
    assertThat(canDrop(contains(eq(intColumn, 50)), missingMinMaxColumnMetas))
        .isFalse();
  }

  @Test
  public void testContainsAnd() {
    Operators.Contains<Integer> yes = contains(eq(intColumn, 9));
    Operators.Contains<Double> no = contains(eq(doubleColumn, 50D));
    assertThat(canDrop(and(yes, yes), columnMetas)).isTrue();
    assertThat(canDrop(and(yes, no), columnMetas)).isTrue();
    assertThat(canDrop(and(no, yes), columnMetas)).isTrue();
    assertThat(canDrop(and(no, no), columnMetas)).isFalse();
  }

  @Test
  public void testContainsOr() {
    Operators.Contains<Integer> yes = contains(eq(intColumn, 9));
    Operators.Contains<Double> no = contains(eq(doubleColumn, 50D));
    assertThat(canDrop(or(yes, yes), columnMetas)).isTrue();
    assertThat(canDrop(or(yes, no), columnMetas)).isFalse();
    assertThat(canDrop(or(no, yes), columnMetas)).isFalse();
    assertThat(canDrop(or(no, no), columnMetas)).isFalse();
  }

  @Test
  public void testAnd() {
    FilterPredicate yes = eq(intColumn, 9);
    FilterPredicate no = eq(doubleColumn, 50D);
    assertThat(canDrop(and(yes, yes), columnMetas)).isTrue();
    assertThat(canDrop(and(yes, no), columnMetas)).isTrue();
    assertThat(canDrop(and(no, yes), columnMetas)).isTrue();
    assertThat(canDrop(and(no, no), columnMetas)).isFalse();
  }

  @Test
  public void testOr() {
    FilterPredicate yes = eq(intColumn, 9);
    FilterPredicate no = eq(doubleColumn, 50D);
    assertThat(canDrop(or(yes, yes), columnMetas)).isTrue();
    assertThat(canDrop(or(yes, no), columnMetas)).isFalse();
    assertThat(canDrop(or(no, yes), columnMetas)).isFalse();
    assertThat(canDrop(or(no, no), columnMetas)).isFalse();
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

    assertThat(canDrop(pred, List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(pred, List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(pred, List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(invPred, List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(invPred, List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(invPred, List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    // udpDropMissingColumn drops null column.
    assertThat(canDrop(
            udpDropMissingColumn,
            List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(
            udpDropMissingColumn,
            List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(
            udpDropMissingColumn,
            List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    // invUdpDropMissingColumn (i.e., not(udpDropMissingColumn)) keeps null column.
    assertThat(canDrop(
            invUdpDropMissingColumn,
            List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(
            invUdpDropMissingColumn,
            List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(
            invUdpDropMissingColumn,
            List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    // udpKeepMissingColumn keeps null column.
    assertThat(canDrop(
            udpKeepMissingColumn,
            List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(
            udpKeepMissingColumn,
            List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    assertThat(canDrop(
            udpKeepMissingColumn,
            List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isFalse();

    // invUdpKeepMissingColumn (i.e., not(udpKeepMissingColumn)) drops null column.
    assertThat(canDrop(
            invUdpKeepMissingColumn,
            List.of(getIntColumnMeta(seven, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(
            invUdpKeepMissingColumn,
            List.of(getIntColumnMeta(eight, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(
            invUdpKeepMissingColumn,
            List.of(getIntColumnMeta(neither, 177L), getDoubleColumnMeta(doubleStats, 177L))))
        .isTrue();

    assertThat(canDrop(allPositivePred, missingMinMaxColumnMetas)).isFalse();
  }

  @Test
  public void testClearExceptionForNots() {
    List<ColumnChunkMetaData> columnMetas =
        List.of(getDoubleColumnMeta(new DoubleStatistics(), 0L), getIntColumnMeta(new IntStatistics(), 0L));

    FilterPredicate pred = and(not(eq(doubleColumn, 12.0)), eq(intColumn, 17));

    assertThatThrownBy(() -> canDrop(pred, columnMetas))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter?"
                + " not(eq(double.column, 12.0))");
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

    assertThat(canDrop(eq(doubleColumn, 5.0), metas)).isTrue();
    assertThat(canDrop(notEq(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(in(doubleColumn, new HashSet<>(List.of(5.0))), metas))
        .isTrue();

    assertThat(canDrop(eq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(notEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(in(doubleColumn, new HashSet<>(List.of(Double.NaN))), metas))
        .isFalse();
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
    assertThat(canDrop(eq(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(notEq(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(in(doubleColumn, new HashSet<>(List.of(50.0))), metas))
        .isFalse();
    assertThat(canDrop(lt(doubleColumn, 0.0), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, 200.0), metas)).isFalse();

    DoubleStatistics mixedEqualStats = new DoubleStatistics();
    mixedEqualStats.setMinMax(5.0, 5.0);
    mixedEqualStats.setNumNulls(0);
    mixedEqualStats.incrementNanCount(1);
    List<ColumnChunkMetaData> mixedEqualMetas =
        List.of(getIntColumnMeta(intStats, 177L), getDoubleColumnMeta(mixedEqualStats, 177L));
    assertThat(canDrop(notEq(doubleColumn, 5.0), mixedEqualMetas)).isFalse();

    // NaN literal: NaN values are present so cannot drop
    assertThat(canDrop(eq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(notEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(in(doubleColumn, new HashSet<>(List.of(Double.NaN))), metas))
        .isFalse();
  }

  @Test
  public void testNaNDoubleMissingNaNCountIsConservative() {
    org.apache.parquet.column.statistics.Statistics<?> statsWithUnknownNaNs =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(
                Types.required(PrimitiveTypeName.DOUBLE).named("test_double"))
            .withMin(BytesUtils.longToBytes(Double.doubleToLongBits(10.0)))
            .withMax(BytesUtils.longToBytes(Double.doubleToLongBits(100.0)))
            .withNumNulls(0)
            .build();

    List<ColumnChunkMetaData> metas =
        List.of(getIntColumnMeta(intStats, 177L), getDoubleColumnMeta(statsWithUnknownNaNs, 177L));

    assertThat(canDrop(eq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(notEq(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, 200.0), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, 200.0), metas)).isFalse();
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

    assertThat(canDrop(eq(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(notEq(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, 50.0), metas)).isFalse();
    assertThat(canDrop(in(doubleColumn, new HashSet<>(List.of(50.0))), metas))
        .isFalse();

    assertThat(canDrop(eq(doubleColumn, Double.NaN), metas)).isTrue();
    assertThat(canDrop(notEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(in(doubleColumn, new HashSet<>(List.of(Double.NaN))), metas))
        .isFalse();
  }

  @Test
  public void testNaNDoubleIeee754TotalOrder() {
    // All-NaN with IEEE_754_TOTAL_ORDER, stats built via builder (no min/max)
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

    assertThat(canDrop(eq(doubleColumn, 5.0), metas)).isTrue();
    assertThat(canDrop(notEq(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, 5.0), metas)).isFalse();
    assertThat(canDrop(in(doubleColumn, new HashSet<>(List.of(5.0))), metas))
        .isTrue();

    assertThat(canDrop(eq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(notEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(lt(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(ltEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(gt(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(gtEq(doubleColumn, Double.NaN), metas)).isFalse();
    assertThat(canDrop(in(doubleColumn, new HashSet<>(List.of(Double.NaN))), metas))
        .isFalse();
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

    assertThat(canDrop(eq(floatCol, 5.0f), metas)).isTrue();
    assertThat(canDrop(notEq(floatCol, 5.0f), metas)).isFalse();
    assertThat(canDrop(lt(floatCol, 5.0f), metas)).isFalse();
    assertThat(canDrop(ltEq(floatCol, 5.0f), metas)).isFalse();
    assertThat(canDrop(gt(floatCol, 5.0f), metas)).isFalse();
    assertThat(canDrop(gtEq(floatCol, 5.0f), metas)).isFalse();
    assertThat(canDrop(in(floatCol, new HashSet<>(List.of(5.0f))), metas)).isTrue();

    assertThat(canDrop(eq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(notEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(lt(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(ltEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(gt(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(gtEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(in(floatCol, new HashSet<>(List.of(Float.NaN))), metas))
        .isFalse();
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

    assertThat(canDrop(eq(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(notEq(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(lt(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(ltEq(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(gt(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(gtEq(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(in(floatCol, new HashSet<>(List.of(50.0f))), metas)).isFalse();
    assertThat(canDrop(lt(floatCol, 0.0f), metas)).isFalse();
    assertThat(canDrop(gt(floatCol, 200.0f), metas)).isFalse();

    FloatStatistics mixedEqualStats = new FloatStatistics();
    mixedEqualStats.setMinMax(5.0f, 5.0f);
    mixedEqualStats.setNumNulls(0);
    mixedEqualStats.incrementNanCount(1);
    List<ColumnChunkMetaData> mixedEqualMetas =
        List.of(getIntColumnMeta(intStats, 177L), getFloatColumnMeta(mixedEqualStats, 177L));
    assertThat(canDrop(notEq(floatCol, 5.0f), mixedEqualMetas)).isFalse();

    assertThat(canDrop(eq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(notEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(lt(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(ltEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(gt(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(gtEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(in(floatCol, new HashSet<>(List.of(Float.NaN))), metas))
        .isFalse();
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

    assertThat(canDrop(eq(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(notEq(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(lt(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(ltEq(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(gt(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(gtEq(floatCol, 50.0f), metas)).isFalse();
    assertThat(canDrop(in(floatCol, new HashSet<>(List.of(50.0f))), metas)).isFalse();

    assertThat(canDrop(eq(floatCol, Float.NaN), metas)).isTrue();
    assertThat(canDrop(notEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(lt(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(ltEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(gt(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(gtEq(floatCol, Float.NaN), metas)).isFalse();
    assertThat(canDrop(in(floatCol, new HashSet<>(List.of(Float.NaN))), metas))
        .isFalse();
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

    assertThat(canDrop(eq(float16Column, FLOAT16_ONE), metas)).isTrue();
    assertThat(canDrop(notEq(float16Column, FLOAT16_ONE), metas)).isFalse();
    assertThat(canDrop(lt(float16Column, FLOAT16_ONE), metas)).isFalse();
    assertThat(canDrop(ltEq(float16Column, FLOAT16_ONE), metas)).isFalse();
    assertThat(canDrop(gt(float16Column, FLOAT16_ONE), metas)).isFalse();
    assertThat(canDrop(gtEq(float16Column, FLOAT16_ONE), metas)).isFalse();
    assertThat(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_ONE))), metas))
        .isTrue();

    assertThat(canDrop(eq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(notEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(lt(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(ltEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(gt(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(gtEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_NAN))), metas))
        .isFalse();
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

    assertThat(canDrop(eq(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(notEq(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(lt(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(ltEq(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(gt(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(gtEq(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_FIFTY))), metas))
        .isFalse();

    assertThat(canDrop(eq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(notEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(lt(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(ltEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(gt(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(gtEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_NAN))), metas))
        .isFalse();
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

    assertThat(canDrop(eq(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(notEq(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(lt(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(ltEq(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(gt(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(gtEq(float16Column, FLOAT16_FIFTY), metas)).isFalse();
    assertThat(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_FIFTY))), metas))
        .isFalse();

    assertThat(canDrop(eq(float16Column, FLOAT16_NAN), metas)).isTrue();
    assertThat(canDrop(notEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(lt(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(ltEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(gt(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(gtEq(float16Column, FLOAT16_NAN), metas)).isFalse();
    assertThat(canDrop(in(float16Column, new HashSet<>(List.of(FLOAT16_NAN))), metas))
        .isFalse();
  }
}
