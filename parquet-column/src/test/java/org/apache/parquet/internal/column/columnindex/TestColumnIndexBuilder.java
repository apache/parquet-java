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
package org.apache.parquet.internal.column.columnindex;

import static java.util.Arrays.asList;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.contains;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notIn;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.size;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.predicate.LogicalInverter.invert;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.UINT_8;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.ContainsRewriter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.BooleanColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

/**
 * Tests for {@link ColumnIndexBuilder}.
 */
public class TestColumnIndexBuilder {

  public static class BinaryDecimalIsNullOrZeroUdp extends UserDefinedPredicate<Binary> {
    private static final Binary ZERO = decimalBinary("0.0");

    @Override
    public boolean keep(Binary value) {
      return value == null || value.equals(ZERO);
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics) {
      Comparator<Binary> cmp = statistics.getComparator();
      return cmp.compare(statistics.getMin(), ZERO) > 0 || cmp.compare(statistics.getMax(), ZERO) < 0;
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics) {
      Comparator<Binary> cmp = statistics.getComparator();
      return cmp.compare(statistics.getMin(), ZERO) == 0 && cmp.compare(statistics.getMax(), ZERO) == 0;
    }
  }

  public static class BinaryUtf8StartsWithB extends UserDefinedPredicate<Binary> {
    private static final Binary B = stringBinary("B");
    private static final Binary C = stringBinary("C");

    @Override
    public boolean keep(Binary value) {
      return value != null && value.length() > 0 && value.getBytesUnsafe()[0] == 'B';
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics) {
      Comparator<Binary> cmp = statistics.getComparator();
      return cmp.compare(statistics.getMin(), C) >= 0 || cmp.compare(statistics.getMax(), B) < 0;
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics) {
      Comparator<Binary> cmp = statistics.getComparator();
      return cmp.compare(statistics.getMin(), B) >= 0 && cmp.compare(statistics.getMax(), C) < 0;
    }
  }

  public static class BooleanIsTrueOrNull extends UserDefinedPredicate<Boolean> {
    @Override
    public boolean keep(Boolean value) {
      return value == null || value;
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Boolean> statistics) {
      return statistics.getComparator().compare(statistics.getMax(), true) != 0;
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Boolean> statistics) {
      return statistics.getComparator().compare(statistics.getMin(), true) == 0;
    }
  }

  public static class DoubleIsInteger extends UserDefinedPredicate<Double> {
    @Override
    public boolean keep(Double value) {
      return value != null && Math.floor(value) == value;
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Double> statistics) {
      double min = statistics.getMin();
      double max = statistics.getMax();
      Comparator<Double> cmp = statistics.getComparator();
      return cmp.compare(Math.floor(min), Math.floor(max)) == 0
          && cmp.compare(Math.floor(min), min) != 0
          && cmp.compare(Math.floor(max), max) != 0;
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Double> statistics) {
      double min = statistics.getMin();
      double max = statistics.getMax();
      Comparator<Double> cmp = statistics.getComparator();
      return cmp.compare(min, max) == 0 && cmp.compare(Math.floor(min), min) == 0;
    }
  }

  public static class FloatIsInteger extends UserDefinedPredicate<Float> {
    private static float floor(float value) {
      return (float) Math.floor(value);
    }

    @Override
    public boolean keep(Float value) {
      return value != null && Math.floor(value) == value;
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Float> statistics) {
      float min = statistics.getMin();
      float max = statistics.getMax();
      Comparator<Float> cmp = statistics.getComparator();
      return cmp.compare(floor(min), floor(max)) == 0
          && cmp.compare(floor(min), min) != 0
          && cmp.compare(floor(max), max) != 0;
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Float> statistics) {
      float min = statistics.getMin();
      float max = statistics.getMax();
      Comparator<Float> cmp = statistics.getComparator();
      return cmp.compare(min, max) == 0 && cmp.compare(floor(min), min) == 0;
    }
  }

  public static class IntegerIsDivisableWith3 extends UserDefinedPredicate<Integer> {
    @Override
    public boolean keep(Integer value) {
      return value != null && value % 3 == 0;
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics) {
      int min = statistics.getMin();
      int max = statistics.getMax();
      return min % 3 != 0 && max % 3 != 0 && max - min < 3;
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics) {
      int min = statistics.getMin();
      int max = statistics.getMax();
      return min == max && min % 3 == 0;
    }
  }

  public static class LongIsDivisableWith3 extends UserDefinedPredicate<Long> {
    @Override
    public boolean keep(Long value) {
      return value != null && value % 3 == 0;
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Long> statistics) {
      long min = statistics.getMin();
      long max = statistics.getMax();
      return min % 3 != 0 && max % 3 != 0 && max - min < 3;
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Long> statistics) {
      long min = statistics.getMin();
      long max = statistics.getMax();
      return min == max && min % 3 == 0;
    }
  }

  @Test
  public void testArrayContainsDouble() {
    PrimitiveType type = Types.required(DOUBLE).named("test_double");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(DoubleColumnIndexBuilder.class));
    assertNull(builder.build());
    DoubleColumn col = doubleColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4.2, -4.1));
    builder.add(sb.stats(type, -11.7, 7.0, null));
    builder.add(sb.stats(type, 2.2, 2.2, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1.9, 2.32));
    builder.add(sb.stats(type, -21.0, 8.1));
    builder.add(sb.stats(type, 10.0, 25.0));
    assertEquals(7, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), -4.1, 7.0, 2.2, null, 2.32, 8.1, 25.0);
    assertCorrectValues(columnIndex.getMinValues(), -4.2, -11.7, 2.2, null, 1.9, -21.0, 10.0);

    // Validate that contains(eq()) matches eq() when not combined using or() and and()
    assertCorrectFiltering(columnIndex, eq(col, 0.0), 1, 5);
    assertCorrectFiltering(columnIndex, contains(eq(col, 0.0)), 1, 5);

    assertCorrectFiltering(columnIndex, eq(col, 2.2), 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, contains(eq(col, 2.2)), 1, 2, 4, 5);

    assertCorrectFiltering(columnIndex, eq(col, 25.0), 6);
    assertCorrectFiltering(columnIndex, contains(eq(col, 25.0)), 6);

    // Should equal intersection of [1, 5] and [1, 2, 4, 5] --> [1, 5]
    assertCorrectFiltering(
        columnIndex, ContainsRewriter.rewrite(and(contains(eq(col, 0.0)), contains(eq(col, 2.2)))), 1, 5);

    // Should equal intersection of [6] and [1, 5] --> []
    assertCorrectFiltering(
        columnIndex, ContainsRewriter.rewrite(and(contains(eq(col, 25.0)), contains(eq(col, 0.0)))));

    // Should equal union of [6] and [1, 5] --> [1, 5, 6]
    assertCorrectFiltering(
        columnIndex, ContainsRewriter.rewrite(or(contains(eq(col, 25.0)), contains(eq(col, 0.0)))), 1, 5, 6);

    // Should equal de-duplicated union of [1, 5] and [1, 2, 4, 5] --> [1, 2, 4, 5]
    assertCorrectFiltering(
        columnIndex, ContainsRewriter.rewrite(or(contains(eq(col, 0.0)), contains(eq(col, 2.2)))), 1, 2, 4, 5);
  }

  @Test
  public void testBuildBinaryDecimal() {
    PrimitiveType type =
        Types.required(BINARY).as(DECIMAL).precision(12).scale(2).named("test_binary_decimal");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(BinaryColumnIndexBuilder.class));
    assertNull(builder.build());
    BinaryColumn col = binaryColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, decimalBinary("-0.17"), decimalBinary("1234567890.12")));
    builder.add(sb.stats(type, decimalBinary("-234.23"), null, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, decimalBinary("-9999293.23"), decimalBinary("2348978.45")));
    builder.add(sb.stats(type, null, null, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, decimalBinary("87656273")));
    assertEquals(8, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 0, 3, 3, 0, 4, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, false, true, false, true, true, false);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        null,
        decimalBinary("1234567890.12"),
        decimalBinary("-234.23"),
        null,
        decimalBinary("2348978.45"),
        null,
        null,
        decimalBinary("87656273"));
    assertCorrectValues(
        columnIndex.getMinValues(),
        null,
        decimalBinary("-0.17"),
        decimalBinary("-234.23"),
        null,
        decimalBinary("-9999293.23"),
        null,
        null,
        decimalBinary("87656273"));
    assertCorrectFiltering(columnIndex, eq(col, decimalBinary("0.0")), 1, 4);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 5, 6);
    Set<Binary> set1 = new HashSet<>();
    set1.add(Binary.fromString("0.0"));
    assertCorrectFiltering(columnIndex, in(col, set1), 1, 4);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5, 6, 7);

    set1.add(null);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, decimalBinary("87656273")), 0, 1, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 4, 7);
    assertCorrectFiltering(columnIndex, gt(col, decimalBinary("2348978.45")), 1);
    assertCorrectFiltering(columnIndex, gtEq(col, decimalBinary("2348978.45")), 1, 4);
    assertCorrectFiltering(columnIndex, lt(col, decimalBinary("-234.23")), 4);
    assertCorrectFiltering(columnIndex, ltEq(col, decimalBinary("-234.23")), 2, 4);
    assertCorrectFiltering(columnIndex, userDefined(col, BinaryDecimalIsNullOrZeroUdp.class), 0, 1, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryDecimalIsNullOrZeroUdp.class)), 1, 2, 4, 7);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null, null, null));
    builder.add(sb.stats(type, decimalBinary("-9999293.23"), decimalBinary("-234.23")));
    builder.add(sb.stats(type, decimalBinary("-0.17"), decimalBinary("87656273")));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, decimalBinary("87656273")));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, decimalBinary("1234567890.12"), null, null, null));
    builder.add(sb.stats(type, null, null, null));
    assertEquals(8, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 4, 0, 0, 2, 0, 2, 3, 3);
    assertCorrectNullPages(columnIndex, true, false, false, true, false, true, false, true);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        null,
        decimalBinary("-234.23"),
        decimalBinary("87656273"),
        null,
        decimalBinary("87656273"),
        null,
        decimalBinary("1234567890.12"),
        null);
    assertCorrectValues(
        columnIndex.getMinValues(),
        null,
        decimalBinary("-9999293.23"),
        decimalBinary("-0.17"),
        null,
        decimalBinary("87656273"),
        null,
        decimalBinary("1234567890.12"),
        null);
    assertCorrectFiltering(columnIndex, eq(col, decimalBinary("87656273")), 2, 4);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 3, 5, 6, 7);
    Set<Binary> set2 = new HashSet<>();
    set2.add(decimalBinary("87656273"));
    assertCorrectFiltering(columnIndex, in(col, set2), 2, 4);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7);
    set2.add(null);
    assertCorrectFiltering(columnIndex, in(col, set2), 0, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, decimalBinary("87656273")), 0, 1, 2, 3, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 4, 6);
    assertCorrectFiltering(columnIndex, gt(col, decimalBinary("87656273")), 6);
    assertCorrectFiltering(columnIndex, gtEq(col, decimalBinary("87656273")), 2, 4, 6);
    assertCorrectFiltering(columnIndex, lt(col, decimalBinary("-0.17")), 1);
    assertCorrectFiltering(columnIndex, ltEq(col, decimalBinary("-0.17")), 1, 2);
    assertCorrectFiltering(columnIndex, userDefined(col, BinaryDecimalIsNullOrZeroUdp.class), 0, 2, 3, 5, 6, 7);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryDecimalIsNullOrZeroUdp.class)), 1, 2, 4, 6);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, decimalBinary("1234567890.12"), null, null, null));
    builder.add(sb.stats(type, null, null, null, null));
    builder.add(sb.stats(type, decimalBinary("1234567890.12"), decimalBinary("87656273")));
    builder.add(sb.stats(type, decimalBinary("987656273"), decimalBinary("-0.17")));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, decimalBinary("-234.23"), decimalBinary("-9999293.23")));
    assertEquals(8, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 3, 2, 3, 4, 0, 0, 2, 0);
    assertCorrectNullPages(columnIndex, true, true, false, true, false, false, true, false);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        null,
        null,
        decimalBinary("1234567890.12"),
        null,
        decimalBinary("1234567890.12"),
        decimalBinary("987656273"),
        null,
        decimalBinary("-234.23"));
    assertCorrectValues(
        columnIndex.getMinValues(),
        null,
        null,
        decimalBinary("1234567890.12"),
        null,
        decimalBinary("87656273"),
        decimalBinary("-0.17"),
        null,
        decimalBinary("-9999293.23"));
    assertCorrectFiltering(columnIndex, eq(col, decimalBinary("1234567890.12")), 2, 4);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 6);
    Set<Binary> set3 = new HashSet<>();
    set3.add(decimalBinary("1234567890.12"));
    assertCorrectFiltering(columnIndex, in(col, set3), 2, 4);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7);
    set3.add(null);
    assertCorrectFiltering(columnIndex, in(col, set3), 0, 1, 2, 3, 4, 6);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, decimalBinary("0.0")), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, null), 2, 4, 5, 7);
    assertCorrectFiltering(columnIndex, gt(col, decimalBinary("1234567890.12")));
    assertCorrectFiltering(columnIndex, gtEq(col, decimalBinary("1234567890.12")), 2, 4);
    assertCorrectFiltering(columnIndex, lt(col, decimalBinary("-0.17")), 7);
    assertCorrectFiltering(columnIndex, ltEq(col, decimalBinary("-0.17")), 5, 7);
    assertCorrectFiltering(columnIndex, userDefined(col, BinaryDecimalIsNullOrZeroUdp.class), 0, 1, 2, 3, 5, 6);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryDecimalIsNullOrZeroUdp.class)), 2, 4, 5, 7);
  }

  @Test
  public void testBuildBinaryUtf8() {
    PrimitiveType type = Types.required(BINARY).as(UTF8).named("test_binary_utf8");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(BinaryColumnIndexBuilder.class));
    assertNull(builder.build());
    BinaryColumn col = binaryColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Jeltz"), stringBinary("Slartibartfast"), null, null));
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Beeblebrox"), stringBinary("Prefect")));
    builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Trilian"), null));
    builder.add(sb.stats(type, stringBinary("Beeblebrox")));
    builder.add(sb.stats(type, null, null));
    assertEquals(8, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 5, 2, 0, 1, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, true, true, false, false, false, true);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        null,
        stringBinary("Slartibartfast"),
        null,
        null,
        stringBinary("Prefect"),
        stringBinary("Trilian"),
        stringBinary("Beeblebrox"),
        null);
    assertCorrectValues(
        columnIndex.getMinValues(),
        null,
        stringBinary("Jeltz"),
        null,
        null,
        stringBinary("Beeblebrox"),
        stringBinary("Dent"),
        stringBinary("Beeblebrox"),
        null);
    assertCorrectFiltering(columnIndex, eq(col, stringBinary("Marvin")), 1, 4, 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 5, 7);
    Set<Binary> set1 = new HashSet<>();
    set1.add(stringBinary("Marvin"));
    assertCorrectFiltering(columnIndex, in(col, set1), 1, 4, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5, 6, 7);
    set1.add(null);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 2, 3, 4, 5, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, stringBinary("Beeblebrox")), 0, 1, 2, 3, 4, 5, 7);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 4, 5, 6);
    assertCorrectFiltering(columnIndex, gt(col, stringBinary("Prefect")), 1, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, stringBinary("Prefect")), 1, 4, 5);
    assertCorrectFiltering(columnIndex, lt(col, stringBinary("Dent")), 4, 6);
    assertCorrectFiltering(columnIndex, ltEq(col, stringBinary("Dent")), 4, 5, 6);
    assertCorrectFiltering(columnIndex, userDefined(col, BinaryUtf8StartsWithB.class), 4, 6);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryUtf8StartsWithB.class)), 0, 1, 2, 3, 4, 5, 7);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, stringBinary("Beeblebrox"), stringBinary("Dent"), null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Jeltz")));
    builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Prefect"), null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Slartibartfast")));
    builder.add(sb.stats(type, null, null));
    assertEquals(8, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 5, 0, 1, 2, 0, 2);
    assertCorrectNullPages(columnIndex, false, true, true, false, false, true, false, true);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        stringBinary("Dent"),
        null,
        null,
        stringBinary("Jeltz"),
        stringBinary("Prefect"),
        null,
        stringBinary("Slartibartfast"),
        null);
    assertCorrectValues(
        columnIndex.getMinValues(),
        stringBinary("Beeblebrox"),
        null,
        null,
        stringBinary("Dent"),
        stringBinary("Dent"),
        null,
        stringBinary("Slartibartfast"),
        null);
    assertCorrectFiltering(columnIndex, eq(col, stringBinary("Jeltz")), 3, 4);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 4, 5, 7);
    Set<Binary> set2 = new HashSet<>();
    set2.add(stringBinary("Jeltz"));
    assertCorrectFiltering(columnIndex, in(col, set2), 3, 4);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7);
    set2.add(null);
    assertCorrectFiltering(columnIndex, in(col, set2), 0, 1, 2, 3, 4, 5, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, stringBinary("Slartibartfast")), 0, 1, 2, 3, 4, 5, 7);
    assertCorrectFiltering(columnIndex, notEq(col, null), 0, 3, 4, 6);
    assertCorrectFiltering(columnIndex, gt(col, stringBinary("Marvin")), 4, 6);
    assertCorrectFiltering(columnIndex, gtEq(col, stringBinary("Marvin")), 4, 6);
    assertCorrectFiltering(columnIndex, lt(col, stringBinary("Dent")), 0);
    assertCorrectFiltering(columnIndex, ltEq(col, stringBinary("Dent")), 0, 3, 4);
    assertCorrectFiltering(columnIndex, userDefined(col, BinaryUtf8StartsWithB.class), 0);
    assertCorrectFiltering(
        columnIndex, invert(userDefined(col, BinaryUtf8StartsWithB.class)), 0, 1, 2, 3, 4, 5, 6, 7);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Slartibartfast")));
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, stringBinary("Prefect"), stringBinary("Jeltz"), null));
    builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Dent")));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Beeblebrox"), null, null));
    assertEquals(8, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 0, 5, 1, 0, 2, 2, 2);
    assertCorrectNullPages(columnIndex, true, false, true, false, false, true, true, false);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        null,
        stringBinary("Slartibartfast"),
        null,
        stringBinary("Prefect"),
        stringBinary("Dent"),
        null,
        null,
        stringBinary("Dent"));
    assertCorrectValues(
        columnIndex.getMinValues(),
        null,
        stringBinary("Slartibartfast"),
        null,
        stringBinary("Jeltz"),
        stringBinary("Dent"),
        null,
        null,
        stringBinary("Beeblebrox"));
    assertCorrectFiltering(columnIndex, eq(col, stringBinary("Marvin")), 3);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 5, 6, 7);
    Set<Binary> set3 = new HashSet<>();
    set3.add(stringBinary("Marvin"));
    assertCorrectFiltering(columnIndex, in(col, set3), 3);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7);
    set3.add(null);
    assertCorrectFiltering(columnIndex, in(col, set3), 0, 2, 3, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, stringBinary("Dent")), 0, 1, 2, 3, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 4, 7);
    assertCorrectFiltering(columnIndex, gt(col, stringBinary("Prefect")), 1);
    assertCorrectFiltering(columnIndex, gtEq(col, stringBinary("Prefect")), 1, 3);
    assertCorrectFiltering(columnIndex, lt(col, stringBinary("Marvin")), 3, 4, 7);
    assertCorrectFiltering(columnIndex, ltEq(col, stringBinary("Marvin")), 3, 4, 7);
    assertCorrectFiltering(columnIndex, userDefined(col, BinaryUtf8StartsWithB.class), 7);
    assertCorrectFiltering(
        columnIndex, invert(userDefined(col, BinaryUtf8StartsWithB.class)), 0, 1, 2, 3, 4, 5, 6, 7);
  }

  @Test
  public void testBinaryWithTruncate() {
    PrimitiveType type = Types.required(BINARY).as(UTF8).named("test_binary_utf8");
    int truncateLen = 5;
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, truncateLen);
    assertThat(builder, instanceOf(BinaryColumnIndexBuilder.class));
    assertNull(builder.build());

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Jeltz"), stringBinary("Slartibartfast"), null, null));
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Beeblebrox"), stringBinary("Prefect")));
    builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Trilian"), null));
    builder.add(sb.stats(type, stringBinary("Beeblebrox")));
    builder.add(sb.stats(type, null, null));
    assertEquals(8, builder.getPageCount());
    assertEquals(39, builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 5, 2, 0, 1, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, true, true, false, false, false, true);

    BinaryTruncator truncator = BinaryTruncator.getTruncator(type);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        null,
        truncator.truncateMax(stringBinary("Slartibartfast"), truncateLen),
        null,
        null,
        truncator.truncateMax(stringBinary("Prefect"), truncateLen),
        truncator.truncateMax(stringBinary("Trilian"), truncateLen),
        truncator.truncateMax(stringBinary("Beeblebrox"), truncateLen),
        null);
    assertCorrectValues(
        columnIndex.getMinValues(),
        null,
        truncator.truncateMin(stringBinary("Jeltz"), truncateLen),
        null,
        null,
        truncator.truncateMin(stringBinary("Beeblebrox"), truncateLen),
        truncator.truncateMin(stringBinary("Dent"), truncateLen),
        truncator.truncateMin(stringBinary("Beeblebrox"), truncateLen),
        null);
  }

  @Test
  public void testStaticBuildBinary() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(BINARY).as(UTF8).named("test_binary_utf8"),
        BoundaryOrder.ASCENDING,
        asList(true, true, false, false, true, false, true, false),
        asList(1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l),
        toBBList(
            null,
            null,
            stringBinary("Beeblebrox"),
            stringBinary("Dent"),
            null,
            stringBinary("Jeltz"),
            null,
            stringBinary("Slartibartfast")),
        toBBList(
            null,
            null,
            stringBinary("Dent"),
            stringBinary("Dent"),
            null,
            stringBinary("Prefect"),
            null,
            stringBinary("Slartibartfast")));
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectNullPages(columnIndex, true, true, false, false, true, false, true, false);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        null,
        null,
        stringBinary("Dent"),
        stringBinary("Dent"),
        null,
        stringBinary("Prefect"),
        null,
        stringBinary("Slartibartfast"));
    assertCorrectValues(
        columnIndex.getMinValues(),
        null,
        null,
        stringBinary("Beeblebrox"),
        stringBinary("Dent"),
        null,
        stringBinary("Jeltz"),
        null,
        stringBinary("Slartibartfast"));
  }

  @Test
  public void testFilterWithoutNullCounts() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(BINARY).as(UTF8).named("test_binary_utf8"),
        BoundaryOrder.ASCENDING,
        asList(true, true, false, false, true, false, true, false),
        null,
        toBBList(
            null,
            null,
            stringBinary("Beeblebrox"),
            stringBinary("Dent"),
            null,
            stringBinary("Jeltz"),
            null,
            stringBinary("Slartibartfast")),
        toBBList(
            null,
            null,
            stringBinary("Dent"),
            stringBinary("Dent"),
            null,
            stringBinary("Prefect"),
            null,
            stringBinary("Slartibartfast")));
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertNull(columnIndex.getNullCounts());
    assertCorrectNullPages(columnIndex, true, true, false, false, true, false, true, false);
    assertCorrectValues(
        columnIndex.getMaxValues(),
        null,
        null,
        stringBinary("Dent"),
        stringBinary("Dent"),
        null,
        stringBinary("Prefect"),
        null,
        stringBinary("Slartibartfast"));
    assertCorrectValues(
        columnIndex.getMinValues(),
        null,
        null,
        stringBinary("Beeblebrox"),
        stringBinary("Dent"),
        null,
        stringBinary("Jeltz"),
        null,
        stringBinary("Slartibartfast"));

    BinaryColumn col = binaryColumn("test_col");
    assertCorrectFiltering(columnIndex, eq(col, stringBinary("Dent")), 2, 3);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 5, 6, 7);
    Set<Binary> set = new HashSet<>();
    set.add(stringBinary("Dent"));
    assertCorrectFiltering(columnIndex, in(col, set), 2, 3);
    assertCorrectFiltering(columnIndex, notIn(col, set), 0, 1, 2, 3, 4, 5, 6, 7);
    set.add(null);
    assertCorrectFiltering(columnIndex, in(col, set), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, stringBinary("Dent")), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notEq(col, null), 2, 3, 5, 7);
    assertCorrectFiltering(
        columnIndex, userDefined(col, BinaryDecimalIsNullOrZeroUdp.class), 0, 1, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryDecimalIsNullOrZeroUdp.class)), 2, 3, 5, 7);
  }

  @Test
  public void testBuildBoolean() {
    PrimitiveType type = Types.required(BOOLEAN).named("test_boolean");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(BooleanColumnIndexBuilder.class));
    assertNull(builder.build());
    BooleanColumn col = booleanColumn("test_col");

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, false, true));
    builder.add(sb.stats(type, true, false, null));
    builder.add(sb.stats(type, true, true, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, false, false));
    assertEquals(5, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), true, true, true, null, false);
    assertCorrectValues(columnIndex.getMinValues(), false, false, true, null, false);
    assertCorrectFiltering(columnIndex, eq(col, true), 0, 1, 2);
    assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
    Set<Boolean> set1 = new HashSet<>();
    set1.add(true);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 2);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4);
    set1.add(null);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 2, 3);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4);
    assertCorrectFiltering(columnIndex, notEq(col, true), 0, 1, 2, 3, 4);
    assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4);
    assertCorrectFiltering(columnIndex, userDefined(col, BooleanIsTrueOrNull.class), 0, 1, 2, 3);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, BooleanIsTrueOrNull.class)), 0, 1, 4);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, false, false));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, null, null, null, null));
    builder.add(sb.stats(type, false, true, null));
    builder.add(sb.stats(type, false, true, null, null));
    builder.add(sb.stats(type, null, null, null));
    assertEquals(7, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 0, 3, 4, 1, 2, 3);
    assertCorrectNullPages(columnIndex, true, false, true, true, false, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, false, null, null, true, true, null);
    assertCorrectValues(columnIndex.getMinValues(), null, false, null, null, false, false, null);
    assertCorrectFiltering(columnIndex, eq(col, true), 4, 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 5, 6);
    Set<Boolean> set2 = new HashSet<>();
    set2.add(true);
    assertCorrectFiltering(columnIndex, in(col, set2), 4, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6);
    set2.add(null);
    assertCorrectFiltering(columnIndex, in(col, set2), 0, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, notEq(col, true), 0, 1, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 4, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, BooleanIsTrueOrNull.class), 0, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, BooleanIsTrueOrNull.class)), 1, 4, 5);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, true, true));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, null, null, null, null));
    builder.add(sb.stats(type, true, false, null));
    builder.add(sb.stats(type, false, false, null, null));
    builder.add(sb.stats(type, null, null, null));
    assertEquals(7, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 0, 3, 4, 1, 2, 3);
    assertCorrectNullPages(columnIndex, true, false, true, true, false, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, true, null, null, true, false, null);
    assertCorrectValues(columnIndex.getMinValues(), null, true, null, null, false, false, null);
    assertCorrectFiltering(columnIndex, eq(col, true), 1, 4);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 5, 6);
    Set<Boolean> set3 = new HashSet<>();
    set3.add(true);
    assertCorrectFiltering(columnIndex, in(col, set3), 1, 4);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6);
    set3.add(null);
    assertCorrectFiltering(columnIndex, in(col, set3), 0, 1, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, notEq(col, true), 0, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 4, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, BooleanIsTrueOrNull.class), 0, 1, 2, 3, 4, 5, 6);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, BooleanIsTrueOrNull.class)), 4, 5);
  }

  @Test
  public void testStaticBuildBoolean() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(BOOLEAN).named("test_boolean"),
        BoundaryOrder.DESCENDING,
        asList(false, true, false, true, false, true),
        asList(9l, 8l, 7l, 6l, 5l, 0l),
        toBBList(false, null, false, null, true, null),
        toBBList(true, null, false, null, true, null));
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 9, 8, 7, 6, 5, 0);
    assertCorrectNullPages(columnIndex, false, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), true, null, false, null, true, null);
    assertCorrectValues(columnIndex.getMinValues(), false, null, false, null, true, null);
  }

  @Test
  public void testBuildDouble() {
    PrimitiveType type = Types.required(DOUBLE).named("test_double");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(DoubleColumnIndexBuilder.class));
    assertNull(builder.build());
    DoubleColumn col = doubleColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4.2, -4.1));
    builder.add(sb.stats(type, -11.7, 7.0, null));
    builder.add(sb.stats(type, 2.2, 2.2, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1.9, 2.32));
    builder.add(sb.stats(type, -21.0, 8.1));
    assertEquals(6, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), -4.1, 7.0, 2.2, null, 2.32, 8.1);
    assertCorrectValues(columnIndex.getMinValues(), -4.2, -11.7, 2.2, null, 1.9, -21.0);
    assertCorrectFiltering(columnIndex, eq(col, 0.0), 1, 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
    Set<Double> set1 = new HashSet<>();
    set1.add(0.0);
    set1.add(-4.2);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    set1.add(null);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 2, 3, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, 2.2), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, gt(col, 2.2), 1, 4, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 2.2), 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, lt(col, -4.2), 1, 5);
    assertCorrectFiltering(columnIndex, ltEq(col, -4.2), 0, 1, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, DoubleIsInteger.class), 1, 4, 5);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, DoubleIsInteger.class)), 0, 1, 2, 3, 4, 5);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -532.3, -345.2, null, null));
    builder.add(sb.stats(type, -234.7, -234.6, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, -234.6, 2.99999));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 3.0, 42.83));
    builder.add(sb.stats(type, null, null));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, -345.2, -234.6, null, null, 2.99999, null, 42.83, null);
    assertCorrectValues(columnIndex.getMinValues(), null, -532.3, -234.7, null, null, -234.6, null, 3.0, null);
    assertCorrectFiltering(columnIndex, eq(col, 0.0), 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
    Set<Double> set2 = new HashSet<>();
    set2.add(0.0);
    set2.add(3.5);
    set2.add(-346.0);
    assertCorrectFiltering(columnIndex, in(col, set2), 1, 2, 5, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set2.add(null);
    assertCorrectFiltering(columnIndex, in(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 0.0), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
    assertCorrectFiltering(columnIndex, gt(col, 2.99999), 7);
    assertCorrectFiltering(columnIndex, gtEq(col, 2.99999), 5, 7);
    assertCorrectFiltering(columnIndex, lt(col, -234.6), 1, 2);
    assertCorrectFiltering(columnIndex, ltEq(col, -234.6), 1, 2, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, DoubleIsInteger.class), 1, 5, 7);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, DoubleIsInteger.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, 532.3, 345.2));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 234.7, 234.6, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 234.69, -2.99999));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -3.0, -42.83));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 532.3, null, 234.7, null, 234.69, null, null, -3.0);
    assertCorrectValues(columnIndex.getMinValues(), null, 345.2, null, 234.6, null, -2.99999, null, null, -42.83);
    assertCorrectFiltering(columnIndex, eq(col, 234.6), 3, 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
    Set<Double> set3 = new HashSet<>();
    set3.add(234.6);
    assertCorrectFiltering(columnIndex, in(col, set3), 3, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set3.add(null);
    assertCorrectFiltering(columnIndex, in(col, set3), 0, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 2.2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
    assertCorrectFiltering(columnIndex, gt(col, 2.2), 1, 3, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 234.69), 1, 3, 5);
    assertCorrectFiltering(columnIndex, lt(col, -2.99999), 8);
    assertCorrectFiltering(columnIndex, ltEq(col, -2.99999), 5, 8);
    assertCorrectFiltering(columnIndex, userDefined(col, DoubleIsInteger.class), 1, 5, 8);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, DoubleIsInteger.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testBuildDoubleZeroNaN() {
    PrimitiveType type = Types.required(DOUBLE).named("test_double");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -1.0, -0.0));
    builder.add(sb.stats(type, 0.0, 1.0));
    builder.add(sb.stats(type, 1.0, 100.0));
    ColumnIndex columnIndex = builder.build();
    assertCorrectValues(columnIndex.getMinValues(), -1.0, -0.0, 1.0);
    assertCorrectValues(columnIndex.getMaxValues(), 0.0, 1.0, 100.0);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    builder.add(sb.stats(type, -1.0, -0.0));
    builder.add(sb.stats(type, 0.0, Double.NaN));
    builder.add(sb.stats(type, 1.0, 100.0));
    assertNull(builder.build());
  }

  @Test
  public void testStaticBuildDouble() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(DOUBLE).named("test_double"),
        BoundaryOrder.UNORDERED,
        asList(false, false, false, false, false, false),
        asList(0l, 1l, 2l, 3l, 4l, 5l),
        toBBList(-1.0, -2.0, -3.0, -4.0, -5.0, -6.0),
        toBBList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0));
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 4, 5);
    assertCorrectNullPages(columnIndex, false, false, false, false, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), 1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
    assertCorrectValues(columnIndex.getMinValues(), -1.0, -2.0, -3.0, -4.0, -5.0, -6.0);
  }

  @Test
  public void testBuildFloat() {
    PrimitiveType type = Types.required(FLOAT).named("test_float");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(FloatColumnIndexBuilder.class));
    assertNull(builder.build());
    FloatColumn col = floatColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4.2f, -4.1f));
    builder.add(sb.stats(type, -11.7f, 7.0f, null));
    builder.add(sb.stats(type, 2.2f, 2.2f, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1.9f, 2.32f));
    builder.add(sb.stats(type, -21.0f, 8.1f));
    assertEquals(6, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), -4.1f, 7.0f, 2.2f, null, 2.32f, 8.1f);
    assertCorrectValues(columnIndex.getMinValues(), -4.2f, -11.7f, 2.2f, null, 1.9f, -21.0f);
    assertCorrectFiltering(columnIndex, eq(col, 0.0f), 1, 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
    Set<Float> set1 = new HashSet<>();
    set1.add(0.0f);
    assertCorrectFiltering(columnIndex, in(col, set1), 1, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    set1.add(null);
    assertCorrectFiltering(columnIndex, in(col, set1), 1, 2, 3, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, 2.2f), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, gt(col, 2.2f), 1, 4, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 2.2f), 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, lt(col, 0.0f), 0, 1, 5);
    assertCorrectFiltering(columnIndex, ltEq(col, 1.9f), 0, 1, 4, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, FloatIsInteger.class), 1, 4, 5);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, FloatIsInteger.class)), 0, 1, 2, 3, 4, 5);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -532.3f, -345.2f, null, null));
    builder.add(sb.stats(type, -300.6f, -234.7f, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, -234.6f, 2.99999f));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 3.0f, 42.83f));
    builder.add(sb.stats(type, null, null));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(
        columnIndex.getMaxValues(), null, -345.2f, -234.7f, null, null, 2.99999f, null, 42.83f, null);
    assertCorrectValues(columnIndex.getMinValues(), null, -532.3f, -300.6f, null, null, -234.6f, null, 3.0f, null);
    assertCorrectFiltering(columnIndex, eq(col, 0.0f), 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
    Set<Float> set2 = new HashSet<>();
    set2.add(0.0f);
    assertCorrectFiltering(columnIndex, in(col, set2), 5);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set2.add(null);
    assertCorrectFiltering(columnIndex, in(col, set2), 0, 1, 2, 3, 4, 5, 6, 8);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 2.2f), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
    assertCorrectFiltering(columnIndex, gt(col, 2.2f), 5, 7);
    assertCorrectFiltering(columnIndex, gtEq(col, -234.7f), 2, 5, 7);
    assertCorrectFiltering(columnIndex, lt(col, -234.6f), 1, 2);
    assertCorrectFiltering(columnIndex, ltEq(col, -234.6f), 1, 2, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, FloatIsInteger.class), 1, 2, 5, 7);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, FloatIsInteger.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, 532.3f, 345.2f));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 234.7f, 234.6f, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 234.6f, -2.99999f));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -3.0f, -42.83f));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 532.3f, null, 234.7f, null, 234.6f, null, null, -3.0f);
    assertCorrectValues(
        columnIndex.getMinValues(), null, 345.2f, null, 234.6f, null, -2.99999f, null, null, -42.83f);
    assertCorrectFiltering(columnIndex, eq(col, 234.65f), 3);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
    Set<Float> set3 = new HashSet<>();
    set3.add(234.65f);
    assertCorrectFiltering(columnIndex, in(col, set3), 3);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set3.add(null);
    assertCorrectFiltering(columnIndex, in(col, set3), 0, 2, 3, 4, 6, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 2.2f), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
    assertCorrectFiltering(columnIndex, gt(col, 2.2f), 1, 3, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 2.2f), 1, 3, 5);
    assertCorrectFiltering(columnIndex, lt(col, 0.0f), 5, 8);
    assertCorrectFiltering(columnIndex, ltEq(col, 0.0f), 5, 8);
    assertCorrectFiltering(columnIndex, userDefined(col, FloatIsInteger.class), 1, 5, 8);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, FloatIsInteger.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testBuildFloatZeroNaN() {
    PrimitiveType type = Types.required(FLOAT).named("test_float");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -1.0f, -0.0f));
    builder.add(sb.stats(type, 0.0f, 1.0f));
    builder.add(sb.stats(type, 1.0f, 100.0f));
    ColumnIndex columnIndex = builder.build();
    assertCorrectValues(columnIndex.getMinValues(), -1.0f, -0.0f, 1.0f);
    assertCorrectValues(columnIndex.getMaxValues(), 0.0f, 1.0f, 100.0f);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    builder.add(sb.stats(type, -1.0f, -0.0f));
    builder.add(sb.stats(type, 0.0f, Float.NaN));
    builder.add(sb.stats(type, 1.0f, 100.0f));
    assertNull(builder.build());
  }

  @Test
  public void testStaticBuildFloat() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(FLOAT).named("test_float"),
        BoundaryOrder.ASCENDING,
        asList(true, true, true, false, false, false),
        asList(9l, 8l, 7l, 6l, 0l, 0l),
        toBBList(null, null, null, -3.0f, -2.0f, 0.1f),
        toBBList(null, null, null, -2.0f, 0.0f, 6.0f));
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 9, 8, 7, 6, 0, 0);
    assertCorrectNullPages(columnIndex, true, true, true, false, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, null, null, -2.0f, 0.0f, 6.0f);
    assertCorrectValues(columnIndex.getMinValues(), null, null, null, -3.0f, -2.0f, 0.1f);
  }

  @Test
  public void testBuildInt32() {
    PrimitiveType type = Types.required(INT32).named("test_int32");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(IntColumnIndexBuilder.class));
    assertNull(builder.build());
    IntColumn col = intColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4, 10));
    builder.add(sb.stats(type, -11, 7, null));
    builder.add(sb.stats(type, 2, 2, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1, 2));
    builder.add(sb.stats(type, -21, 8));
    assertEquals(6, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), 10, 7, 2, null, 2, 8);
    assertCorrectValues(columnIndex.getMinValues(), -4, -11, 2, null, 1, -21);
    assertCorrectFiltering(columnIndex, eq(col, 2), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
    Set<Integer> set1 = new HashSet<>();
    set1.add(2);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    set1.add(null);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, gt(col, 2), 0, 1, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 2), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, lt(col, 2), 0, 1, 4, 5);
    assertCorrectFiltering(columnIndex, ltEq(col, 2), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisableWith3.class), 0, 1, 5);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, IntegerIsDivisableWith3.class)), 0, 1, 2, 3, 4, 5);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -532, -345, null, null));
    builder.add(sb.stats(type, -500, -42, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, -42, 2));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 3, 42));
    builder.add(sb.stats(type, null, null));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, -345, -42, null, null, 2, null, 42, null);
    assertCorrectValues(columnIndex.getMinValues(), null, -532, -500, null, null, -42, null, 3, null);
    assertCorrectFiltering(columnIndex, eq(col, 2), 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
    Set<Integer> set2 = new HashSet<>();
    set2.add(2);
    assertCorrectFiltering(columnIndex, in(col, set2), 5);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set2.add(null);
    assertCorrectFiltering(columnIndex, in(col, set2), 0, 1, 2, 3, 4, 5, 6, 8);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
    assertCorrectFiltering(columnIndex, gt(col, 2), 7);
    assertCorrectFiltering(columnIndex, gtEq(col, 2), 5, 7);
    assertCorrectFiltering(columnIndex, lt(col, 2), 1, 2, 5);
    assertCorrectFiltering(columnIndex, ltEq(col, 2), 1, 2, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisableWith3.class), 1, 2, 5, 7);
    assertCorrectFiltering(
        columnIndex, invert(userDefined(col, IntegerIsDivisableWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, 532, 345));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 234, 42, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 42, -2));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -3, -42));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 532, null, 234, null, 42, null, null, -3);
    assertCorrectValues(columnIndex.getMinValues(), null, 345, null, 42, null, -2, null, null, -42);
    assertCorrectFiltering(columnIndex, eq(col, 2), 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
    Set<Integer> set3 = new HashSet<>();
    set3.add(2);
    assertCorrectFiltering(columnIndex, in(col, set3), 5);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set3.add(null);
    assertCorrectFiltering(columnIndex, in(col, set3), 0, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
    assertCorrectFiltering(columnIndex, gt(col, 2), 1, 3, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 2), 1, 3, 5);
    assertCorrectFiltering(columnIndex, lt(col, 2), 5, 8);
    assertCorrectFiltering(columnIndex, ltEq(col, 2), 5, 8);
    assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisableWith3.class), 1, 3, 5, 8);
    assertCorrectFiltering(
        columnIndex, invert(userDefined(col, IntegerIsDivisableWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testStaticBuildInt32() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(INT32).named("test_int32"),
        BoundaryOrder.DESCENDING,
        asList(false, false, false, true, true, true),
        asList(0l, 10l, 0l, 3l, 5l, 7l),
        toBBList(10, 8, 6, null, null, null),
        toBBList(9, 7, 5, null, null, null));
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 10, 0, 3, 5, 7);
    assertCorrectNullPages(columnIndex, false, false, false, true, true, true);
    assertCorrectValues(columnIndex.getMaxValues(), 9, 7, 5, null, null, null);
    assertCorrectValues(columnIndex.getMinValues(), 10, 8, 6, null, null, null);
  }

  @Test
  public void testBuildUInt8() {
    PrimitiveType type = Types.required(INT32).as(UINT_8).named("test_uint8");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(IntColumnIndexBuilder.class));
    assertNull(builder.build());
    IntColumn col = intColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, 4, 10));
    builder.add(sb.stats(type, 11, 17, null));
    builder.add(sb.stats(type, 2, 2, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1, 0xFF));
    builder.add(sb.stats(type, 0xEF, 0xFA));
    assertEquals(6, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), 10, 17, 2, null, 0xFF, 0xFA);
    assertCorrectValues(columnIndex.getMinValues(), 4, 11, 2, null, 1, 0xEF);
    assertCorrectFiltering(columnIndex, eq(col, 2), 2, 4);
    assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
    Set<Integer> set1 = new HashSet<>();
    set1.add(2);
    assertCorrectFiltering(columnIndex, in(col, set1), 2, 4);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    set1.add(null);
    assertCorrectFiltering(columnIndex, in(col, set1), 1, 2, 3, 4);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, gt(col, 2), 0, 1, 4, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 2), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, lt(col, 0xEF), 0, 1, 2, 4);
    assertCorrectFiltering(columnIndex, ltEq(col, 0xEF), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisableWith3.class), 0, 1, 4, 5);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, IntegerIsDivisableWith3.class)), 0, 1, 2, 3, 4, 5);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 0, 0, null, null));
    builder.add(sb.stats(type, 0, 42, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 42, 0xEE));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 0xEF, 0xFF));
    builder.add(sb.stats(type, null, null));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, 0, 42, null, null, 0xEE, null, 0xFF, null);
    assertCorrectValues(columnIndex.getMinValues(), null, 0, 0, null, null, 42, null, 0xEF, null);
    assertCorrectFiltering(columnIndex, eq(col, 2), 2);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
    Set<Integer> set2 = new HashSet<>();
    set2.add(2);
    assertCorrectFiltering(columnIndex, in(col, set2), 2);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set2.add(null);
    assertCorrectFiltering(columnIndex, in(col, set2), 0, 1, 2, 3, 4, 6, 8);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
    assertCorrectFiltering(columnIndex, gt(col, 0xEE), 7);
    assertCorrectFiltering(columnIndex, gtEq(col, 0xEE), 5, 7);
    assertCorrectFiltering(columnIndex, lt(col, 42), 1, 2);
    assertCorrectFiltering(columnIndex, ltEq(col, 42), 1, 2, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisableWith3.class), 1, 2, 5, 7);
    assertCorrectFiltering(
        columnIndex, invert(userDefined(col, IntegerIsDivisableWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, 0xFF, 0xFF));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 0xEF, 0xEA, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 0xEE, 42));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 41, 0));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 0xFF, null, 0xEF, null, 0xEE, null, null, 41);
    assertCorrectValues(columnIndex.getMinValues(), null, 0xFF, null, 0xEA, null, 42, null, null, 0);
    assertCorrectFiltering(columnIndex, eq(col, 0xAB), 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
    Set<Integer> set3 = new HashSet<>();
    set3.add(0xAB);
    assertCorrectFiltering(columnIndex, in(col, set3), 5);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set3.add(null);
    assertCorrectFiltering(columnIndex, in(col, set3), 0, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 0xFF), 0, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
    assertCorrectFiltering(columnIndex, gt(col, 0xFF));
    assertCorrectFiltering(columnIndex, gtEq(col, 0xFF), 1);
    assertCorrectFiltering(columnIndex, lt(col, 42), 8);
    assertCorrectFiltering(columnIndex, ltEq(col, 42), 5, 8);
    assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisableWith3.class), 1, 3, 5, 8);
    assertCorrectFiltering(
        columnIndex, invert(userDefined(col, IntegerIsDivisableWith3.class)), 0, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testBuildInt64() {
    PrimitiveType type = Types.required(INT64).named("test_int64");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder, instanceOf(LongColumnIndexBuilder.class));
    assertNull(builder.build());
    LongColumn col = longColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4l, 10l));
    builder.add(sb.stats(type, -11l, 7l, null));
    builder.add(sb.stats(type, 2l, 2l, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1l, 2l));
    builder.add(sb.stats(type, -21l, 8l));
    assertEquals(6, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0l, 1l, 2l, 3l, 0l, 0l);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), 10l, 7l, 2l, null, 2l, 8l);
    assertCorrectValues(columnIndex.getMinValues(), -4l, -11l, 2l, null, 1l, -21l);
    assertCorrectFiltering(columnIndex, eq(col, 0l), 0, 1, 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
    Set<Long> set1 = new HashSet<>();
    set1.add(0l);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    set1.add(null);
    assertCorrectFiltering(columnIndex, in(col, set1), 0, 1, 2, 3, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set1), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, 0l), 0, 1, 2, 3, 4, 5);
    assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, gt(col, 2l), 0, 1, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 2l), 0, 1, 2, 4, 5);
    assertCorrectFiltering(columnIndex, lt(col, -21l));
    assertCorrectFiltering(columnIndex, ltEq(col, -21l), 5);
    assertCorrectFiltering(columnIndex, userDefined(col, LongIsDivisableWith3.class), 0, 1, 5);
    assertCorrectFiltering(columnIndex, invert(userDefined(col, LongIsDivisableWith3.class)), 0, 1, 2, 3, 4, 5);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -532l, -345l, null, null));
    builder.add(sb.stats(type, -234l, -42l, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, -42l, 2l));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -3l, 42l));
    builder.add(sb.stats(type, null, null));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, -345l, -42l, null, null, 2l, null, 42l, null);
    assertCorrectValues(columnIndex.getMinValues(), null, -532l, -234l, null, null, -42l, null, -3l, null);
    assertCorrectFiltering(columnIndex, eq(col, -42l), 2, 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
    Set<Long> set2 = new HashSet<>();
    set2.add(-42l);
    assertCorrectFiltering(columnIndex, in(col, set2), 2, 5);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set2.add(null);
    assertCorrectFiltering(columnIndex, in(col, set2), 0, 1, 2, 3, 4, 5, 6, 8);
    assertCorrectFiltering(columnIndex, notIn(col, set2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, -42l), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
    assertCorrectFiltering(columnIndex, gt(col, 2l), 7);
    assertCorrectFiltering(columnIndex, gtEq(col, 2l), 5, 7);
    assertCorrectFiltering(columnIndex, lt(col, -42l), 1, 2);
    assertCorrectFiltering(columnIndex, ltEq(col, -42l), 1, 2, 5);
    assertCorrectFiltering(columnIndex, userDefined(col, LongIsDivisableWith3.class), 1, 2, 5, 7);
    assertCorrectFiltering(
        columnIndex, invert(userDefined(col, LongIsDivisableWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, 532l, 345l));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 234l, 42l, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, 42l, -2l));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, -3l, -42l));
    assertEquals(9, builder.getPageCount());
    assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 532l, null, 234l, null, 42l, null, null, -3l);
    assertCorrectValues(columnIndex.getMinValues(), null, 345l, null, 42l, null, -2l, null, null, -42l);
    assertCorrectFiltering(columnIndex, eq(col, 0l), 5);
    assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
    Set<Long> set3 = new HashSet<>();
    set3.add(0l);
    assertCorrectFiltering(columnIndex, in(col, set3), 5);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    set3.add(null);
    assertCorrectFiltering(columnIndex, in(col, set3), 0, 2, 3, 4, 5, 6, 7);
    assertCorrectFiltering(columnIndex, notIn(col, set3), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, 0l), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
    assertCorrectFiltering(columnIndex, gt(col, 2l), 1, 3, 5);
    assertCorrectFiltering(columnIndex, gtEq(col, 2l), 1, 3, 5);
    assertCorrectFiltering(columnIndex, lt(col, -42l));
    assertCorrectFiltering(columnIndex, ltEq(col, -42l), 8);
    assertCorrectFiltering(columnIndex, userDefined(col, LongIsDivisableWith3.class), 1, 3, 5, 8);
    assertCorrectFiltering(
        columnIndex, invert(userDefined(col, LongIsDivisableWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testStaticBuildInt64() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(INT64).named("test_int64"),
        BoundaryOrder.UNORDERED,
        asList(true, false, true, false, true, false),
        asList(1l, 2l, 3l, 4l, 5l, 6l),
        toBBList(null, 2l, null, 4l, null, 9l),
        toBBList(null, 3l, null, 15l, null, 10l));
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 1, 2, 3, 4, 5, 6);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 3l, null, 15l, null, 10l);
    assertCorrectValues(columnIndex.getMinValues(), null, 2l, null, 4l, null, 9l);
  }

  @Test
  public void testNoOpBuilder() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getNoOpBuilder();
    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(
        Types.required(BINARY).as(UTF8).named("test_binary_utf8"),
        stringBinary("Jeltz"),
        stringBinary("Slartibartfast"),
        null,
        null));
    builder.add(sb.stats(Types.required(BOOLEAN).named("test_boolean"), true, true, null, null));
    builder.add(sb.stats(Types.required(DOUBLE).named("test_double"), null, null, null));
    builder.add(sb.stats(Types.required(INT32).named("test_int32"), null, null));
    builder.add(sb.stats(Types.required(INT64).named("test_int64"), -234l, -42l, null));
    assertEquals(0, builder.getPageCount());
    assertEquals(0, builder.getMinMaxSize());
    assertNull(builder.build());
  }

  @Test
  public void testSizeRequiredElements() {
    final PrimitiveType type = Types.required(DOUBLE).named("element");
    final DoubleColumn col = doubleColumn(type.getName());

    final List<List<Double>> pageValueList = new ArrayList<>();
    pageValueList.add(ImmutableList.of(1.0, 2.0, 3.0));
    pageValueList.add(ImmutableList.of(1.0, 2.0, 3.0, 4.0, 5.0));
    pageValueList.add(ImmutableList.of(-1.0));
    pageValueList.add(ImmutableList.of());
    pageValueList.add(null);

    final ColumnIndex columnIndex = createArrayColumnIndex(type, pageValueList);

    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 0, 0, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, true);
    assertCorrectValues(columnIndex.getMaxValues(), 3.0, 5.0, -1.0, null, null);
    assertCorrectValues(columnIndex.getMinValues(), 1.0, 1.0, -1.0, null, null);

    // we know max array size is 5; all elements of page 2 have size 1; and page 3 and 4 are null or empty
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.EQ, 0), 0, 1, 3, 4);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.EQ, 4), 1);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.EQ, 3), 0, 1);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.LT, 2), 0, 1, 2, 3, 4);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.LTE, 1), 0, 1, 2, 3, 4);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.GT, 0), 0, 1, 2);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.GTE, 0), 0, 1, 2, 3, 4);
  }

  @Test
  public void testSizeOptionalElements() {
    final PrimitiveType type = Types.optional(DOUBLE).named("element");
    final DoubleColumn col = doubleColumn(type.getName());

    final List<Double> listWithNulls = new ArrayList<>();
    listWithNulls.add(null);
    listWithNulls.add(3.0);
    listWithNulls.add(null);

    final List<List<Double>> pageValueList = new ArrayList<>();
    pageValueList.add(listWithNulls);

    final ColumnIndex columnIndex = createArrayColumnIndex(type, pageValueList);

    assertCorrectNullCounts(columnIndex, 2);
    assertCorrectNullPages(columnIndex, false);
    assertCorrectValues(columnIndex.getMaxValues(), 3.0);
    assertCorrectValues(columnIndex.getMinValues(), 3.0);

    // We know that the array values for the page have min size 0 and max size 3
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.EQ, 0), 0);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.EQ, 5));
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.LT, 4), 0);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.LTE, 3), 0);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.GT, 0), 0);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.GT, 3));
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.GTE, 3), 0);
    assertCorrectFiltering(columnIndex, size(col, Operators.Size.Operator.GTE, 4));
  }

  private static ColumnIndex createArrayColumnIndex(PrimitiveType type, List<List<Double>> pageValueList) {
    final ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);

    for (List<Double> pageValues : pageValueList) {
      final StatsBuilder sb = new StatsBuilder();
      boolean isNullOrEmpty = pageValues == null || pageValues.isEmpty();

      final SizeStatistics.Builder sizeStatistics =
          SizeStatistics.newBuilder(type, isNullOrEmpty ? 0 : 1, isNullOrEmpty ? 0 : 1);

      if (isNullOrEmpty) sizeStatistics.add(0, 0);

      if (pageValues != null) {
        for (int i = 0; i < pageValues.size(); i++) {
          if (i == 0) {
            sizeStatistics.add(0, 1);
          } else {
            sizeStatistics.add(1, 1);
          }
        }
      }

      if (pageValues == null) {
        builder.add(sb.stats(type), sizeStatistics.build());
      } else {
        builder.add(sb.stats(type, pageValues.toArray(new Double[0])), sizeStatistics.build());
      }
    }

    return builder.build();
  }

  private static List<ByteBuffer> toBBList(Binary... values) {
    List<ByteBuffer> buffers = new ArrayList<>(values.length);
    for (Binary value : values) {
      if (value == null) {
        buffers.add(ByteBuffer.allocate(0));
      } else {
        buffers.add(value.toByteBuffer());
      }
    }
    return buffers;
  }

  private static List<ByteBuffer> toBBList(Boolean... values) {
    List<ByteBuffer> buffers = new ArrayList<>(values.length);
    for (Boolean value : values) {
      if (value == null) {
        buffers.add(ByteBuffer.allocate(0));
      } else {
        buffers.add(ByteBuffer.wrap(BytesUtils.booleanToBytes(value)));
      }
    }
    return buffers;
  }

  private static List<ByteBuffer> toBBList(Double... values) {
    List<ByteBuffer> buffers = new ArrayList<>(values.length);
    for (Double value : values) {
      if (value == null) {
        buffers.add(ByteBuffer.allocate(0));
      } else {
        buffers.add(ByteBuffer.wrap(BytesUtils.longToBytes(Double.doubleToLongBits(value))));
      }
    }
    return buffers;
  }

  private static List<ByteBuffer> toBBList(Float... values) {
    List<ByteBuffer> buffers = new ArrayList<>(values.length);
    for (Float value : values) {
      if (value == null) {
        buffers.add(ByteBuffer.allocate(0));
      } else {
        buffers.add(ByteBuffer.wrap(BytesUtils.intToBytes(Float.floatToIntBits(value))));
      }
    }
    return buffers;
  }

  private static List<ByteBuffer> toBBList(Integer... values) {
    List<ByteBuffer> buffers = new ArrayList<>(values.length);
    for (Integer value : values) {
      if (value == null) {
        buffers.add(ByteBuffer.allocate(0));
      } else {
        buffers.add(ByteBuffer.wrap(BytesUtils.intToBytes(value)));
      }
    }
    return buffers;
  }

  private static List<ByteBuffer> toBBList(Long... values) {
    List<ByteBuffer> buffers = new ArrayList<>(values.length);
    for (Long value : values) {
      if (value == null) {
        buffers.add(ByteBuffer.allocate(0));
      } else {
        buffers.add(ByteBuffer.wrap(BytesUtils.longToBytes(value)));
      }
    }
    return buffers;
  }

  private static Binary decimalBinary(String num) {
    return Binary.fromConstantByteArray(new BigDecimal(num).unscaledValue().toByteArray());
  }

  private static Binary stringBinary(String str) {
    return Binary.fromString(str);
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Binary... expectedValues) {
    assertEquals(expectedValues.length, values.size());
    for (int i = 0; i < expectedValues.length; ++i) {
      Binary expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertFalse("The byte buffer should be empty for null pages", value.hasRemaining());
      } else {
        assertArrayEquals("Invalid value for page " + i, expectedValue.getBytesUnsafe(), value.array());
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Boolean... expectedValues) {
    assertEquals(expectedValues.length, values.size());
    for (int i = 0; i < expectedValues.length; ++i) {
      Boolean expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertFalse("The byte buffer should be empty for null pages", value.hasRemaining());
      } else {
        assertEquals("The byte buffer should be 1 byte long for boolean", 1, value.remaining());
        assertEquals("Invalid value for page " + i, expectedValue.booleanValue(), value.get(0) != 0);
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Double... expectedValues) {
    assertEquals(expectedValues.length, values.size());
    for (int i = 0; i < expectedValues.length; ++i) {
      Double expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertFalse("The byte buffer should be empty for null pages", value.hasRemaining());
      } else {
        assertEquals("The byte buffer should be 8 bytes long for double", 8, value.remaining());
        assertTrue(
            "Invalid value for page " + i,
            Double.compare(expectedValue.doubleValue(), value.getDouble(0)) == 0);
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Float... expectedValues) {
    assertEquals(expectedValues.length, values.size());
    for (int i = 0; i < expectedValues.length; ++i) {
      Float expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertFalse("The byte buffer should be empty for null pages", value.hasRemaining());
      } else {
        assertEquals("The byte buffer should be 4 bytes long for double", 4, value.remaining());
        assertTrue(
            "Invalid value for page " + i,
            Float.compare(expectedValue.floatValue(), value.getFloat(0)) == 0);
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Integer... expectedValues) {
    assertEquals(expectedValues.length, values.size());
    for (int i = 0; i < expectedValues.length; ++i) {
      Integer expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertFalse("The byte buffer should be empty for null pages", value.hasRemaining());
      } else {
        assertEquals("The byte buffer should be 4 bytes long for int32", 4, value.remaining());
        assertEquals("Invalid value for page " + i, expectedValue.intValue(), value.getInt(0));
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Long... expectedValues) {
    assertEquals(expectedValues.length, values.size());
    for (int i = 0; i < expectedValues.length; ++i) {
      Long expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertFalse("The byte buffer should be empty for null pages", value.hasRemaining());
      } else {
        assertEquals("The byte buffer should be 8 bytes long for int64", 8, value.remaining());
        assertEquals("Invalid value for page " + i, expectedValue.intValue(), value.getLong(0));
      }
    }
  }

  private static void assertCorrectNullCounts(ColumnIndex columnIndex, long... expectedNullCounts) {
    List<Long> nullCounts = columnIndex.getNullCounts();
    assertEquals(expectedNullCounts.length, nullCounts.size());
    for (int i = 0; i < expectedNullCounts.length; ++i) {
      assertEquals(
          "Invalid null count at page " + i,
          expectedNullCounts[i],
          nullCounts.get(i).longValue());
    }
  }

  private static void assertCorrectNullPages(ColumnIndex columnIndex, boolean... expectedNullPages) {
    List<Boolean> nullPages = columnIndex.getNullPages();
    assertEquals(expectedNullPages.length, nullPages.size());
    for (int i = 0; i < expectedNullPages.length; ++i) {
      assertEquals(
          "Invalid null pages at page " + i,
          expectedNullPages[i],
          nullPages.get(i).booleanValue());
    }
  }

  private static class StatsBuilder {
    private long minMaxSize;

    Statistics<?> stats(PrimitiveType type, Object... values) {
      Statistics<?> stats = Statistics.createStats(type);
      for (Object value : values) {
        if (value == null) {
          stats.incrementNumNulls();
          continue;
        }
        switch (type.getPrimitiveTypeName()) {
          case BINARY:
          case FIXED_LEN_BYTE_ARRAY:
          case INT96:
            stats.updateStats((Binary) value);
            break;
          case BOOLEAN:
            stats.updateStats((boolean) value);
            break;
          case DOUBLE:
            stats.updateStats((double) value);
            break;
          case FLOAT:
            stats.updateStats((float) value);
            break;
          case INT32:
            stats.updateStats((int) value);
            break;
          case INT64:
            stats.updateStats((long) value);
            break;
          default:
            fail("Unsupported value type for stats: " + value.getClass());
        }
      }
      if (stats.hasNonNullValue()) {
        minMaxSize += stats.getMinBytes().length;
        minMaxSize += stats.getMaxBytes().length;
      }
      return stats;
    }

    long getMinMaxSize() {
      return minMaxSize;
    }
  }

  private static void assertCorrectFiltering(ColumnIndex ci, FilterPredicate predicate, int... expectedIndexes) {
    TestIndexIterator.assertEquals(predicate.accept(ci), expectedIndexes);
  }
}
