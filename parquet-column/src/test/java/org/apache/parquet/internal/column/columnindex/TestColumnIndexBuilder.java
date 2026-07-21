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
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.predicate.LogicalInverter.invert;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.UINT_8;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.ContainsRewriter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
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
import org.junit.jupiter.api.Test;

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
    assertThat(builder).isInstanceOf(DoubleColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
    DoubleColumn col = doubleColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4.2, -4.1));
    builder.add(sb.stats(type, -11.7, 7.0, null));
    builder.add(sb.stats(type, 2.2, 2.2, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1.9, 2.32));
    builder.add(sb.stats(type, -21.0, 8.1));
    builder.add(sb.stats(type, 10.0, 25.0));
    assertThat(builder.getPageCount()).isEqualTo(7);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder).isInstanceOf(BinaryColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
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
    assertThat(builder.getPageCount()).isEqualTo(8);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(8);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
    assertThat(builder.getPageCount()).isEqualTo(8);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
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
    assertThat(builder).isInstanceOf(BinaryColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
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
    assertThat(builder.getPageCount()).isEqualTo(8);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(8);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
    assertThat(builder.getPageCount()).isEqualTo(8);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
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
    assertThat(builder).isInstanceOf(BinaryColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Jeltz"), stringBinary("Slartibartfast"), null, null));
    builder.add(sb.stats(type, null, null, null, null, null));
    builder.add(sb.stats(type, null, null));
    builder.add(sb.stats(type, stringBinary("Beeblebrox"), stringBinary("Prefect")));
    builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Trilian"), null));
    builder.add(sb.stats(type, stringBinary("Beeblebrox")));
    builder.add(sb.stats(type, null, null));
    assertThat(builder.getPageCount()).isEqualTo(8);
    assertThat(builder.getMinMaxSize()).isEqualTo(39);
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
        List.of(true, true, false, false, true, false, true, false),
        List.of(1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l),
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
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
        List.of(true, true, false, false, true, false, true, false),
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
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    assertThat(columnIndex.getNullCounts()).isNull();
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
    assertThat(builder).isInstanceOf(BooleanColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
    BooleanColumn col = booleanColumn("test_col");

    builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, false, true));
    builder.add(sb.stats(type, true, false, null));
    builder.add(sb.stats(type, true, true, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, false, false));
    assertThat(builder.getPageCount()).isEqualTo(5);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(7);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
    assertThat(builder.getPageCount()).isEqualTo(7);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
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
        List.of(false, true, false, true, false, true),
        List.of(9l, 8l, 7l, 6l, 5l, 4l),
        toBBList(false, null, false, null, true, null),
        toBBList(true, null, false, null, true, null));
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
    assertCorrectNullCounts(columnIndex, 9, 8, 7, 6, 5, 4);
    assertCorrectNullPages(columnIndex, false, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), true, null, false, null, true, null);
    assertCorrectValues(columnIndex.getMinValues(), false, null, false, null, true, null);
  }

  @Test
  public void testBuildDouble() {
    PrimitiveType type = Types.required(DOUBLE).named("test_double");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder).isInstanceOf(DoubleColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
    DoubleColumn col = doubleColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4.2, -4.1));
    builder.add(sb.stats(type, -11.7, 7.0, null));
    builder.add(sb.stats(type, 2.2, 2.2, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1.9, 2.32));
    builder.add(sb.stats(type, -21.0, 8.1));
    assertThat(builder.getPageCount()).isEqualTo(6);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
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
    assertThat(builder.build()).isNull();
  }

  @Test
  public void testStaticBuildDouble() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(DOUBLE).named("test_double"),
        BoundaryOrder.UNORDERED,
        List.of(false, false, false, false, false, false),
        List.of(0l, 1l, 2l, 3l, 4l, 5l),
        toBBList(-1.0, -2.0, -3.0, -4.0, -5.0, -6.0),
        toBBList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0));
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 4, 5);
    assertCorrectNullPages(columnIndex, false, false, false, false, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), 1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
    assertCorrectValues(columnIndex.getMinValues(), -1.0, -2.0, -3.0, -4.0, -5.0, -6.0);
  }

  @Test
  public void testBuildFloat() {
    PrimitiveType type = Types.required(FLOAT).named("test_float");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder).isInstanceOf(FloatColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
    FloatColumn col = floatColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4.2f, -4.1f));
    builder.add(sb.stats(type, -11.7f, 7.0f, null));
    builder.add(sb.stats(type, 2.2f, 2.2f, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1.9f, 2.32f));
    builder.add(sb.stats(type, -21.0f, 8.1f));
    assertThat(builder.getPageCount()).isEqualTo(6);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
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
    assertThat(builder.build()).isNull();
  }

  @Test
  public void testStaticBuildFloat() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(FLOAT).named("test_float"),
        BoundaryOrder.ASCENDING,
        List.of(true, true, true, false, false, false),
        List.of(9l, 8l, 7l, 6l, 0l, 0l),
        toBBList(null, null, null, -3.0f, -2.0f, 0.1f),
        toBBList(null, null, null, -2.0f, 0.0f, 6.0f));
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    assertCorrectNullCounts(columnIndex, 9, 8, 7, 6, 0, 0);
    assertCorrectNullPages(columnIndex, true, true, true, false, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, null, null, -2.0f, 0.0f, 6.0f);
    assertCorrectValues(columnIndex.getMinValues(), null, null, null, -3.0f, -2.0f, 0.1f);
  }

  @Test
  public void testBuildInt32() {
    PrimitiveType type = Types.required(INT32).named("test_int32");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder).isInstanceOf(IntColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
    IntColumn col = intColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4, 10));
    builder.add(sb.stats(type, -11, 7, null));
    builder.add(sb.stats(type, 2, 2, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1, 2));
    builder.add(sb.stats(type, -21, 8));
    assertThat(builder.getPageCount()).isEqualTo(6);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
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
        List.of(false, false, false, true, true, true),
        List.of(0l, 10l, 0l, 3l, 5l, 7l),
        toBBList(10, 8, 6, null, null, null),
        toBBList(9, 7, 5, null, null, null));
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
    assertCorrectNullCounts(columnIndex, 0, 10, 0, 3, 5, 7);
    assertCorrectNullPages(columnIndex, false, false, false, true, true, true);
    assertCorrectValues(columnIndex.getMaxValues(), 9, 7, 5, null, null, null);
    assertCorrectValues(columnIndex.getMinValues(), 10, 8, 6, null, null, null);
  }

  @Test
  public void testBuildUInt8() {
    PrimitiveType type = Types.required(INT32).as(UINT_8).named("test_uint8");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder).isInstanceOf(IntColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
    IntColumn col = intColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, 4, 10));
    builder.add(sb.stats(type, 11, 17, null));
    builder.add(sb.stats(type, 2, 2, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1, 0xFF));
    builder.add(sb.stats(type, 0xEF, 0xFA));
    assertThat(builder.getPageCount()).isEqualTo(6);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
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
    assertThat(builder).isInstanceOf(LongColumnIndexBuilder.class);
    assertThat(builder.build()).isNull();
    LongColumn col = longColumn("test_col");

    StatsBuilder sb = new StatsBuilder();
    builder.add(sb.stats(type, -4l, 10l));
    builder.add(sb.stats(type, -11l, 7l, null));
    builder.add(sb.stats(type, 2l, 2l, null, null));
    builder.add(sb.stats(type, null, null, null));
    builder.add(sb.stats(type, 1l, 2l));
    builder.add(sb.stats(type, -21l, 8l));
    assertThat(builder.getPageCount()).isEqualTo(6);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
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
    assertThat(builder.getPageCount()).isEqualTo(9);
    assertThat(builder.getMinMaxSize()).isEqualTo(sb.getMinMaxSize());
    columnIndex = builder.build();
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
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
        List.of(true, false, true, false, true, false),
        List.of(1l, 2l, 3l, 4l, 5l, 6l),
        toBBList(null, 2l, null, 4l, null, 9l),
        toBBList(null, 3l, null, 15l, null, 10l));
    assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
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
    assertThat(builder.getPageCount()).isEqualTo(0);
    assertThat(builder.getMinMaxSize()).isEqualTo(0);
    assertThat(builder.build()).isNull();
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
    assertThat(values).hasSameSizeAs(expectedValues);
    for (int i = 0; i < expectedValues.length; ++i) {
      Binary expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertThat(value.remaining())
            .as("The byte buffer should be empty for null pages")
            .isZero();
      } else {
        assertThat(value.array()).as("Invalid value for page " + i).isEqualTo(expectedValue.getBytesUnsafe());
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Boolean... expectedValues) {
    assertThat(values).hasSameSizeAs(expectedValues);
    for (int i = 0; i < expectedValues.length; ++i) {
      Boolean expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertThat(value.remaining())
            .as("The byte buffer should be empty for null pages")
            .isZero();
      } else {
        assertThat(value.remaining())
            .as("The byte buffer should be 1 byte long for boolean")
            .isEqualTo(1);
        assertThat(value.get(0) != 0).isEqualTo(expectedValue.booleanValue());
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Double... expectedValues) {
    assertThat(values).hasSameSizeAs(expectedValues);
    for (int i = 0; i < expectedValues.length; ++i) {
      Double expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertThat(value.remaining())
            .as("The byte buffer should be empty for null pages")
            .isZero();
      } else {
        assertThat(value.remaining())
            .as("The byte buffer should be 8 bytes long for double")
            .isEqualTo(8);
        assertThat(value.getDouble(0))
            .as("Invalid value for page " + i)
            .usingComparator(Double::compare)
            .isEqualByComparingTo(expectedValue);
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Float... expectedValues) {
    assertThat(values).hasSameSizeAs(expectedValues);
    for (int i = 0; i < expectedValues.length; ++i) {
      Float expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertThat(value.remaining())
            .as("The byte buffer should be empty for null pages")
            .isZero();
      } else {
        assertThat(value.remaining())
            .as("The byte buffer should be 4 bytes long for double")
            .isEqualTo(4);
        assertThat(value.getFloat(0))
            .as("Invalid value for page " + i)
            .usingComparator(Float::compare)
            .isEqualByComparingTo(expectedValue);
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Integer... expectedValues) {
    assertThat(values).hasSameSizeAs(expectedValues);
    for (int i = 0; i < expectedValues.length; ++i) {
      Integer expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertThat(value.remaining())
            .as("The byte buffer should be empty for null pages")
            .isZero();
      } else {
        assertThat(value.remaining())
            .as("The byte buffer should be 4 bytes long for int32")
            .isEqualTo(4);
        assertThat(value.getInt(0)).isEqualTo(expectedValue.intValue());
      }
    }
  }

  private static void assertCorrectValues(List<ByteBuffer> values, Long... expectedValues) {
    assertThat(values).hasSameSizeAs(expectedValues);
    for (int i = 0; i < expectedValues.length; ++i) {
      Long expectedValue = expectedValues[i];
      ByteBuffer value = values.get(i);
      if (expectedValue == null) {
        assertThat(value.remaining())
            .as("The byte buffer should be empty for null pages")
            .isZero();
      } else {
        assertThat(value.remaining())
            .as("The byte buffer should be 8 bytes long for int64")
            .isEqualTo(8);
        assertThat(value.getLong(0)).isEqualTo(expectedValue.intValue());
      }
    }
  }

  private static void assertCorrectNullCounts(ColumnIndex columnIndex, long... expectedNullCounts) {
    List<Long> nullCounts = columnIndex.getNullCounts();
    assertThat(nullCounts).hasSameSizeAs(expectedNullCounts);
    for (int i = 0; i < expectedNullCounts.length; ++i) {
      assertThat(nullCounts.get(i).longValue()).isEqualTo(expectedNullCounts[i]);
    }
  }

  private static void assertCorrectNullPages(ColumnIndex columnIndex, boolean... expectedNullPages) {
    List<Boolean> nullPages = columnIndex.getNullPages();
    assertThat(nullPages).hasSameSizeAs(expectedNullPages);
    for (int i = 0; i < expectedNullPages.length; ++i) {
      assertThat(nullPages.get(i).booleanValue()).isEqualTo(expectedNullPages[i]);
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
    assertThat(predicate.accept(ci))
        .toIterable()
        .containsExactly(Arrays.stream(expectedIndexes).boxed().toArray(Integer[]::new));
  }

  @Test
  public void testBuildReturnsNullForNullPageCountContradiction() {
    // null_pages[i]=true indicates the page is entirely null, but null_counts[i]=0
    // says there are zero null values. This contradiction indicates invalid metadata.
    // build() should return null to prevent incorrect page skipping.
    PrimitiveType type = Types.required(INT32).named("test_col");

    // Pages 1-3 have null_pages=true with null_counts=0 — contradictory
    assertThat(ColumnIndexBuilder.build(
            type,
            BoundaryOrder.ASCENDING,
            List.of(false, true, true, true),
            List.of(0L, 0L, 0L, 0L),
            toBBList(Integer.valueOf(-99), null, null, null),
            toBBList(Integer.valueOf(5), null, null, null)))
        .as("Column index with null_pages=true and null_counts=0 should be rejected")
        .isNull();

    // Contradiction on a single page (last page) should also be rejected
    assertThat(ColumnIndexBuilder.build(
            type,
            BoundaryOrder.UNORDERED,
            List.of(false, false, true),
            List.of(0L, 5L, 0L),
            toBBList(Integer.valueOf(1), Integer.valueOf(50), null),
            toBBList(Integer.valueOf(49), Integer.valueOf(99), null)))
        .as("Single contradictory page should cause rejection")
        .isNull();

    // Contradiction on the first page
    assertThat(ColumnIndexBuilder.build(
            type,
            BoundaryOrder.UNORDERED,
            List.of(true, false, false),
            List.of(0L, 0L, 5L),
            toBBList(null, Integer.valueOf(1), Integer.valueOf(50)),
            toBBList(null, Integer.valueOf(49), Integer.valueOf(99))))
        .as("Contradictory first page should cause rejection")
        .isNull();

    // Single page with contradiction
    assertThat(ColumnIndexBuilder.build(
            type,
            BoundaryOrder.UNORDERED,
            List.of(true),
            List.of(0L),
            toBBList((Integer) null),
            toBBList((Integer) null)))
        .as("Single-page column index with contradiction should be rejected")
        .isNull();

    // All pages are contradictory null pages
    assertThat(ColumnIndexBuilder.build(
            type,
            BoundaryOrder.UNORDERED,
            List.of(true, true, true),
            List.of(0L, 0L, 0L),
            toBBList((Integer) null, null, null),
            toBBList((Integer) null, null, null)))
        .as("All-contradictory column index should be rejected")
        .isNull();
  }

  @Test
  public void testBuildPreservesValidColumnIndex() {
    PrimitiveType type = Types.required(INT32).named("test_col");

    // Legitimate null page: null_pages=true with null_counts > 0 — valid
    ColumnIndex ci = ColumnIndexBuilder.build(
        type,
        BoundaryOrder.ASCENDING,
        List.of(false, true, false),
        List.of(0L, 100L, 0L),
        toBBList(Integer.valueOf(1), null, Integer.valueOf(50)),
        toBBList(Integer.valueOf(49), null, Integer.valueOf(99)));
    assertCorrectNullPages(ci, false, true, false);
    assertCorrectNullCounts(ci, 0, 100, 0);
    assertCorrectValues(ci.getMinValues(), 1, null, 50);
    assertCorrectValues(ci.getMaxValues(), 49, null, 99);

    // All non-null pages — valid
    ColumnIndex ci2 = ColumnIndexBuilder.build(
        type,
        BoundaryOrder.ASCENDING,
        List.of(false, false, false),
        List.of(0L, 5L, 10L),
        toBBList(Integer.valueOf(1), Integer.valueOf(50), Integer.valueOf(100)),
        toBBList(Integer.valueOf(49), Integer.valueOf(99), Integer.valueOf(150)));
    assertCorrectNullPages(ci2, false, false, false);
    assertCorrectNullCounts(ci2, 0, 5, 10);

    // Single non-null page
    ColumnIndex ci3 = ColumnIndexBuilder.build(
        type,
        BoundaryOrder.UNORDERED,
        List.of(false),
        List.of(0L),
        toBBList(Integer.valueOf(42)),
        toBBList(Integer.valueOf(42)));
    assertCorrectNullPages(ci3, false);
    assertCorrectNullCounts(ci3, 0);

    // Single legitimate all-null page (null_pages=true, null_counts > 0)
    ColumnIndex ci4 = ColumnIndexBuilder.build(
        type, BoundaryOrder.UNORDERED, List.of(true), List.of(50L), toBBList((Integer) null), toBBList((Integer)
            null));
    assertCorrectNullPages(ci4, true);
    assertCorrectNullCounts(ci4, 50);

    // All pages legitimately null
    ColumnIndex ci5 = ColumnIndexBuilder.build(
        type,
        BoundaryOrder.UNORDERED,
        List.of(true, true, true),
        List.of(10L, 20L, 30L),
        toBBList((Integer) null, null, null),
        toBBList((Integer) null, null, null));
    assertCorrectNullPages(ci5, true, true, true);
    assertCorrectNullCounts(ci5, 10, 20, 30);

    // Boundary: null_counts=1 on a null page (minimum valid value) — should NOT be rejected
    ColumnIndex ci6 = ColumnIndexBuilder.build(
        type,
        BoundaryOrder.UNORDERED,
        List.of(false, true),
        List.of(0L, 1L),
        toBBList(Integer.valueOf(1), null),
        toBBList(Integer.valueOf(99), null));
    assertCorrectNullPages(ci6, false, true);
    assertCorrectNullCounts(ci6, 0, 1);
  }

  @Test
  public void testBuildFlba12Timestamp() {
    // FLBA(12) TIMESTAMP column with negative (pre-1970) and positive (post-1970) values.
    // The LE signed comparator must order negatives before zero before positives.
    PrimitiveType type = Types.required(FIXED_LEN_BYTE_ARRAY)
        .length(12)
        .as(timestampType(true, NANOS))
        .named("ts");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
    assertThat(builder).isInstanceOf(BinaryColumnIndexBuilder.class);

    // -1 (all 0xFF) encodes a value just before epoch — smallest in the test
    byte[] negOne = new byte[12];
    for (int i = 0; i < 12; i++) negOne[i] = (byte) 0xFF;
    // 0 (all 0x00) — epoch
    byte[] epoch = new byte[12];
    // +1 (0x01 followed by zeros) — just after epoch
    byte[] posOne = new byte[12];
    posOne[0] = 1;

    StatsBuilder sb = new StatsBuilder();
    // Page 0: only negative values, min=-1 max=-1
    builder.add(sb.stats(type, Binary.fromConstantByteArray(negOne)));
    // Page 1: straddles epoch, min=-1 max=+1
    builder.add(sb.stats(type, Binary.fromConstantByteArray(negOne), Binary.fromConstantByteArray(posOne)));
    // Page 2: only positive values, min=epoch max=+1
    builder.add(sb.stats(type, Binary.fromConstantByteArray(epoch), Binary.fromConstantByteArray(posOne)));

    ColumnIndex columnIndex = builder.build();
    assertThat(columnIndex).isNotNull();
    // min of page 0 must equal -1
    assertCorrectValues(
        columnIndex.getMinValues(),
        Binary.fromConstantByteArray(negOne),
        Binary.fromConstantByteArray(negOne),
        Binary.fromConstantByteArray(epoch));
    // max of page 0 must equal -1 (all values are negative)
    assertCorrectValues(
        columnIndex.getMaxValues(),
        Binary.fromConstantByteArray(negOne),
        Binary.fromConstantByteArray(posOne),
        Binary.fromConstantByteArray(posOne));
  }

  @Test
  public void testBuildWithoutNullCountsIsNotRejected() {
    PrimitiveType type = Types.required(INT32).named("test_col");

    // null_counts absent (optional field) — cannot detect contradiction, should build normally
    ColumnIndex ci = ColumnIndexBuilder.build(
        type,
        BoundaryOrder.UNORDERED,
        List.of(false, true, true),
        null,
        toBBList(Integer.valueOf(1), null, null),
        toBBList(Integer.valueOf(99), null, null));
    assertCorrectNullPages(ci, false, true, true);
    assertThat(ci.getNullCounts())
        .as("null_counts should be null when not provided")
        .isNull();
  }
}
