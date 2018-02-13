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
package org.apache.parquet.column.columnindex;

import static java.util.Arrays.asList;
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
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

/**
 * Tests for {@link ColumnIndexBuilder}.
 */
public class TestColumnIndexBuilder {

  @Test
  public void testBuildBinaryDecimal() {
    PrimitiveType type = Types.required(BINARY).as(DECIMAL).precision(12).scale(2).named("test_binary_decimal");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type);
    assertThat(builder, instanceOf(BinaryColumnIndexBuilder.class));

    builder.add(stats(type, null, null));
    builder.add(stats(type, decimalBinary("-0.17"), decimalBinary("1234567890.12")));
    builder.add(stats(type, decimalBinary("-234.23"), null, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, decimalBinary("-9999293.23"), decimalBinary("2348978.45")));
    builder.add(stats(type, null, null, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, decimalBinary("87656273")));
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 0, 3, 3, 0, 4, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(),
        null,
        decimalBinary("1234567890.12"),
        decimalBinary("-234.23"),
        null,
        decimalBinary("2348978.45"),
        null,
        null,
        decimalBinary("87656273"));
    assertCorrectValues(columnIndex.getMinValues(),
        null,
        decimalBinary("-0.17"),
        decimalBinary("-234.23"),
        null,
        decimalBinary("-9999293.23"),
        null,
        null,
        decimalBinary("87656273"));
    assertNull(builder.build());

    builder.add(stats(type, null, null, null, null));
    builder.add(stats(type, decimalBinary("-9999293.23"), decimalBinary("-234.23")));
    builder.add(stats(type, decimalBinary("-0.17"), decimalBinary("87656273")));
    builder.add(stats(type, null, null));
    builder.add(stats(type, decimalBinary("87656273")));
    builder.add(stats(type, null, null));
    builder.add(stats(type, decimalBinary("1234567890.12"), null, null, null));
    builder.add(stats(type, null, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 4, 0, 0, 2, 0, 2, 3, 3);
    assertCorrectNullPages(columnIndex, true, false, false, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(),
        null,
        decimalBinary("-234.23"),
        decimalBinary("87656273"),
        null,
        decimalBinary("87656273"),
        null,
        decimalBinary("1234567890.12"),
        null);
    assertCorrectValues(columnIndex.getMinValues(),
        null,
        decimalBinary("-9999293.23"),
        decimalBinary("-0.17"),
        null,
        decimalBinary("87656273"),
        null,
        decimalBinary("1234567890.12"),
        null);
    assertNull(builder.build());

    builder.add(stats(type, null, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, decimalBinary("1234567890.12"), null, null, null));
    builder.add(stats(type, null, null, null, null));
    builder.add(stats(type, decimalBinary("87656273")));
    builder.add(stats(type, decimalBinary("87656273"), decimalBinary("-0.17")));
    builder.add(stats(type, null, null));
    builder.add(stats(type, decimalBinary("-234.23"), decimalBinary("-9999293.23")));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 3, 2, 3, 4, 0, 0, 2, 0);
    assertCorrectNullPages(columnIndex, true, true, false, true, false, false, true, false);
    assertCorrectValues(columnIndex.getMaxValues(),
        null,
        null,
        decimalBinary("1234567890.12"),
        null,
        decimalBinary("87656273"),
        decimalBinary("87656273"),
        null,
        decimalBinary("-234.23"));
    assertCorrectValues(columnIndex.getMinValues(),
        null,
        null,
        decimalBinary("1234567890.12"),
        null,
        decimalBinary("87656273"),
        decimalBinary("-0.17"),
        null,
        decimalBinary("-9999293.23"));
    assertNull(builder.build());
  }

  @Test
  public void testBuildBinaryUtf8() {
    PrimitiveType type = Types.required(BINARY).as(UTF8).named("test_binary_utf8");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type);
    assertThat(builder, instanceOf(BinaryColumnIndexBuilder.class));

    builder.add(stats(type, null, null));
    builder.add(stats(type, stringBinary("Jeltz"), stringBinary("Slartibartfast"), null, null));
    builder.add(stats(type, null, null, null, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, stringBinary("Beeblebrox"), stringBinary("Perfect")));
    builder.add(stats(type, stringBinary("Dent"), stringBinary("Trilian"), null));
    builder.add(stats(type, stringBinary("Beeblebrox")));
    builder.add(stats(type, null, null));
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 5, 2, 0, 1, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, true, true, false, false, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, stringBinary("Slartibartfast"), null, null,
        stringBinary("Perfect"),
        stringBinary("Trilian"), stringBinary("Beeblebrox"), null);
    assertCorrectValues(columnIndex.getMinValues(), null, stringBinary("Jeltz"), null, null, stringBinary("Beeblebrox"),
        stringBinary("Dent"), stringBinary("Beeblebrox"), null);
    assertNull(builder.build());

    builder.add(stats(type, stringBinary("Beeblebrox"), stringBinary("Dent"), null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null, null, null, null));
    builder.add(stats(type, stringBinary("Dent"), stringBinary("Dent")));
    builder.add(stats(type, stringBinary("Jeltz"), stringBinary("Perfect"), null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, stringBinary("Slartibartfast")));
    builder.add(stats(type, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 5, 0, 1, 2, 0, 2);
    assertCorrectNullPages(columnIndex, false, true, true, false, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), stringBinary("Dent"), null, null, stringBinary("Dent"),
        stringBinary("Perfect"), null,
        stringBinary("Slartibartfast"), null);
    assertCorrectValues(columnIndex.getMinValues(), stringBinary("Beeblebrox"), null, null, stringBinary("Dent"),
        stringBinary("Jeltz"),
        null, stringBinary("Slartibartfast"), null);
    assertNull(builder.build());

    builder.add(stats(type, null, null));
    builder.add(stats(type, stringBinary("Slartibartfast")));
    builder.add(stats(type, null, null, null, null, null));
    builder.add(stats(type, stringBinary("Perfect"), stringBinary("Jeltz"), null));
    builder.add(stats(type, stringBinary("Dent"), stringBinary("Dent")));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, stringBinary("Dent"), stringBinary("Beeblebrox"), null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 0, 5, 1, 0, 2, 2, 2);
    assertCorrectNullPages(columnIndex, true, false, true, false, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, stringBinary("Slartibartfast"), null, stringBinary("Perfect"),
        stringBinary("Dent"), null, null, stringBinary("Dent"));
    assertCorrectValues(columnIndex.getMinValues(), null, stringBinary("Slartibartfast"), null, stringBinary("Jeltz"),
        stringBinary("Dent"), null, null, stringBinary("Beeblebrox"));
    assertNull(builder.build());
  }

  @Test
  public void testStaticBuildBinary() {
    ColumnIndex columnIndex = ColumnIndexBuilder.build(
        Types.required(BINARY).as(UTF8).named("test_binary_utf8"),
        BoundaryOrder.ASCENDING,
        asList(true, true, false, false, true, false, true, false),
        asList(1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l),
        toBBList(null, null, stringBinary("Beeblebrox"), stringBinary("Dent"), null, stringBinary("Jeltz"), null,
            stringBinary("Slartibartfast")),
        toBBList(null, null, stringBinary("Dent"), stringBinary("Dent"), null, stringBinary("Perfect"), null,
            stringBinary("Slartibartfast")));
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 1, 2, 3, 4, 5, 6, 7, 8);
    assertCorrectNullPages(columnIndex, true, true, false, false, true, false, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, null, stringBinary("Dent"), stringBinary("Dent"), null,
        stringBinary("Perfect"), null, stringBinary("Slartibartfast"));
    assertCorrectValues(columnIndex.getMinValues(), null, null, stringBinary("Beeblebrox"), stringBinary("Dent"), null,
        stringBinary("Jeltz"), null, stringBinary("Slartibartfast"));
  }

  @Test
  public void testBuildBoolean() {
    PrimitiveType type = Types.required(BOOLEAN).named("test_boolean");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type);
    assertThat(builder, instanceOf(BooleanColumnIndexBuilder.class));

    builder.add(stats(type, false, true));
    builder.add(stats(type, true, false, null));
    builder.add(stats(type, true, true, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, false, false));
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), true, true, true, null, false);
    assertCorrectValues(columnIndex.getMinValues(), false, false, true, null, false);
    assertNull(builder.build());

    builder.add(stats(type, null, null));
    builder.add(stats(type, false, false));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, null, null, null, null));
    builder.add(stats(type, false, true, null));
    builder.add(stats(type, true, true, null, null));
    builder.add(stats(type, null, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 0, 3, 4, 1, 2, 3);
    assertCorrectNullPages(columnIndex, true, false, true, true, false, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, false, null, null, true, true, null);
    assertCorrectValues(columnIndex.getMinValues(), null, false, null, null, false, true, null);
    assertNull(builder.build());

    builder.add(stats(type, null, null));
    builder.add(stats(type, true, true));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, null, null, null, null));
    builder.add(stats(type, true, false, null));
    builder.add(stats(type, false, false, null, null));
    builder.add(stats(type, null, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 0, 3, 4, 1, 2, 3);
    assertCorrectNullPages(columnIndex, true, false, true, true, false, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, true, null, null, true, false, null);
    assertCorrectValues(columnIndex.getMinValues(), null, true, null, null, false, false, null);
    assertNull(builder.build());
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
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type);
    assertThat(builder, instanceOf(DoubleColumnIndexBuilder.class));

    builder.add(stats(type, -4.2, -4.1));
    builder.add(stats(type, -11.7, 7.0, null));
    builder.add(stats(type, 2.2, 2.2, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 1.9, 2.32));
    builder.add(stats(type, -21.0, 8.1));
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), -4.1, 7.0, 2.2, null, 2.32, 8.1);
    assertCorrectValues(columnIndex.getMinValues(), -4.2, -11.7, 2.2, null, 1.9, -21.0);
    assertNull(builder.build());

    builder.add(stats(type, null, null));
    builder.add(stats(type, -532.3, -345.2, null, null));
    builder.add(stats(type, -234.7, -234.6, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, -234.6, 2.99999));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 3.0, 42.83));
    builder.add(stats(type, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, -345.2, -234.6, null, null, 2.99999, null, 42.83, null);
    assertCorrectValues(columnIndex.getMinValues(), null, -532.3, -234.7, null, null, -234.6, null, 3.0, null);
    assertNull(builder.build());

    builder.add(stats(type, null, null, null, null, null));
    builder.add(stats(type, 532.3, 345.2));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 234.7, 234.6, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 234.6, -2.99999));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, -3.0, -42.83));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 532.3, null, 234.7, null, 234.6, null, null, -3.0);
    assertCorrectValues(columnIndex.getMinValues(), null, 345.2, null, 234.6, null, -2.99999, null, null, -42.83);
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
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type);
    assertThat(builder, instanceOf(FloatColumnIndexBuilder.class));

    builder.add(stats(type, -4.2f, -4.1f));
    builder.add(stats(type, -11.7f, 7.0f, null));
    builder.add(stats(type, 2.2f, 2.2f, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 1.9f, 2.32f));
    builder.add(stats(type, -21.0f, 8.1f));
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), -4.1f, 7.0f, 2.2f, null, 2.32f, 8.1f);
    assertCorrectValues(columnIndex.getMinValues(), -4.2f, -11.7f, 2.2f, null, 1.9f, -21.0f);
    assertNull(builder.build());

    builder.add(stats(type, null, null));
    builder.add(stats(type, -532.3f, -345.2f, null, null));
    builder.add(stats(type, -234.7f, -234.6f, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, -234.6f, 2.99999f));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 3.0f, 42.83f));
    builder.add(stats(type, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, -345.2f, -234.6f, null, null, 2.99999f, null, 42.83f, null);
    assertCorrectValues(columnIndex.getMinValues(), null, -532.3f, -234.7f, null, null, -234.6f, null, 3.0f, null);
    assertNull(builder.build());

    builder.add(stats(type, null, null, null, null, null));
    builder.add(stats(type, 532.3f, 345.2f));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 234.7f, 234.6f, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 234.6f, -2.99999f));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, -3.0f, -42.83f));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 532.3f, null, 234.7f, null, 234.6f, null, null, -3.0f);
    assertCorrectValues(columnIndex.getMinValues(), null, 345.2f, null, 234.6f, null, -2.99999f, null, null, -42.83f);
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
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type);
    assertThat(builder, instanceOf(IntColumnIndexBuilder.class));

    builder.add(stats(type, -4, 10));
    builder.add(stats(type, -11, 7, null));
    builder.add(stats(type, 2, 2, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 1, 2));
    builder.add(stats(type, -21, 8));
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), 10, 7, 2, null, 2, 8);
    assertCorrectValues(columnIndex.getMinValues(), -4, -11, 2, null, 1, -21);
    assertNull(builder.build());

    builder.add(stats(type, null, null));
    builder.add(stats(type, -532, -345, null, null));
    builder.add(stats(type, -234, -42, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, -42, 2));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 3, 42));
    builder.add(stats(type, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, -345, -42, null, null, 2, null, 42, null);
    assertCorrectValues(columnIndex.getMinValues(), null, -532, -234, null, null, -42, null, 3, null);
    assertNull(builder.build());

    builder.add(stats(type, null, null, null, null, null));
    builder.add(stats(type, 532, 345));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 234, 42, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 42, -2));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, -3, -42));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 532, null, 234, null, 42, null, null, -3);
    assertCorrectValues(columnIndex.getMinValues(), null, 345, null, 42, null, -2, null, null, -42);
    assertNull(builder.build());
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
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type);
    assertThat(builder, instanceOf(IntColumnIndexBuilder.class));

    builder.add(stats(type, 4, 10));
    builder.add(stats(type, 11, 17, null));
    builder.add(stats(type, 2, 2, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 1, 0xFF));
    builder.add(stats(type, 0xEF, 0xFA));
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), 10, 17, 2, null, 0xFF, 0xFA);
    assertCorrectValues(columnIndex.getMinValues(), 4, 11, 2, null, 1, 0xEF);
    assertNull(builder.build());

    builder.add(stats(type, null, null));
    builder.add(stats(type, 0, 0, null, null));
    builder.add(stats(type, 0, 42, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 42, 0xEE));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 0xEF, 0xFF));
    builder.add(stats(type, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, 0, 42, null, null, 0xEE, null, 0xFF, null);
    assertCorrectValues(columnIndex.getMinValues(), null, 0, 0, null, null, 42, null, 0xEF, null);
    assertNull(builder.build());

    builder.add(stats(type, null, null, null, null, null));
    builder.add(stats(type, 0xFF, 0xFF));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 0xEF, 0xEE, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 0xEE, 42));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 41, 0));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 0xFF, null, 0xEF, null, 0xEE, null, null, 41);
    assertCorrectValues(columnIndex.getMinValues(), null, 0xFF, null, 0xEE, null, 42, null, null, 0);
    assertNull(builder.build());
  }

  @Test
  public void testBuildInt64() {
    PrimitiveType type = Types.required(INT64).named("test_int64");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type);
    assertThat(builder, instanceOf(LongColumnIndexBuilder.class));

    builder.add(stats(type, -4l, 10l));
    builder.add(stats(type, -11l, 7l, null));
    builder.add(stats(type, 2l, 2l, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 1l, 2l));
    builder.add(stats(type, -21l, 8l));
    ColumnIndex columnIndex = builder.build();
    assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 0l, 1l, 2l, 3l, 0l, 0l);
    assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
    assertCorrectValues(columnIndex.getMaxValues(), 10l, 7l, 2l, null, 2l, 8l);
    assertCorrectValues(columnIndex.getMinValues(), -4l, -11l, 2l, null, 1l, -21l);
    assertNull(builder.build());

    builder.add(stats(type, null, null));
    builder.add(stats(type, -532l, -345l, null, null));
    builder.add(stats(type, -234l, -42l, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, -42l, 2l));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 3l, 42l));
    builder.add(stats(type, null, null));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
    assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
    assertCorrectValues(columnIndex.getMaxValues(), null, -345l, -42l, null, null, 2l, null, 42l, null);
    assertCorrectValues(columnIndex.getMinValues(), null, -532l, -234l, null, null, -42l, null, 3l, null);
    assertNull(builder.build());

    builder.add(stats(type, null, null, null, null, null));
    builder.add(stats(type, 532l, 345l));
    builder.add(stats(type, null, null, null));
    builder.add(stats(type, 234l, 42l, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, 42l, -2l));
    builder.add(stats(type, null, null));
    builder.add(stats(type, null, null));
    builder.add(stats(type, -3l, -42l));
    columnIndex = builder.build();
    assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
    assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
    assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
    assertCorrectValues(columnIndex.getMaxValues(), null, 532l, null, 234l, null, 42l, null, null, -3l);
    assertCorrectValues(columnIndex.getMinValues(), null, 345l, null, 42l, null, -2l, null, null, -42l);
    assertNull(builder.build());
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
    builder.add(stats(Types.required(BINARY).as(UTF8).named("test_binary_utf8"), stringBinary("Jeltz"),
        stringBinary("Slartibartfast"), null, null));
    builder.add(stats(Types.required(BOOLEAN).named("test_boolean"), true, true, null, null));
    builder.add(stats(Types.required(DOUBLE).named("test_double"), null, null, null));
    builder.add(stats(Types.required(INT32).named("test_int32"), null, null));
    builder.add(stats(Types.required(INT64).named("test_int64"), -234l, -42l, null));
    assertNull(builder.build());
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
        assertEquals("Invalid value for page " + i, expectedValue.doubleValue(), value.getDouble(0), 0.0);
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
        assertEquals("Invalid value for page " + i, expectedValue.floatValue(), value.getFloat(0), 0.0f);
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
      assertEquals("Invalid null count at page " + i, expectedNullCounts[i], nullCounts.get(i).longValue());
    }
  }

  private static void assertCorrectNullPages(ColumnIndex columnIndex, boolean... expectedNullPages) {
    List<Boolean> nullPages = columnIndex.getNullPages();
    assertEquals(expectedNullPages.length, nullPages.size());
    for (int i = 0; i < expectedNullPages.length; ++i) {
      assertEquals("Invalid null pages at page " + i, expectedNullPages[i], nullPages.get(i).booleanValue());
    }
  }

  private static Statistics<?> stats(PrimitiveType type, Object... values) {
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
    return stats;
  }
}
