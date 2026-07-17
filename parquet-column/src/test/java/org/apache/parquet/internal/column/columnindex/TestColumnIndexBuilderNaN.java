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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PrimitiveIterator;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

/**
 * Tests for ColumnIndex NaN handling under IEEE_754_TOTAL_ORDER and TYPE_DEFINED_ORDER.
 */
public class TestColumnIndexBuilderNaN {

  private static final PrimitiveType FLOAT_TYPE =
      Types.required(PrimitiveTypeName.FLOAT).named("test_float");
  private static final PrimitiveType FLOAT_IEEE754_TYPE = Types.required(PrimitiveTypeName.FLOAT)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("test_float_ieee754");
  private static final PrimitiveType DOUBLE_TYPE =
      Types.required(PrimitiveTypeName.DOUBLE).named("test_double");
  private static final PrimitiveType DOUBLE_IEEE754_TYPE = Types.required(PrimitiveTypeName.DOUBLE)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("test_double_ieee754");
  private static final PrimitiveType FLOAT16_TYPE = Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
      .length(2)
      .as(LogicalTypeAnnotation.float16Type())
      .named("test_float16");
  private static final PrimitiveType FLOAT16_IEEE754_TYPE = Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
      .length(2)
      .as(LogicalTypeAnnotation.float16Type())
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("test_float16_ieee754");

  private static final FloatColumn FLOAT_COL = FilterApi.floatColumn("test_float_ieee754");
  private static final DoubleColumn DOUBLE_COL = FilterApi.doubleColumn("test_double_ieee754");
  private static final BinaryColumn FLOAT16_COL = FilterApi.binaryColumn("test_float16_ieee754");

  private static Binary float16Binary(short h) {
    return Binary.fromConstantByteArray(new byte[] {(byte) (h & 0xFF), (byte) ((h >> 8) & 0xFF)});
  }

  private static final Binary FLOAT16_NAN = float16Binary((short) 0x7e00);
  private static final Binary FLOAT16_NAN_SMALL = float16Binary((short) 0x7c01);
  private static final Binary FLOAT16_NAN_LARGE = float16Binary((short) 0x7fff);
  private static final Binary FLOAT16_ONE = float16Binary((short) 0x3C00); // 1.0
  private static final Binary FLOAT16_TWO = float16Binary((short) 0x4000); // 2.0
  private static final Binary FLOAT16_THREE = float16Binary((short) 0x4200); // 3.0
  private static final Binary FLOAT16_FOUR = float16Binary((short) 0x4400); // 4.0

  private static Statistics<?> floatStats(PrimitiveType type, float... values) {
    Statistics<?> stats = Statistics.createStats(type);
    for (float value : values) {
      stats.updateStats(value);
    }
    return stats;
  }

  private static Statistics<?> doubleStats(PrimitiveType type, double... values) {
    Statistics<?> stats = Statistics.createStats(type);
    for (double value : values) {
      stats.updateStats(value);
    }
    return stats;
  }

  private static Statistics<?> binaryStats(PrimitiveType type, Binary... values) {
    Statistics<?> stats = Statistics.createStats(type);
    for (Binary value : values) {
      stats.updateStats(value);
    }
    return stats;
  }

  private static List<Integer> toList(PrimitiveIterator.OfInt iter) {
    List<Integer> result = new ArrayList<>();
    iter.forEachRemaining((int i) -> result.add(i));
    return result;
  }

  private static ByteBuffer floatBuffer(float value) {
    ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putFloat(0, value);
    return buffer;
  }

  // TYPE_DEFINED_ORDER: build column index with NaN

  @Test
  public void testFloatTypeDefinedOrderNaN() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT_TYPE, Integer.MAX_VALUE);
    builder.add(floatStats(FLOAT_TYPE, 1.0f, 2.0f));
    builder.add(floatStats(FLOAT_TYPE, Float.NaN));
    builder.add(floatStats(FLOAT_TYPE, 3.0f, 4.0f));
    assertThat(builder.build()).isNull();
  }

  @Test
  public void testDoubleTypeDefinedOrderNaN() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(DOUBLE_TYPE, Integer.MAX_VALUE);
    builder.add(doubleStats(DOUBLE_TYPE, 1.0, 2.0));
    builder.add(doubleStats(DOUBLE_TYPE, Double.NaN));
    builder.add(doubleStats(DOUBLE_TYPE, 3.0, 4.0));
    assertThat(builder.build()).isNull();
  }

  @Test
  public void testFloat16TypeDefinedOrderNaN() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT16_TYPE, Integer.MAX_VALUE);
    builder.add(binaryStats(FLOAT16_TYPE, FLOAT16_ONE, FLOAT16_TWO));
    builder.add(binaryStats(FLOAT16_TYPE, FLOAT16_NAN));
    builder.add(binaryStats(FLOAT16_TYPE, FLOAT16_ONE));
    assertThat(builder.build()).isNull();
  }

  // IEEE_754_TOTAL_ORDER: build column index with NaN

  @Test
  public void testFloatIeee754BuildNanCounts() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(floatStats(FLOAT_IEEE754_TYPE, 1.0f, 2.0f));
    builder.add(floatStats(FLOAT_IEEE754_TYPE, Float.NaN, 2.5f, Float.NaN));
    builder.add(floatStats(FLOAT_IEEE754_TYPE, 3.0f, Float.NaN, 4.0f));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();
    assertThat(ci.getNanCounts()).containsExactly(0L, 2L, 1L);
  }

  @Test
  public void testDoubleIeee754BuildNanCounts() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(DOUBLE_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, 1.0, 2.0));
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, Double.NaN, 2.5, Double.NaN));
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, 3.0, Double.NaN, 4.0));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();
    assertThat(ci.getNanCounts()).containsExactly(0L, 2L, 1L);
  }

  @Test
  public void testFloat16Ieee754BuildNanCounts() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT16_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_ONE, FLOAT16_NAN, FLOAT16_TWO));
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_NAN, FLOAT16_TWO, FLOAT16_NAN));
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_ONE));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();
    assertThat(ci.getNanCounts()).containsExactly(1L, 2L, 0L);
  }

  @Test
  public void testFloatIeee754AllNanPageKeepsNanRangeInColumnIndex() {
    float minNaN = Float.intBitsToFloat(0x7fc00001);
    float maxNaN = Float.intBitsToFloat(0x7fffffff);
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(floatStats(FLOAT_IEEE754_TYPE, maxNaN, minNaN));

    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();
    assertThat(ci.getMinValues()).hasSize(1);
    assertThat(ci.getMinValues().get(0).order(ByteOrder.LITTLE_ENDIAN).getInt(0))
        .isEqualTo(0x7fc00001);
    assertThat(ci.getMaxValues().get(0).order(ByteOrder.LITTLE_ENDIAN).getInt(0))
        .isEqualTo(0x7fffffff);
  }

  @Test
  public void testDoubleIeee754AllNanPageKeepsNanRangeInColumnIndex() {
    double minNaN = Double.longBitsToDouble(0x7ff0000000000001L);
    double maxNaN = Double.longBitsToDouble(0x7fffffffffffffffL);
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(DOUBLE_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, maxNaN, minNaN));

    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();
    assertThat(ci.getMinValues()).hasSize(1);
    assertThat(ci.getMinValues().get(0).order(ByteOrder.LITTLE_ENDIAN).getLong(0))
        .isEqualTo(0x7ff0000000000001L);
    assertThat(ci.getMaxValues().get(0).order(ByteOrder.LITTLE_ENDIAN).getLong(0))
        .isEqualTo(0x7fffffffffffffffL);
  }

  @Test
  public void testFloat16Ieee754AllNanPageKeepsNanRangeInColumnIndex() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT16_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_NAN_LARGE, FLOAT16_NAN_SMALL));

    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();
    assertThat(ci.getMinValues()).hasSize(1);
    assertThat(ci.getMinValues().get(0).order(ByteOrder.LITTLE_ENDIAN).getShort(0))
        .isEqualTo(FLOAT16_NAN_SMALL.get2BytesLittleEndian());
    assertThat(ci.getMaxValues().get(0).order(ByteOrder.LITTLE_ENDIAN).getShort(0))
        .isEqualTo(FLOAT16_NAN_LARGE.get2BytesLittleEndian());
  }

  // Column index filtering for float

  @Test
  public void testNaNFloatZeroNaN() {
    // Pages: [1.0, 2.0], [3.0, 4.0]
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(floatStats(FLOAT_IEEE754_TYPE, 1.0f, 2.0f));
    builder.add(floatStats(FLOAT_IEEE754_TYPE, 3.0f, 4.0f));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();

    // Non-NaN literal (1.5 within page 0 range; ASCENDING boundary order)
    assertThat(toList(ci.visit(FilterApi.eq(FLOAT_COL, 1.5f)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT_COL, 1.5f)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT_COL, 1.5f)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.ltEq(FLOAT_COL, 1.5f)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT_COL, 1.5f)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gtEq(FLOAT_COL, 1.5f)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.in(FLOAT_COL, new HashSet<>(List.of(1.5f))))))
        .containsExactly(0);

    // NaN literal: nanCounts all zero, so eq returns empty and notEq returns all
    assertThat(toList(ci.visit(FilterApi.eq(FLOAT_COL, Float.NaN)))).isEmpty();
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT_COL, Float.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT_COL, Float.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.ltEq(FLOAT_COL, Float.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT_COL, Float.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gtEq(FLOAT_COL, Float.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.in(FLOAT_COL, new HashSet<>(List.of(Float.NaN))))))
        .isEmpty();
  }

  @Test
  public void testNaNFloatUnknownNaNCounts() {
    ColumnIndex ci = ColumnIndexBuilder.build(
        FLOAT_IEEE754_TYPE,
        BoundaryOrder.ASCENDING,
        List.of(false, false),
        List.of(0L, 0L),
        null,
        List.of(floatBuffer(1.0f), floatBuffer(3.0f)),
        List.of(floatBuffer(2.0f), floatBuffer(4.0f)),
        null,
        null);
    assertThat(ci).isNotNull();

    assertThat(toList(ci.visit(FilterApi.eq(FLOAT_COL, Float.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT_COL, 0.0f)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT_COL, 5.0f)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT_COL, 1.5f)))).containsExactly(0, 1);
  }

  @Test
  public void testNaNFloatMixed() {
    // Pages: [1.0, 2.0], [NaN, 2.5], [3.0, 4.0]
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(floatStats(FLOAT_IEEE754_TYPE, 1.0f, 2.0f));
    builder.add(floatStats(FLOAT_IEEE754_TYPE, Float.NaN, 2.5f));
    builder.add(floatStats(FLOAT_IEEE754_TYPE, 3.0f, 4.0f));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();

    assertThat(toList(ci.visit(FilterApi.eq(FLOAT_COL, 1.5f)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT_COL, 1.5f)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT_COL, 1.5f)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.ltEq(FLOAT_COL, 1.5f)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT_COL, 1.5f)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gtEq(FLOAT_COL, 1.5f)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.in(FLOAT_COL, new HashSet<>(List.of(1.5f))))))
        .containsExactly(0);

    assertThat(toList(ci.visit(FilterApi.eq(FLOAT_COL, Float.NaN)))).containsExactly(1);
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT_COL, Float.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT_COL, Float.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.ltEq(FLOAT_COL, Float.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT_COL, Float.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gtEq(FLOAT_COL, Float.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.in(FLOAT_COL, new HashSet<>(List.of(Float.NaN))))))
        .containsExactly(1);
  }

  // Column index filtering for double

  @Test
  public void testNaNDoubleZeroNaN() {
    // Pages: [1.0, 2.0], [3.0, 4.0]
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(DOUBLE_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, 1.0, 2.0));
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, 3.0, 4.0));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();

    assertThat(toList(ci.visit(FilterApi.eq(DOUBLE_COL, 1.5)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.notEq(DOUBLE_COL, 1.5)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.lt(DOUBLE_COL, 1.5)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.ltEq(DOUBLE_COL, 1.5)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.gt(DOUBLE_COL, 1.5)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gtEq(DOUBLE_COL, 1.5)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.in(DOUBLE_COL, new HashSet<>(List.of(1.5))))))
        .containsExactly(0);

    assertThat(toList(ci.visit(FilterApi.eq(DOUBLE_COL, Double.NaN)))).isEmpty();
    assertThat(toList(ci.visit(FilterApi.notEq(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.lt(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.ltEq(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gt(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gtEq(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.in(DOUBLE_COL, new HashSet<>(List.of(Double.NaN))))))
        .isEmpty();
  }

  @Test
  public void testNaNDoubleMixed() {
    // Pages: [1.0, 2.0], [NaN, 2.5], [3.0, 4.0]
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(DOUBLE_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, 1.0, 2.0));
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, Double.NaN, 2.5));
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, 3.0, 4.0));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();

    assertThat(toList(ci.visit(FilterApi.eq(DOUBLE_COL, 1.5)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.notEq(DOUBLE_COL, 1.5)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.lt(DOUBLE_COL, 1.5)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.ltEq(DOUBLE_COL, 1.5)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gt(DOUBLE_COL, 1.5)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gtEq(DOUBLE_COL, 1.5)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.in(DOUBLE_COL, new HashSet<>(List.of(1.5))))))
        .containsExactly(0);

    assertThat(toList(ci.visit(FilterApi.eq(DOUBLE_COL, Double.NaN)))).containsExactly(1);
    assertThat(toList(ci.visit(FilterApi.notEq(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.lt(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.ltEq(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gt(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gtEq(DOUBLE_COL, Double.NaN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.in(DOUBLE_COL, new HashSet<>(List.of(Double.NaN))))))
        .containsExactly(1);
  }

  // Column index filtering for float16

  @Test
  public void testNaNFloat16ZeroNaN() {
    // Pages: [1.0, 2.0], [3.0, 4.0]
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT16_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_ONE, FLOAT16_TWO));
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_THREE, FLOAT16_FOUR));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();

    assertThat(toList(ci.visit(FilterApi.eq(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT16_COL, FLOAT16_ONE)))).isEmpty();
    assertThat(toList(ci.visit(FilterApi.ltEq(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gtEq(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.in(FLOAT16_COL, new HashSet<>(List.of(FLOAT16_ONE))))))
        .containsExactly(0);

    assertThat(toList(ci.visit(FilterApi.eq(FLOAT16_COL, FLOAT16_NAN)))).isEmpty();
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.ltEq(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gtEq(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.in(FLOAT16_COL, new HashSet<>(List.of(FLOAT16_NAN))))))
        .isEmpty();
  }

  @Test
  public void testNaNFloat16Mixed() {
    // Pages: [1.0, 2.0], [NaN, 2.0], [3.0, 4.0]
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT16_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_ONE, FLOAT16_TWO));
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_NAN, FLOAT16_TWO));
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_THREE, FLOAT16_FOUR));
    ColumnIndex ci = builder.build();
    assertThat(ci).isNotNull();

    assertThat(toList(ci.visit(FilterApi.eq(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0);
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(1);
    assertThat(toList(ci.visit(FilterApi.ltEq(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0, 1);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gtEq(FLOAT16_COL, FLOAT16_ONE)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.in(FLOAT16_COL, new HashSet<>(List.of(FLOAT16_ONE))))))
        .containsExactly(0);

    assertThat(toList(ci.visit(FilterApi.eq(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(1);
    assertThat(toList(ci.visit(FilterApi.notEq(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.lt(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.ltEq(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gt(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.gtEq(FLOAT16_COL, FLOAT16_NAN)))).containsExactly(0, 1, 2);
    assertThat(toList(ci.visit(FilterApi.in(FLOAT16_COL, new HashSet<>(List.of(FLOAT16_NAN))))))
        .containsExactly(1);
  }
}
