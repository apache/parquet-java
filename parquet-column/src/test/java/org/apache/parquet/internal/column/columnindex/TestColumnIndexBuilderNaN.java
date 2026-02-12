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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

/**
 * Tests for ColumnIndex NaN handling under IEEE 754 total order and TYPE_DEFINED_ORDER.
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

  private static Binary float16Binary(short h) {
    return Binary.fromConstantByteArray(new byte[] {(byte) (h & 0xFF), (byte) ((h >> 8) & 0xFF)});
  }

  private static final Binary FLOAT16_NAN = float16Binary((short) 0x7e00);
  private static final Binary FLOAT16_ONE = float16Binary((short) 0x3C00);
  private static final Binary FLOAT16_TWO = float16Binary((short) 0x4000);

  private Statistics<?> floatStats(PrimitiveType type, Object... values) {
    Statistics<?> stats = Statistics.createStats(type);
    for (Object value : values) {
      if (value == null) {
        stats.incrementNumNulls();
      } else {
        stats.updateStats((float) value);
      }
    }
    return stats;
  }

  private Statistics<?> doubleStats(PrimitiveType type, Object... values) {
    Statistics<?> stats = Statistics.createStats(type);
    for (Object value : values) {
      if (value == null) {
        stats.incrementNumNulls();
      } else {
        stats.updateStats((double) value);
      }
    }
    return stats;
  }

  private Statistics<?> binaryStats(PrimitiveType type, Binary... values) {
    Statistics<?> stats = Statistics.createStats(type);
    for (Binary value : values) {
      if (value == null) {
        stats.incrementNumNulls();
      } else {
        stats.updateStats(value);
      }
    }
    return stats;
  }

  @Test
  public void testFloatTypeDefinedOrderNaNOnlyPage() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT_TYPE, Integer.MAX_VALUE);
    builder.add(floatStats(FLOAT_TYPE, 1.0f, 2.0f));
    builder.add(floatStats(FLOAT_TYPE, Float.NaN));
    builder.add(floatStats(FLOAT_TYPE, 3.0f, 4.0f));
    ColumnIndex ci = builder.build();
    assertNull(ci);
  }

  @Test
  public void testFloatIeee754NaNOnlyPage() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(floatStats(FLOAT_IEEE754_TYPE, 1.0f, 2.0f));
    builder.add(floatStats(FLOAT_IEEE754_TYPE, Float.NaN, Float.NaN));
    builder.add(floatStats(FLOAT_IEEE754_TYPE, 3.0f, Float.NaN, 4.0f));
    ColumnIndex ci = builder.build();
    assertNotNull(ci);
    assertEquals(3, ci.getNullPages().size());
    List<Long> nanCounts = ci.getNanCounts();
    assertNotNull(nanCounts);
    assertEquals(3, nanCounts.size());
    assertEquals(0L, (long) nanCounts.get(0));
    assertEquals(2L, (long) nanCounts.get(1));
    assertEquals(1L, (long) nanCounts.get(2));
  }

  @Test
  public void testDoubleTypeDefinedOrderNaNOnlyPage() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(DOUBLE_TYPE, Integer.MAX_VALUE);
    builder.add(doubleStats(DOUBLE_TYPE, 1.0, 2.0));
    builder.add(doubleStats(DOUBLE_TYPE, Double.NaN));
    builder.add(doubleStats(DOUBLE_TYPE, 3.0, 4.0));
    ColumnIndex ci = builder.build();
    assertNull(ci);
  }

  @Test
  public void testDoubleIeee754NaNOnlyPage() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(DOUBLE_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, 1.0, 2.0));
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, Double.NaN, Double.NaN, Double.NaN));
    builder.add(doubleStats(DOUBLE_IEEE754_TYPE, 3.0, Double.NaN, 4.0));
    ColumnIndex ci = builder.build();
    assertNotNull(ci);
    assertEquals(3, ci.getNullPages().size());
    List<Long> nanCounts = ci.getNanCounts();
    assertNotNull(nanCounts);
    assertEquals(3, nanCounts.size());
    assertEquals(0L, (long) nanCounts.get(0));
    assertEquals(3L, (long) nanCounts.get(1));
    assertEquals(1L, (long) nanCounts.get(2));
  }

  @Test
  public void testFloat16TypeDefinedOrderNaNOnlyPage() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT16_TYPE, Integer.MAX_VALUE);
    builder.add(binaryStats(FLOAT16_TYPE, FLOAT16_ONE, FLOAT16_TWO));
    builder.add(binaryStats(FLOAT16_TYPE, FLOAT16_NAN));
    builder.add(binaryStats(FLOAT16_TYPE, FLOAT16_ONE));
    ColumnIndex ci = builder.build();
    assertNull(ci);
  }

  @Test
  public void testFloat16Ieee754NaNOnlyPage() {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(FLOAT16_IEEE754_TYPE, Integer.MAX_VALUE);
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_ONE, FLOAT16_NAN, FLOAT16_TWO));
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_NAN, FLOAT16_NAN));
    builder.add(binaryStats(FLOAT16_IEEE754_TYPE, FLOAT16_ONE));
    ColumnIndex ci = builder.build();
    assertNotNull(ci);
    assertEquals(3, ci.getNullPages().size());
    List<Long> nanCounts = ci.getNanCounts();
    assertNotNull(nanCounts);
    assertEquals(3, nanCounts.size());
    assertEquals(1L, (long) nanCounts.get(0));
    assertEquals(2L, (long) nanCounts.get(1));
    assertEquals(0L, (long) nanCounts.get(2));
  }
}
