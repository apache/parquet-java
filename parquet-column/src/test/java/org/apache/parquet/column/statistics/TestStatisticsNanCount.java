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
package org.apache.parquet.column.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.Float16;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;

public class TestStatisticsNanCount {

  private static final PrimitiveType FLOAT_TYPE =
      Types.optional(PrimitiveTypeName.FLOAT).named("test_float");
  private static final PrimitiveType DOUBLE_TYPE =
      Types.optional(PrimitiveTypeName.DOUBLE).named("test_double");
  private static final PrimitiveType FLOAT16_TYPE = Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
      .length(2)
      .as(LogicalTypeAnnotation.float16Type())
      .named("test_float16");

  private static final PrimitiveType FLOAT_IEEE754_TYPE = Types.optional(PrimitiveTypeName.FLOAT)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("test_float_ieee754");
  private static final PrimitiveType DOUBLE_IEEE754_TYPE = Types.optional(PrimitiveTypeName.DOUBLE)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("test_double_ieee754");
  private static final PrimitiveType FLOAT16_IEEE754_TYPE = Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
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

  @Test
  public void testFloatNanCountMixedValues() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(1.0f);
    stats.updateStats(Float.NaN);
    stats.updateStats(2.0f);
    stats.updateStats(Float.NaN);
    stats.updateStats(3.0f);

    assertTrue(stats.isNanCountSet());
    assertEquals(2, stats.getNanCount());
    assertTrue(Float.isNaN(stats.getMax()) || Float.isNaN(stats.getMin()));
  }

  @Test
  public void testFloatNanCountAllNaN() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(Float.NaN);
    stats.updateStats(Float.NaN);

    assertTrue(stats.isNanCountSet());
    assertEquals(2, stats.getNanCount());
    assertTrue(stats.hasNonNullValue());
  }

  @Test
  public void testFloatNanCountNoNaN() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(1.0f);
    stats.updateStats(2.0f);

    assertTrue(stats.isNanCountSet());
    assertEquals(0, stats.getNanCount());
  }

  @Test
  public void testDoubleNanCountMixedValues() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_TYPE);
    stats.updateStats(1.0);
    stats.updateStats(Double.NaN);
    stats.updateStats(2.0);

    assertTrue(stats.isNanCountSet());
    assertEquals(1, stats.getNanCount());
    assertTrue(Double.isNaN(stats.getMax()) || Double.isNaN(stats.getMin()));
  }

  @Test
  public void testFloat16NanCountMixedValues() {
    BinaryStatistics stats = (BinaryStatistics) Statistics.createStats(FLOAT16_TYPE);
    stats.updateStats(FLOAT16_ONE);
    stats.updateStats(FLOAT16_NAN);
    stats.updateStats(FLOAT16_TWO);

    assertTrue(stats.isNanCountSet());
    assertEquals(1, stats.getNanCount());
    assertTrue(stats.hasNonNullValue());
    assertTrue(Float16.isNaN(stats.getMin().get2BytesLittleEndian())
        || Float16.isNaN(stats.getMax().get2BytesLittleEndian()));
  }

  @Test
  public void testFloat16NanCountNoNaN() {
    BinaryStatistics stats = (BinaryStatistics) Statistics.createStats(FLOAT16_TYPE);
    stats.updateStats(FLOAT16_ONE);

    assertTrue(stats.isNanCountSet());
    assertEquals(0, stats.getNanCount());
  }

  @Test
  public void testMergeNanCounts() {
    FloatStatistics stats1 = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats1.updateStats(1.0f);
    stats1.updateStats(Float.NaN);

    FloatStatistics stats2 = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats2.updateStats(2.0f);
    stats2.updateStats(Float.NaN);
    stats2.updateStats(Float.NaN);

    stats1.mergeStatistics(stats2);
    assertEquals(3, stats1.getNanCount());
  }

  @Test
  public void testCopyPreservesNanCount() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(1.0f);
    stats.updateStats(Float.NaN);
    stats.updateStats(Float.NaN);

    FloatStatistics copy = stats.copy();
    assertEquals(stats.getNanCount(), copy.getNanCount());
    assertTrue(copy.isNanCountSet());
    assertEquals(2, copy.getNanCount());
  }

  @Test
  public void testFloatBuilderIEEE754KeepsNanMinMax() {
    Statistics.Builder builder = Statistics.getBuilderForReading(FLOAT_IEEE754_TYPE);
    byte[] nanBytes = org.apache.parquet.bytes.BytesUtils.intToBytes(Float.floatToIntBits(Float.NaN));
    Statistics<?> stats = builder.withMin(nanBytes)
        .withMax(nanBytes)
        .withNanCount(10)
        .withNumNulls(0)
        .build();

    assertTrue(stats.hasNonNullValue());
    assertTrue(Float.isNaN(((FloatStatistics) stats).getMin()));
    assertTrue(Float.isNaN(((FloatStatistics) stats).getMax()));
    assertEquals(10, stats.getNanCount());
  }

  @Test
  public void testFloatBuilderTypeDefinedDropsNanMinMax() {
    PrimitiveType type = FLOAT_TYPE;
    Statistics.Builder builder = Statistics.getBuilderForReading(type);
    byte[] nanBytes = org.apache.parquet.bytes.BytesUtils.intToBytes(Float.floatToIntBits(Float.NaN));
    Statistics<?> stats =
        builder.withMin(nanBytes).withMax(nanBytes).withNumNulls(0).build();

    assertFalse(stats.hasNonNullValue());
    assertFalse(Float.isNaN(((FloatStatistics) stats).getMin()));
    assertFalse(Float.isNaN(((FloatStatistics) stats).getMax()));
  }

  @Test
  public void testDoubleBuilderIEEE754KeepsNanMinMax() {
    PrimitiveType type = DOUBLE_IEEE754_TYPE;
    Statistics.Builder builder = Statistics.getBuilderForReading(type);
    byte[] nanBytes = org.apache.parquet.bytes.BytesUtils.longToBytes(Double.doubleToLongBits(Double.NaN));
    Statistics<?> stats = builder.withMin(nanBytes)
        .withMax(nanBytes)
        .withNanCount(10)
        .withNumNulls(0)
        .build();

    assertTrue(stats.hasNonNullValue());
    assertTrue(Double.isNaN(((DoubleStatistics) stats).getMin()));
    assertTrue(Double.isNaN(((DoubleStatistics) stats).getMax()));
    assertEquals(10, stats.getNanCount());
  }

  @Test
  public void testFloatIEEE754NanOnlySetHasNonNullValue() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_IEEE754_TYPE);
    stats.updateStats(Float.NaN);
    stats.updateStats(Float.NaN);

    assertTrue(stats.hasNonNullValue());
    assertEquals(2, stats.getNanCount());
    assertTrue(Float.isNaN(stats.getMin()));
    assertTrue(Float.isNaN(stats.getMax()));
  }

  @Test
  public void testDoubleIEEE754NanOnlySetHasNonNullValue() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_IEEE754_TYPE);
    stats.updateStats(Double.NaN);

    assertTrue(stats.hasNonNullValue());
    assertEquals(1, stats.getNanCount());
    assertTrue(Double.isNaN(stats.getMin()));
    assertTrue(Double.isNaN(stats.getMax()));
  }

  @Test
  public void testFloat16IEEE754NanOnlySetHasNonNullValue() {
    BinaryStatistics stats = (BinaryStatistics) Statistics.createStats(FLOAT16_IEEE754_TYPE);
    stats.updateStats(FLOAT16_NAN);

    assertTrue(stats.hasNonNullValue());
    assertEquals(1, stats.getNanCount());
    assertTrue(Float16.isNaN(stats.getMin().get2BytesLittleEndian()));
    assertTrue(Float16.isNaN(stats.getMax().get2BytesLittleEndian()));
  }

  @Test
  public void testFloatIEEE754NanExcludedFromMax() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_IEEE754_TYPE);
    stats.updateStats(1.0f);
    stats.updateStats(Float.NaN);
    stats.updateStats(2.0f);

    // NaN is excluded from min/max on the write path for all column orders
    assertEquals(2.0f, stats.getMax(), 0.0f);
    assertEquals(1.0f, stats.getMin(), 0.0f);
    assertEquals(1, stats.getNanCount());
  }

  @Test
  public void testDoubleIEEE754NanExcludedFromMax() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_IEEE754_TYPE);
    stats.updateStats(1.0);
    stats.updateStats(Double.NaN);
    stats.updateStats(2.0);

    // NaN is excluded from min/max on the write path for all column orders
    assertEquals(2.0, stats.getMax(), 0.0);
    assertEquals(1.0, stats.getMin(), 0.0);
    assertEquals(1, stats.getNanCount());
  }

  @Test
  public void testFloatTypeDefinedNanOnlySetHasNonNullValue() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(Float.NaN);
    stats.updateStats(Float.NaN);

    assertTrue(stats.hasNonNullValue());
    assertEquals(2, stats.getNanCount());
  }

  @Test
  public void testDoubleTypeDefinedNanOnlySetHasNonNullValue() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_TYPE);
    stats.updateStats(Double.NaN);

    assertTrue(stats.hasNonNullValue());
    assertEquals(1, stats.getNanCount());
  }

  @Test
  public void testFloat16TypeDefinedNanOnlySetHasNonNullValue() {
    BinaryStatistics stats = (BinaryStatistics) Statistics.createStats(FLOAT16_TYPE);
    stats.updateStats(FLOAT16_NAN);

    assertTrue(stats.hasNonNullValue());
    assertEquals(1, stats.getNanCount());
  }
}
