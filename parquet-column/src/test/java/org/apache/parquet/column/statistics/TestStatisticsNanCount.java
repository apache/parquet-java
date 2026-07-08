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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
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
  private static final Binary FLOAT16_NAN_SMALL = float16Binary((short) 0x7c01);
  private static final Binary FLOAT16_NAN_LARGE = float16Binary((short) 0x7fff);
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

    assertThat(stats.isNanCountSet()).isTrue();
    assertThat(stats.getNanCount()).isEqualTo(2);
    assertThat(stats.getMin()).isCloseTo(1.0f, offset(0.0f));
    assertThat(stats.getMax()).isCloseTo(3.0f, offset(0.0f));
  }

  @Test
  public void testFloatNanCountAllNaN() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(Float.NaN);
    stats.updateStats(Float.NaN);

    assertThat(stats.isNanCountSet()).isTrue();
    assertThat(stats.getNanCount()).isEqualTo(2);
    assertThat(stats.hasNonNullValue()).isFalse();
  }

  @Test
  public void testFloatNanCountNoNaN() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(1.0f);
    stats.updateStats(2.0f);

    assertThat(stats.isNanCountSet()).isTrue();
    assertThat(stats.getNanCount()).isEqualTo(0);
  }

  @Test
  public void testDoubleNanCountMixedValues() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_TYPE);
    stats.updateStats(1.0);
    stats.updateStats(Double.NaN);
    stats.updateStats(2.0);

    assertThat(stats.isNanCountSet()).isTrue();
    assertThat(stats.getNanCount()).isEqualTo(1);
    assertThat(stats.getMin()).isCloseTo(1.0, offset(0.0));
    assertThat(stats.getMax()).isCloseTo(2.0, offset(0.0));
  }

  @Test
  public void testFloat16NanCountMixedValues() {
    Float16Statistics stats = (Float16Statistics) Statistics.createStats(FLOAT16_TYPE);
    stats.updateStats(FLOAT16_ONE);
    stats.updateStats(FLOAT16_NAN);
    stats.updateStats(FLOAT16_TWO);

    assertThat(stats.isNanCountSet()).isTrue();
    assertThat(stats.getNanCount()).isEqualTo(1);
    assertThat(stats.hasNonNullValue()).isTrue();
    assertThat(stats.genericGetMin()).isEqualTo(FLOAT16_ONE);
    assertThat(stats.genericGetMax()).isEqualTo(FLOAT16_TWO);
  }

  @Test
  public void testFloat16NanCountNoNaN() {
    Float16Statistics stats = (Float16Statistics) Statistics.createStats(FLOAT16_TYPE);
    stats.updateStats(FLOAT16_ONE);

    assertThat(stats.isNanCountSet()).isTrue();
    assertThat(stats.getNanCount()).isEqualTo(0);
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
    assertThat(stats1.getNanCount()).isEqualTo(3);
  }

  @Test
  public void testCopyPreservesNanCount() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(1.0f);
    stats.updateStats(Float.NaN);
    stats.updateStats(Float.NaN);

    FloatStatistics copy = stats.copy();
    assertThat(copy.getNanCount()).isEqualTo(stats.getNanCount());
    assertThat(copy.isNanCountSet()).isTrue();
    assertThat(copy.getNanCount()).isEqualTo(2);
  }

  @Test
  public void testFloatBuilderIEEE754KeepsNanMinMax() {
    Statistics.Builder builder = Statistics.getBuilderForReading(FLOAT_IEEE754_TYPE);
    byte[] nanBytes = BytesUtils.intToBytes(Float.floatToIntBits(Float.NaN));
    Statistics<?> stats = builder.withMin(nanBytes)
        .withMax(nanBytes)
        .withNanCount(10)
        .withNumNulls(0)
        .build();

    assertThat(stats.hasNonNullValue()).isTrue();
    assertThat(((FloatStatistics) stats).getMin()).isNaN();
    assertThat(((FloatStatistics) stats).getMax()).isNaN();
    assertThat(stats.getNanCount()).isEqualTo(10);
  }

  @Test
  public void testFloatBuilderTypeDefinedDropsNanMinMax() {
    Statistics.Builder builder = Statistics.getBuilderForReading(FLOAT_TYPE);
    byte[] nanBytes = BytesUtils.intToBytes(Float.floatToIntBits(Float.NaN));
    Statistics<?> stats =
        builder.withMin(nanBytes).withMax(nanBytes).withNumNulls(0).build();

    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(((FloatStatistics) stats).getMin()).isNotNaN();
    assertThat(((FloatStatistics) stats).getMax()).isNotNaN();
  }

  @Test
  public void testDoubleBuilderIEEE754KeepsNanMinMax() {
    Statistics.Builder builder = Statistics.getBuilderForReading(DOUBLE_IEEE754_TYPE);
    byte[] nanBytes = BytesUtils.longToBytes(Double.doubleToLongBits(Double.NaN));
    Statistics<?> stats = builder.withMin(nanBytes)
        .withMax(nanBytes)
        .withNanCount(10)
        .withNumNulls(0)
        .build();

    assertThat(stats.hasNonNullValue()).isTrue();
    assertThat(((DoubleStatistics) stats).getMin()).isNaN();
    assertThat(((DoubleStatistics) stats).getMax()).isNaN();
    assertThat(stats.getNanCount()).isEqualTo(10);
  }

  @Test
  public void testFloatIEEE754NanOnlySetsHasNonNullValue() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_IEEE754_TYPE);
    stats.updateStats(Float.NaN);
    stats.updateStats(Float.NaN);

    assertThat(stats.hasNonNullValue()).isTrue();
    assertThat(stats.getMin()).isNaN();
    assertThat(stats.getMax()).isNaN();
    assertThat(stats.getNanCount()).isEqualTo(2);
  }

  @Test
  public void testDoubleIEEE754NanOnlySetsHasNonNullValue() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_IEEE754_TYPE);
    stats.updateStats(Double.NaN);

    assertThat(stats.hasNonNullValue()).isTrue();
    assertThat(stats.getMin()).isNaN();
    assertThat(stats.getMax()).isNaN();
    assertThat(stats.getNanCount()).isEqualTo(1);
  }

  @Test
  public void testFloat16IEEE754NanOnlySetsHasNonNullValue() {
    IEEE754Float16Statistics stats = (IEEE754Float16Statistics) Statistics.createStats(FLOAT16_IEEE754_TYPE);
    stats.updateStats(FLOAT16_NAN);

    assertThat(stats.hasNonNullValue()).isTrue();
    assertThat(stats.genericGetMin()).isEqualTo(FLOAT16_NAN);
    assertThat(stats.genericGetMax()).isEqualTo(FLOAT16_NAN);
    assertThat(stats.getNanCount()).isEqualTo(1);
  }

  @Test
  public void testFloatIEEE754AllNaNTracksNaNRange() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_IEEE754_TYPE);
    float minNaN = Float.intBitsToFloat(0x7fc00001);
    float maxNaN = Float.intBitsToFloat(0x7fffffff);
    stats.updateStats(maxNaN);
    stats.updateStats(minNaN);

    assertThat(stats.getNanCount()).isEqualTo(2);
    assertThat(Float.floatToRawIntBits(stats.getMin())).isEqualTo(0x7fc00001);
    assertThat(Float.floatToRawIntBits(stats.getMax())).isEqualTo(0x7fffffff);
  }

  @Test
  public void testDoubleIEEE754AllNaNTracksNaNRange() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_IEEE754_TYPE);
    double minNaN = Double.longBitsToDouble(0x7ff0000000000001L);
    double maxNaN = Double.longBitsToDouble(0x7fffffffffffffffL);
    stats.updateStats(maxNaN);
    stats.updateStats(minNaN);

    assertThat(stats.getNanCount()).isEqualTo(2);
    assertThat(Double.doubleToRawLongBits(stats.getMin())).isEqualTo(0x7ff0000000000001L);
    assertThat(Double.doubleToRawLongBits(stats.getMax())).isEqualTo(0x7fffffffffffffffL);
  }

  @Test
  public void testFloat16IEEE754AllNaNTracksNaNRange() {
    IEEE754Float16Statistics stats = (IEEE754Float16Statistics) Statistics.createStats(FLOAT16_IEEE754_TYPE);
    stats.updateStats(FLOAT16_NAN_LARGE);
    stats.updateStats(FLOAT16_NAN_SMALL);

    assertThat(stats.getNanCount()).isEqualTo(2);
    assertThat(stats.genericGetMin().get2BytesLittleEndian()).isEqualTo(FLOAT16_NAN_SMALL.get2BytesLittleEndian());
    assertThat(stats.genericGetMax().get2BytesLittleEndian()).isEqualTo(FLOAT16_NAN_LARGE.get2BytesLittleEndian());
  }

  @Test
  public void testFloatIEEE754NonNaNReplacesAllNaNRange() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_IEEE754_TYPE);
    stats.updateStats(Float.intBitsToFloat(0x7fffffff));
    stats.updateStats(Float.intBitsToFloat(0x7fc00001));
    stats.updateStats(1.0f);
    stats.updateStats(2.0f);

    assertThat(stats.getNanCount()).isEqualTo(2);
    assertThat(stats.getMin()).isCloseTo(1.0f, offset(0.0f));
    assertThat(stats.getMax()).isCloseTo(2.0f, offset(0.0f));
  }

  @Test
  public void testDoubleIEEE754NonNaNReplacesAllNaNRange() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_IEEE754_TYPE);
    stats.updateStats(Double.longBitsToDouble(0x7fffffffffffffffL));
    stats.updateStats(Double.longBitsToDouble(0x7ff0000000000001L));
    stats.updateStats(1.0);
    stats.updateStats(2.0);

    assertThat(stats.getNanCount()).isEqualTo(2);
    assertThat(stats.getMin()).isCloseTo(1.0, offset(0.0));
    assertThat(stats.getMax()).isCloseTo(2.0, offset(0.0));
  }

  @Test
  public void testFloat16IEEE754NonNaNReplacesAllNaNRange() {
    IEEE754Float16Statistics stats = (IEEE754Float16Statistics) Statistics.createStats(FLOAT16_IEEE754_TYPE);
    stats.updateStats(FLOAT16_NAN_LARGE);
    stats.updateStats(FLOAT16_NAN_SMALL);
    stats.updateStats(FLOAT16_ONE);
    stats.updateStats(FLOAT16_TWO);

    assertThat(stats.getNanCount()).isEqualTo(2);
    assertThat(stats.genericGetMin()).isEqualTo(FLOAT16_ONE);
    assertThat(stats.genericGetMax()).isEqualTo(FLOAT16_TWO);
  }

  @Test
  public void testFloatIEEE754NanExcludedFromMax() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_IEEE754_TYPE);
    stats.updateStats(1.0f);
    stats.updateStats(Float.NaN);
    stats.updateStats(2.0f);

    assertThat(stats.getMax()).isCloseTo(2.0f, offset(0.0f));
    assertThat(stats.getMin()).isCloseTo(1.0f, offset(0.0f));
    assertThat(stats.getNanCount()).isEqualTo(1);
  }

  @Test
  public void testDoubleIEEE754NanExcludedFromMax() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_IEEE754_TYPE);
    stats.updateStats(1.0);
    stats.updateStats(Double.NaN);
    stats.updateStats(2.0);

    assertThat(stats.getMax()).isCloseTo(2.0, offset(0.0));
    assertThat(stats.getMin()).isCloseTo(1.0, offset(0.0));
    assertThat(stats.getNanCount()).isEqualTo(1);
  }

  @Test
  public void testFloatTypeDefinedNanOnlyDoesNotSetHasNonNullValue() {
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(FLOAT_TYPE);
    stats.updateStats(Float.NaN);
    stats.updateStats(Float.NaN);

    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(stats.getNanCount()).isEqualTo(2);
  }

  @Test
  public void testDoubleTypeDefinedNanOnlyDoesNotSetHasNonNullValue() {
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(DOUBLE_TYPE);
    stats.updateStats(Double.NaN);

    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(stats.getNanCount()).isEqualTo(1);
  }

  @Test
  public void testFloat16TypeDefinedNanOnlyDoesNotSetHasNonNullValue() {
    Float16Statistics stats = (Float16Statistics) Statistics.createStats(FLOAT16_TYPE);
    stats.updateStats(FLOAT16_NAN);

    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(stats.getNanCount()).isEqualTo(1);
  }
}
