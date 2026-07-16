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

import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static org.apache.parquet.bytes.BytesUtils.intToBytes;
import static org.apache.parquet.bytes.BytesUtils.longToBytes;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

import java.nio.ByteBuffer;
import java.util.Locale;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestStatistics {
  private int[] integerArray;
  private long[] longArray;
  private float[] floatArray;
  private double[] doubleArray;
  private String[] stringArray;
  private boolean[] booleanArray;

  @Test
  public void testNumNulls() {
    IntStatistics stats = new IntStatistics();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(0);

    stats.incrementNumNulls();
    stats.incrementNumNulls();
    stats.incrementNumNulls();
    stats.incrementNumNulls();
    assertThat(stats.getNumNulls()).isEqualTo(4);

    stats.incrementNumNulls(5);
    assertThat(stats.getNumNulls()).isEqualTo(9);

    stats.setNumNulls(22);
    assertThat(stats.getNumNulls()).isEqualTo(22);
  }

  @Test
  public void testIntMinMax() {
    // Test basic max/min
    integerArray = new int[] {1, 3, 14, 54, 66, 8, 0, 23, 54};
    IntStatistics stats = new IntStatistics();

    for (int i : integerArray) {
      stats.updateStats(i);
    }
    assertThat(stats.getMax()).isEqualTo(66);
    assertThat(stats.getMin()).isEqualTo(0);

    // Test negative values
    integerArray = new int[] {-11, 3, -14, 54, -66, 8, 0, -23, 54};
    IntStatistics statsNeg = new IntStatistics();

    for (int i : integerArray) {
      statsNeg.updateStats(i);
    }
    assertThat(statsNeg.getMax()).isEqualTo(54);
    assertThat(statsNeg.getMin()).isEqualTo(-66);

    assertThat(statsNeg.compareMaxToValue(55)).isNegative();
    assertThat(statsNeg.compareMaxToValue(54)).isZero();
    assertThat(statsNeg.compareMaxToValue(5)).isPositive();
    assertThat(statsNeg.compareMinToValue(0)).isNegative();
    assertThat(statsNeg.compareMinToValue(-66)).isZero();
    assertThat(statsNeg.compareMinToValue(-67)).isPositive();

    // Test converting to and from byte[]
    byte[] intMaxBytes = statsNeg.getMaxBytes();
    byte[] intMinBytes = statsNeg.getMinBytes();

    assertThat(ByteBuffer.wrap(intMaxBytes)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getInt())
        .isEqualTo(54);
    assertThat(ByteBuffer.wrap(intMinBytes)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getInt())
        .isEqualTo(-66);

    IntStatistics statsFromBytes = new IntStatistics();
    statsFromBytes.setMinMaxFromBytes(intMinBytes, intMaxBytes);

    assertThat(statsFromBytes.getMax()).isEqualTo(54);
    assertThat(statsFromBytes.getMin()).isEqualTo(-66);

    integerArray = new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE};
    IntStatistics minMaxValues = new IntStatistics();

    for (int i : integerArray) {
      minMaxValues.updateStats(i);
    }
    assertThat(minMaxValues.getMax()).isEqualTo(Integer.MAX_VALUE);
    assertThat(minMaxValues.getMin()).isEqualTo(Integer.MIN_VALUE);

    // Test converting to and from byte[] for large and small values
    byte[] intMaxBytesMinMax = minMaxValues.getMaxBytes();
    byte[] intMinBytesMinMax = minMaxValues.getMinBytes();

    assertThat(ByteBuffer.wrap(intMaxBytesMinMax)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getInt())
        .isEqualTo(Integer.MAX_VALUE);
    assertThat(ByteBuffer.wrap(intMinBytesMinMax)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getInt())
        .isEqualTo(Integer.MIN_VALUE);

    IntStatistics statsFromBytesMinMax = new IntStatistics();
    statsFromBytesMinMax.setMinMaxFromBytes(intMinBytesMinMax, intMaxBytesMinMax);

    assertThat(statsFromBytesMinMax.getMax()).isEqualTo(Integer.MAX_VALUE);
    assertThat(statsFromBytesMinMax.getMin()).isEqualTo(Integer.MIN_VALUE);

    // Test print formatting
    assertThat(stats).asString().isEqualTo("min: 0, max: 66, num_nulls: 0");
  }

  @Test
  public void testLongMinMax() {
    // Test basic max/min
    longArray = new long[] {9, 39, 99, 3, 0, 12, 1000, 65, 542};
    LongStatistics stats = new LongStatistics();

    for (long l : longArray) {
      stats.updateStats(l);
    }
    assertThat(stats.getMax()).isEqualTo(1000);
    assertThat(stats.getMin()).isEqualTo(0);

    // Test negative values
    longArray = new long[] {-101, 993, -9914, 54, -9, 89, 0, -23, 90};
    LongStatistics statsNeg = new LongStatistics();

    for (long l : longArray) {
      statsNeg.updateStats(l);
    }
    assertThat(statsNeg.getMax()).isEqualTo(993);
    assertThat(statsNeg.getMin()).isEqualTo(-9914);

    assertThat(statsNeg.compareMaxToValue(994)).isNegative();
    assertThat(statsNeg.compareMaxToValue(993)).isZero();
    assertThat(statsNeg.compareMaxToValue(-1000)).isPositive();
    assertThat(statsNeg.compareMinToValue(10000)).isNegative();
    assertThat(statsNeg.compareMinToValue(-9914)).isZero();
    assertThat(statsNeg.compareMinToValue(-9915)).isPositive();

    // Test converting to and from byte[]
    byte[] longMaxBytes = statsNeg.getMaxBytes();
    byte[] longMinBytes = statsNeg.getMinBytes();

    assertThat(ByteBuffer.wrap(longMaxBytes)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getLong())
        .isEqualTo(993);
    assertThat(ByteBuffer.wrap(longMinBytes)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getLong())
        .isEqualTo(-9914);

    LongStatistics statsFromBytes = new LongStatistics();
    statsFromBytes.setMinMaxFromBytes(longMinBytes, longMaxBytes);

    assertThat(statsFromBytes.getMax()).isEqualTo(993);
    assertThat(statsFromBytes.getMin()).isEqualTo(-9914);

    longArray = new long[] {Long.MAX_VALUE, Long.MIN_VALUE};
    LongStatistics minMaxValues = new LongStatistics();

    for (long l : longArray) {
      minMaxValues.updateStats(l);
    }
    assertThat(minMaxValues.getMax()).isEqualTo(Long.MAX_VALUE);
    assertThat(minMaxValues.getMin()).isEqualTo(Long.MIN_VALUE);

    // Test converting to and from byte[] for large and small values
    byte[] longMaxBytesMinMax = minMaxValues.getMaxBytes();
    byte[] longMinBytesMinMax = minMaxValues.getMinBytes();

    assertThat(ByteBuffer.wrap(longMaxBytesMinMax)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getLong())
        .isEqualTo(Long.MAX_VALUE);
    assertThat(ByteBuffer.wrap(longMinBytesMinMax)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getLong())
        .isEqualTo(Long.MIN_VALUE);

    LongStatistics statsFromBytesMinMax = new LongStatistics();
    statsFromBytesMinMax.setMinMaxFromBytes(longMinBytesMinMax, longMaxBytesMinMax);

    assertThat(statsFromBytesMinMax.getMax()).isEqualTo(Long.MAX_VALUE);
    assertThat(statsFromBytesMinMax.getMin()).isEqualTo(Long.MIN_VALUE);

    // Test print formatting
    assertThat(stats).asString().isEqualTo("min: 0, max: 1000, num_nulls: 0");
  }

  @Test
  public void testFloatMinMax() {
    // Test basic max/min
    floatArray = new float[] {1.5f, 44.5f, 412.99f, 0.65f, 5.6f, 100.6f, 0.0001f, 23.0f, 553.6f};
    FloatStatistics stats = new FloatStatistics();

    for (float f : floatArray) {
      stats.updateStats(f);
    }
    assertThat(stats.getMax()).isCloseTo(553.6f, offset(1e-10f));
    assertThat(stats.getMin()).isCloseTo(0.0001f, offset(1e-10f));

    // Test negative values
    floatArray = new float[] {-1.5f, -44.5f, -412.99f, 0.65f, -5.6f, -100.6f, 0.0001f, -23.0f, -3.6f};
    FloatStatistics statsNeg = new FloatStatistics();

    for (float f : floatArray) {
      statsNeg.updateStats(f);
    }
    assertThat(statsNeg.getMax()).isCloseTo(0.65f, offset(1e-10f));
    assertThat(statsNeg.getMin()).isCloseTo(-412.99f, offset(1e-10f));

    assertThat(statsNeg.compareMaxToValue(1)).isNegative();
    assertThat(statsNeg.compareMaxToValue(0.65F)).isZero();
    assertThat(statsNeg.compareMaxToValue(0.649F)).isPositive();
    assertThat(statsNeg.compareMinToValue(-412.98F)).isNegative();
    assertThat(statsNeg.compareMinToValue(-412.99F)).isZero();
    assertThat(statsNeg.compareMinToValue(-450)).isPositive();

    // Test converting to and from byte[]
    byte[] floatMaxBytes = statsNeg.getMaxBytes();
    byte[] floatMinBytes = statsNeg.getMinBytes();

    assertThat(ByteBuffer.wrap(floatMaxBytes)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getFloat())
        .isCloseTo(0.65f, offset(1e-10f));
    assertThat(ByteBuffer.wrap(floatMinBytes)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getFloat())
        .isCloseTo(-412.99f, offset(1e-10f));

    FloatStatistics statsFromBytes = new FloatStatistics();
    statsFromBytes.setMinMaxFromBytes(floatMinBytes, floatMaxBytes);

    assertThat(statsFromBytes.getMax()).isCloseTo(0.65f, offset(1e-10f));
    assertThat(statsFromBytes.getMin()).isCloseTo(-412.99f, offset(1e-10f));

    floatArray = new float[] {Float.MAX_VALUE, Float.MIN_VALUE};
    FloatStatistics minMaxValues = new FloatStatistics();

    for (float f : floatArray) {
      minMaxValues.updateStats(f);
    }
    assertThat(minMaxValues.getMax()).isCloseTo(Float.MAX_VALUE, offset(1e-10f));
    assertThat(minMaxValues.getMin()).isCloseTo(Float.MIN_VALUE, offset(1e-10f));

    // Test converting to and from byte[] for large and small values
    byte[] floatMaxBytesMinMax = minMaxValues.getMaxBytes();
    byte[] floatMinBytesMinMax = minMaxValues.getMinBytes();

    assertThat(ByteBuffer.wrap(floatMaxBytesMinMax)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getFloat())
        .isCloseTo(Float.MAX_VALUE, offset(1e-10f));
    assertThat(ByteBuffer.wrap(floatMinBytesMinMax)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getFloat())
        .isCloseTo(Float.MIN_VALUE, offset(1e-10f));

    FloatStatistics statsFromBytesMinMax = new FloatStatistics();
    statsFromBytesMinMax.setMinMaxFromBytes(floatMinBytesMinMax, floatMaxBytesMinMax);

    assertThat(statsFromBytesMinMax.getMax()).isCloseTo(Float.MAX_VALUE, offset(1e-10f));
    assertThat(statsFromBytesMinMax.getMin()).isCloseTo(Float.MIN_VALUE, offset(1e-10f));

    // Test print formatting
    assertThat(stats).asString().isEqualTo("min: 1.0E-4, max: 553.6, num_nulls: 0");
  }

  @Test
  public void testDoubleMinMax() {
    // Test basic max/min
    doubleArray = new double[] {81.5d, 944.5f, 2.002d, 334.5d, 5.6d, 0.001d, 0.00001d, 23.0d, 553.6d};
    DoubleStatistics stats = new DoubleStatistics();

    for (double d : doubleArray) {
      stats.updateStats(d);
    }
    assertThat(stats.getMax()).isCloseTo(944.5d, offset(1e-10));
    assertThat(stats.getMin()).isCloseTo(0.00001d, offset(1e-10));

    // Test negative values
    doubleArray = new double[] {-81.5d, -944.5d, 2.002d, -334.5d, -5.6d, -0.001d, -0.00001d, 23.0d, -3.6d};
    DoubleStatistics statsNeg = new DoubleStatistics();

    for (double d : doubleArray) {
      statsNeg.updateStats(d);
    }
    assertThat(statsNeg.getMax()).isCloseTo(23.0d, offset(1e-10));
    assertThat(statsNeg.getMin()).isCloseTo(-944.5d, offset(1e-10));

    assertThat(statsNeg.compareMaxToValue(23.0001D)).isNegative();
    assertThat(statsNeg.compareMaxToValue(23D)).isZero();
    assertThat(statsNeg.compareMaxToValue(0D)).isPositive();
    assertThat(statsNeg.compareMinToValue(-400D)).isNegative();
    assertThat(statsNeg.compareMinToValue(-944.5D)).isZero();
    assertThat(statsNeg.compareMinToValue(-944.500001D)).isPositive();

    // Test converting to and from byte[]
    byte[] doubleMaxBytes = statsNeg.getMaxBytes();
    byte[] doubleMinBytes = statsNeg.getMinBytes();

    assertThat(ByteBuffer.wrap(doubleMaxBytes)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getDouble())
        .isCloseTo(23.0d, offset(1e-10));
    assertThat(ByteBuffer.wrap(doubleMinBytes)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getDouble())
        .isCloseTo(-944.5d, offset(1e-10));

    DoubleStatistics statsFromBytes = new DoubleStatistics();
    statsFromBytes.setMinMaxFromBytes(doubleMinBytes, doubleMaxBytes);

    assertThat(statsFromBytes.getMax()).isCloseTo(23.0d, offset(1e-10));
    assertThat(statsFromBytes.getMin()).isCloseTo(-944.5d, offset(1e-10));

    doubleArray = new double[] {Double.MAX_VALUE, Double.MIN_VALUE};
    DoubleStatistics minMaxValues = new DoubleStatistics();

    for (double d : doubleArray) {
      minMaxValues.updateStats(d);
    }
    assertThat(minMaxValues.getMax()).isCloseTo(Double.MAX_VALUE, offset(1e-10));
    assertThat(minMaxValues.getMin()).isCloseTo(Double.MIN_VALUE, offset(1e-10));

    // Test converting to and from byte[] for large and small values
    byte[] doubleMaxBytesMinMax = minMaxValues.getMaxBytes();
    byte[] doubleMinBytesMinMax = minMaxValues.getMinBytes();

    assertThat(ByteBuffer.wrap(doubleMaxBytesMinMax)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getDouble())
        .isCloseTo(Double.MAX_VALUE, offset(1e-10));
    assertThat(ByteBuffer.wrap(doubleMinBytesMinMax)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getDouble())
        .isCloseTo(Double.MIN_VALUE, offset(1e-10));

    DoubleStatistics statsFromBytesMinMax = new DoubleStatistics();
    statsFromBytesMinMax.setMinMaxFromBytes(doubleMinBytesMinMax, doubleMaxBytesMinMax);

    assertThat(statsFromBytesMinMax.getMax()).isCloseTo(Double.MAX_VALUE, offset(1e-10));
    assertThat(statsFromBytesMinMax.getMin()).isCloseTo(Double.MIN_VALUE, offset(1e-10));

    // Test print formatting
    assertThat(stats).asString().isEqualTo("min: 1.0E-5, max: 944.5, num_nulls: 0");
  }

  @Test
  public void testFloatingPointStringIndependentFromLocale() {
    Statistics<?> floatStats =
        Statistics.createStats(Types.optional(PrimitiveTypeName.FLOAT).named("test-float"));
    floatStats.updateStats(123.456f);
    Statistics<?> doubleStats =
        Statistics.createStats(Types.optional(PrimitiveTypeName.DOUBLE).named("test-double"));
    doubleStats.updateStats(12345.6789);

    Locale defaultLocale = Locale.getDefault();
    try {
      // Set the locale to French where the decimal separator would be ',' instead of '.'
      Locale.setDefault(Locale.FRENCH);
      assertThat(floatStats).asString().isEqualTo("min: 123.456, max: 123.456, num_nulls: 0");
      assertThat(doubleStats).asString().isEqualTo("min: 12345.6789, max: 12345.6789, num_nulls: 0");
    } finally {
      Locale.setDefault(defaultLocale);
    }
  }

  @Test
  public void testBooleanMinMax() {
    // Test all true
    booleanArray = new boolean[] {true, true, true};
    BooleanStatistics statsTrue = new BooleanStatistics();

    for (boolean i : booleanArray) {
      statsTrue.updateStats(i);
    }
    assertThat(statsTrue.getMax()).isTrue();
    assertThat(statsTrue.getMin()).isTrue();

    // Test all false
    booleanArray = new boolean[] {false, false, false};
    BooleanStatistics statsFalse = new BooleanStatistics();

    for (boolean i : booleanArray) {
      statsFalse.updateStats(i);
    }
    assertThat(statsFalse.getMax()).isFalse();
    assertThat(statsFalse.getMin()).isFalse();

    booleanArray = new boolean[] {false, true, false};
    BooleanStatistics statsBoth = new BooleanStatistics();

    for (boolean i : booleanArray) {
      statsBoth.updateStats(i);
    }
    assertThat(statsBoth.getMax()).isTrue();
    assertThat(statsBoth.getMin()).isFalse();

    // Test converting to and from byte[]
    byte[] boolMaxBytes = statsBoth.getMaxBytes();
    byte[] boolMinBytes = statsBoth.getMinBytes();

    assertThat(boolMaxBytes[0] & 255).isEqualTo(1);
    assertThat(boolMinBytes[0] & 255).isEqualTo(0);

    BooleanStatistics statsFromBytes = new BooleanStatistics();
    statsFromBytes.setMinMaxFromBytes(boolMinBytes, boolMaxBytes);

    assertThat(statsFromBytes.getMax()).isTrue();
    assertThat(statsFromBytes.getMin()).isFalse();

    // Test print formatting
    assertThat(statsBoth).asString().isEqualTo("min: false, max: true, num_nulls: 0");
  }

  @Test
  public void testBinaryMinMax() {
    // Test basic max/min
    stringArray = new String[] {"hello", "world", "this", "is", "a", "test", "of", "the", "stats", "class"};
    PrimitiveType type =
        Types.optional(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test_binary_utf8");
    BinaryStatistics stats = (BinaryStatistics) Statistics.createStats(type);

    for (String s : stringArray) {
      stats.updateStats(Binary.fromString(s));
    }
    assertThat(stats.genericGetMax()).isEqualTo(Binary.fromString("world"));
    assertThat(stats.genericGetMin()).isEqualTo(Binary.fromString("a"));

    // Test empty string
    stringArray = new String[] {"", "", "", "", ""};
    BinaryStatistics statsEmpty = (BinaryStatistics) Statistics.createStats(type);

    for (String s : stringArray) {
      statsEmpty.updateStats(Binary.fromString(s));
    }
    assertThat(statsEmpty.genericGetMax()).isEqualTo(Binary.fromString(""));
    assertThat(statsEmpty.genericGetMin()).isEqualTo(Binary.fromString(""));

    // Test converting to and from byte[]
    byte[] stringMaxBytes = stats.getMaxBytes();
    byte[] stringMinBytes = stats.getMinBytes();

    assertThat(new String(stringMaxBytes)).isEqualTo("world");
    assertThat(new String(stringMinBytes)).isEqualTo("a");

    BinaryStatistics statsFromBytes = (BinaryStatistics) Statistics.createStats(type);
    statsFromBytes.setMinMaxFromBytes(stringMinBytes, stringMaxBytes);

    assertThat(statsFromBytes.genericGetMax()).isEqualTo(Binary.fromString("world"));
    assertThat(statsFromBytes.genericGetMin()).isEqualTo(Binary.fromString("a"));

    // Test print formatting
    assertThat(stats).asString().isEqualTo("min: a, max: world, num_nulls: 0");
  }

  @Test
  public void testBinaryMinMaxForReusedBackingByteArray() {
    BinaryStatistics stats = new BinaryStatistics();

    byte[] bytes = new byte[] {10};
    final Binary value = Binary.fromReusedByteArray(bytes);
    stats.updateStats(value);

    bytes[0] = 20;
    stats.updateStats(value);

    bytes[0] = 15;
    stats.updateStats(value);

    assertThat(stats.getMaxBytes()).isEqualTo(new byte[] {20});
    assertThat(stats.getMinBytes()).isEqualTo(new byte[] {10});
  }

  @Test
  public void testMergingStatistics() {
    testMergingIntStats();
    testMergingLongStats();
    testMergingFloatStats();
    testMergingDoubleStats();
    testMergingBooleanStats();
    testMergingStringStats();
  }

  private void testMergingIntStats() {
    integerArray = new int[] {1, 2, 3, 4, 5};
    IntStatistics intStats = new IntStatistics();

    for (int s : integerArray) {
      intStats.updateStats(s);
    }

    integerArray = new int[] {0, 3, 3};
    IntStatistics intStats2 = new IntStatistics();

    for (int s : integerArray) {
      intStats2.updateStats(s);
    }
    intStats.mergeStatistics(intStats2);
    assertThat(intStats.getMax()).isEqualTo(5);
    assertThat(intStats.getMin()).isEqualTo(0);

    integerArray = new int[] {-1, -100, 100};
    IntStatistics intStats3 = new IntStatistics();
    for (int s : integerArray) {
      intStats3.updateStats(s);
    }
    intStats.mergeStatistics(intStats3);

    assertThat(intStats.getMax()).isEqualTo(100);
    assertThat(intStats.getMin()).isEqualTo(-100);
  }

  private void testMergingLongStats() {
    longArray = new long[] {1l, 2l, 3l, 4l, 5l};
    LongStatistics longStats = new LongStatistics();

    for (long s : longArray) {
      longStats.updateStats(s);
    }

    longArray = new long[] {0l, 3l, 3l};
    LongStatistics longStats2 = new LongStatistics();

    for (long s : longArray) {
      longStats2.updateStats(s);
    }
    longStats.mergeStatistics(longStats2);
    assertThat(longStats.getMax()).isEqualTo(5l);
    assertThat(longStats.getMin()).isEqualTo(0l);

    longArray = new long[] {-1l, -100l, 100l};
    LongStatistics longStats3 = new LongStatistics();
    for (long s : longArray) {
      longStats3.updateStats(s);
    }
    longStats.mergeStatistics(longStats3);

    assertThat(longStats.getMax()).isEqualTo(100l);
    assertThat(longStats.getMin()).isEqualTo(-100l);
  }

  private void testMergingFloatStats() {
    floatArray = new float[] {1.44f, 12.2f, 98.3f, 1.4f, 0.05f};
    FloatStatistics floatStats = new FloatStatistics();

    for (float s : floatArray) {
      floatStats.updateStats(s);
    }

    floatArray = new float[] {0.0001f, 9.9f, 3.1f};
    FloatStatistics floatStats2 = new FloatStatistics();

    for (float s : floatArray) {
      floatStats2.updateStats(s);
    }
    floatStats.mergeStatistics(floatStats2);
    assertThat(floatStats.getMax()).isCloseTo(98.3f, offset(1e-10f));
    assertThat(floatStats.getMin()).isCloseTo(0.0001f, offset(1e-10f));

    floatArray = new float[] {-1.91f, -100.9f, 100.54f};
    FloatStatistics floatStats3 = new FloatStatistics();
    for (float s : floatArray) {
      floatStats3.updateStats(s);
    }
    floatStats.mergeStatistics(floatStats3);

    assertThat(floatStats.getMax()).isCloseTo(100.54f, offset(1e-10f));
    assertThat(floatStats.getMin()).isCloseTo(-100.9f, offset(1e-10f));
  }

  private void testMergingDoubleStats() {
    doubleArray = new double[] {1.44d, 12.2d, 98.3d, 1.4d, 0.05d};
    DoubleStatistics doubleStats = new DoubleStatistics();

    for (double s : doubleArray) {
      doubleStats.updateStats(s);
    }

    doubleArray = new double[] {0.0001d, 9.9d, 3.1d};
    DoubleStatistics doubleStats2 = new DoubleStatistics();

    for (double s : doubleArray) {
      doubleStats2.updateStats(s);
    }
    doubleStats.mergeStatistics(doubleStats2);
    assertThat(doubleStats.getMax()).isCloseTo(98.3d, offset(1e-10));
    assertThat(doubleStats.getMin()).isCloseTo(0.0001d, offset(1e-10));

    doubleArray = new double[] {-1.91d, -100.9d, 100.54d};
    DoubleStatistics doubleStats3 = new DoubleStatistics();
    for (double s : doubleArray) {
      doubleStats3.updateStats(s);
    }
    doubleStats.mergeStatistics(doubleStats3);

    assertThat(doubleStats.getMax()).isCloseTo(100.54d, offset(1e-10));
    assertThat(doubleStats.getMin()).isCloseTo(-100.9d, offset(1e-10));
  }

  private void testMergingBooleanStats() {
    booleanArray = new boolean[] {true, true, true};
    BooleanStatistics booleanStats = new BooleanStatistics();

    for (boolean s : booleanArray) {
      booleanStats.updateStats(s);
    }

    booleanArray = new boolean[] {true, false};
    BooleanStatistics booleanStats2 = new BooleanStatistics();

    for (boolean s : booleanArray) {
      booleanStats2.updateStats(s);
    }
    booleanStats.mergeStatistics(booleanStats2);
    assertThat(booleanStats.getMax()).isTrue();
    assertThat(booleanStats.getMin()).isFalse();

    booleanArray = new boolean[] {false, false, false, false};
    BooleanStatistics booleanStats3 = new BooleanStatistics();
    for (boolean s : booleanArray) {
      booleanStats3.updateStats(s);
    }
    booleanStats.mergeStatistics(booleanStats3);

    assertThat(booleanStats.getMax()).isTrue();
    assertThat(booleanStats.getMin()).isFalse();
  }

  private void testMergingStringStats() {
    stringArray = new String[] {"hello", "world", "this", "is", "a", "test", "of", "the", "stats", "class"};
    BinaryStatistics stats = new BinaryStatistics();

    for (String s : stringArray) {
      stats.updateStats(Binary.fromString(s));
    }

    stringArray = new String[] {"zzzz", "asdf", "testing"};
    BinaryStatistics stats2 = new BinaryStatistics();

    for (String s : stringArray) {
      stats2.updateStats(Binary.fromString(s));
    }
    stats.mergeStatistics(stats2);
    assertThat(stats.getMax()).isEqualTo(Binary.fromString("zzzz"));
    assertThat(stats.getMin()).isEqualTo(Binary.fromString("a"));

    stringArray = new String[] {"", "good", "testing"};
    BinaryStatistics stats3 = new BinaryStatistics();
    for (String s : stringArray) {
      stats3.updateStats(Binary.fromString(s));
    }
    stats.mergeStatistics(stats3);

    assertThat(stats.getMax()).isEqualTo(Binary.fromString("zzzz"));
    assertThat(stats.getMin()).isEqualTo(Binary.fromString(""));
  }

  @Test
  public void testBuilder() {
    testBuilder(Types.required(BOOLEAN).named("test_boolean"), false, new byte[] {0}, true, new byte[] {1});
    testBuilder(Types.required(INT32).named("test_int32"), -42, intToBytes(-42), 42, intToBytes(42));
    testBuilder(Types.required(INT64).named("test_int64"), -42l, longToBytes(-42), 42l, longToBytes(42));
    testBuilder(
        Types.required(FLOAT).named("test_float"),
        -42.0f,
        intToBytes(floatToIntBits(-42.0f)),
        42.0f,
        intToBytes(floatToIntBits(42.0f)));
    testBuilder(
        Types.required(DOUBLE).named("test_double"),
        -42.0,
        longToBytes(doubleToLongBits(-42.0)),
        42.0,
        longToBytes(Double.doubleToLongBits(42.0f)));

    byte[] min = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    byte[] max = {13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
    testBuilder(
        Types.required(INT96).named("test_int96"),
        Binary.fromConstantByteArray(min),
        min,
        Binary.fromConstantByteArray(max),
        max);
    testBuilder(
        Types.required(FIXED_LEN_BYTE_ARRAY).length(12).named("test_fixed"),
        Binary.fromConstantByteArray(min),
        min,
        Binary.fromConstantByteArray(max),
        max);
    testBuilder(
        Types.required(BINARY).named("test_binary"),
        Binary.fromConstantByteArray(min),
        min,
        Binary.fromConstantByteArray(max),
        max);
  }

  private void testBuilder(PrimitiveType type, Object min, byte[] minBytes, Object max, byte[] maxBytes) {
    Statistics.Builder builder = Statistics.getBuilderForReading(type);
    Statistics<?> stats = builder.build();
    assertThat(stats.isEmpty()).isTrue();
    assertThat(stats.isNumNullsSet()).isFalse();
    assertThat(stats.hasNonNullValue()).isFalse();

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withNumNulls(0).withMin(minBytes).build();
    assertThat(stats.isEmpty()).isFalse();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(stats.getNumNulls()).isEqualTo(0);

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withNumNulls(11).withMax(maxBytes).build();
    assertThat(stats.isEmpty()).isFalse();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(stats.getNumNulls()).isEqualTo(11);

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withNumNulls(42).withMin(minBytes).withMax(maxBytes).build();
    assertThat(stats.isEmpty()).isFalse();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.hasNonNullValue()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(42);
    assertThat(stats.genericGetMin()).isEqualTo(min);
    assertThat(stats.genericGetMax()).isEqualTo(max);
  }

  @Test
  public void testSpecBuilderForFloat() {
    PrimitiveType type = Types.required(FLOAT).named("test_float");
    Statistics.Builder builder = Statistics.getBuilderForReading(type);
    Statistics<?> stats = builder.withMin(intToBytes(floatToIntBits(Float.NaN)))
        .withMax(intToBytes(floatToIntBits(42.0f)))
        .withNumNulls(0)
        .build();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(0);
    assertThat(stats.hasNonNullValue()).isFalse();

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(intToBytes(floatToIntBits(-42.0f)))
        .withMax(intToBytes(floatToIntBits(Float.NaN)))
        .withNumNulls(11)
        .build();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(11);
    assertThat(stats.hasNonNullValue()).isFalse();

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(intToBytes(floatToIntBits(Float.NaN)))
        .withMax(intToBytes(floatToIntBits(Float.NaN)))
        .withNumNulls(42)
        .build();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(42);
    assertThat(stats.hasNonNullValue()).isFalse();

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(intToBytes(floatToIntBits(0.0f)))
        .withMax(intToBytes(floatToIntBits(42.0f)))
        .build();
    assertThat((Float) stats.genericGetMin())
        .usingComparator(Float::compare)
        .isEqualByComparingTo(-0.0f);
    assertThat((Float) stats.genericGetMax())
        .usingComparator(Float::compare)
        .isEqualByComparingTo(42.0f);

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(intToBytes(floatToIntBits(-42.0f)))
        .withMax(intToBytes(floatToIntBits(-0.0f)))
        .build();
    assertThat((Float) stats.genericGetMin())
        .usingComparator(Float::compare)
        .isEqualByComparingTo(-42.0f);
    assertThat((Float) stats.genericGetMax())
        .usingComparator(Float::compare)
        .isEqualByComparingTo(0.0f);

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(intToBytes(floatToIntBits(0.0f)))
        .withMax(intToBytes(floatToIntBits(-0.0f)))
        .build();
    assertThat((Float) stats.genericGetMin())
        .usingComparator(Float::compare)
        .isEqualByComparingTo(-0.0f);
    assertThat((Float) stats.genericGetMax())
        .usingComparator(Float::compare)
        .isEqualByComparingTo(0.0f);
  }

  @Test
  public void testSpecBuilderForDouble() {
    PrimitiveType type = Types.required(DOUBLE).named("test_double");
    Statistics.Builder builder = Statistics.getBuilderForReading(type);
    Statistics<?> stats = builder.withMin(longToBytes(doubleToLongBits(Double.NaN)))
        .withMax(longToBytes(doubleToLongBits(42.0)))
        .withNumNulls(0)
        .build();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(0);
    assertThat(stats.hasNonNullValue()).isFalse();

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(longToBytes(doubleToLongBits(-42.0)))
        .withMax(longToBytes(doubleToLongBits(Double.NaN)))
        .withNumNulls(11)
        .build();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(11);
    assertThat(stats.hasNonNullValue()).isFalse();

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(longToBytes(doubleToLongBits(Double.NaN)))
        .withMax(longToBytes(doubleToLongBits(Double.NaN)))
        .withNumNulls(42)
        .build();
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(42);
    assertThat(stats.hasNonNullValue()).isFalse();

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(longToBytes(doubleToLongBits(0.0)))
        .withMax(longToBytes(doubleToLongBits(42.0)))
        .build();
    assertThat((Double) stats.genericGetMin())
        .usingComparator(Double::compare)
        .isEqualByComparingTo(-0.0);
    assertThat((Double) stats.genericGetMax())
        .usingComparator(Double::compare)
        .isEqualByComparingTo(42.0);

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(longToBytes(doubleToLongBits(-42.0)))
        .withMax(longToBytes(doubleToLongBits(-0.0)))
        .build();
    assertThat((Double) stats.genericGetMin())
        .usingComparator(Double::compare)
        .isEqualByComparingTo(-42.0);
    assertThat((Double) stats.genericGetMax())
        .usingComparator(Double::compare)
        .isEqualByComparingTo(0.0);

    builder = Statistics.getBuilderForReading(type);
    stats = builder.withMin(longToBytes(doubleToLongBits(0.0)))
        .withMax(longToBytes(doubleToLongBits(-0.0)))
        .build();
    assertThat((Double) stats.genericGetMin())
        .usingComparator(Double::compare)
        .isEqualByComparingTo(-0.0);
    assertThat((Double) stats.genericGetMax())
        .usingComparator(Double::compare)
        .isEqualByComparingTo(0.0);
  }

  @Test
  public void testNoopStatistics() {
    // Test basic max/min
    integerArray = new int[] {1, 3, 14, 54, 66, 8, 0, 23, 54};
    Statistics<?> stats = Statistics.noopStats(new PrimitiveType(REQUIRED, INT32, "int32"));
    assertThat(stats.isEmpty()).isTrue();

    for (int i : integerArray) {
      stats.updateStats(i);
    }

    assertThat(stats.getNumNulls()).isEqualTo(-1);
    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(stats.isNumNullsSet()).isFalse();
    assertThat(stats.isEmpty()).isTrue();

    assertThatThrownBy(stats::genericGetMax)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("genericGetMax is not supported by org.apache.parquet.column.statistics.NoopStatistics");
    assertThatThrownBy(stats::genericGetMin)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("genericGetMin is not supported by org.apache.parquet.column.statistics.NoopStatistics");
    assertThatThrownBy(stats::getMaxBytes)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("getMaxBytes is not supported by org.apache.parquet.column.statistics.NoopStatistics");
    assertThatThrownBy(stats::getMinBytes)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("getMinBytes is not supported by org.apache.parquet.column.statistics.NoopStatistics");
    assertThatThrownBy(stats::maxAsString)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("genericGetMax is not supported by org.apache.parquet.column.statistics.NoopStatistics");
    assertThatThrownBy(stats::minAsString)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("genericGetMin is not supported by org.apache.parquet.column.statistics.NoopStatistics");
    assertThatThrownBy(() -> stats.isSmallerThan(0))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("isSmallerThan is not supported by org.apache.parquet.column.statistics.NoopStatistics");
  }

  @Test
  public void testBinaryIsSmallerThanNoOverflowForLargeValues() {
    BinaryStatistics stats = new BinaryStatistics();
    // Create a Binary whose length() reports 2^30 without allocating 1 GB
    Binary fakeLarge = Binary.fromConstantByteArray(new byte[0], 0, 1 << 30);
    stats.updateStats(fakeLarge);

    // min.length() + max.length() = 2^31, must not overflow int to negative
    assertThat(stats.isSmallerThan(4096)).isFalse();
  }
}
