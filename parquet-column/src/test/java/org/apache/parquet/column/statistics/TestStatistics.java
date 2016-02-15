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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import junit.framework.Assert;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOptBuilder;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOpts;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterStatistics;
import org.junit.Test;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class TestStatistics {
  private int[] integerArray;
  private long[] longArray;
  private float[] floatArray;
  private double[] doubleArray;
  private String[] stringArray;
  private boolean[] booleanArray;

  @Test
  public void testNumNulls() {
    IntStatistics stats = new IntStatistics(new ColumnStatisticsOpts(null));
    assertEquals(stats.getNumNulls(), 0);

    stats.incrementNumNulls();
    stats.incrementNumNulls();
    stats.incrementNumNulls();
    stats.incrementNumNulls();
    assertEquals(stats.getNumNulls(), 4);

    stats.incrementNumNulls(5);
    assertEquals(stats.getNumNulls(), 9);

    stats.setNumNulls(22);
    assertEquals(stats.getNumNulls(), 22);
  }

  @Test
  public void testBloomFilter() {
    MessageType messageType = MessageTypeParser.parseMessageType(
        "message hive_schema {optional int32 a; optional int64 b; optional double c; optional float d; optional binary e;}");
    BloomFilterOpts opts =
        new BloomFilterOptBuilder().enableCols("a,b,c,d,e").expectedEntries("100,100,100,100,100")
            .falsePositiveProbabilities("0.05,0.05,0.05,0.05,0.05").build(messageType);
    StatisticsOpts statisticsOpts = new StatisticsOpts(opts);

    checkBloomFilter(new Integer[] { 1, 2, 3 },
        new IntStatistics(getBloomFilterOpts(statisticsOpts, "a", messageType)));
    checkBloomFilter(new Long[] { 1L, 2L, 3L },
        new LongStatistics(getBloomFilterOpts(statisticsOpts, "b", messageType)));
    checkBloomFilter(new Double[] { 1.2, 3.34, 5.5 },
        new DoubleStatistics(getBloomFilterOpts(statisticsOpts, "c", messageType)));
    checkBloomFilter(new Float[] { 1.2f, 3.3f, 4.4f },
        new FloatStatistics(getBloomFilterOpts(statisticsOpts, "d", messageType)));
    checkBloomFilter(
        new Binary[] { Binary.fromString("ac"), Binary.fromString("bd"), Binary.fromString("ef") },
        new BinaryStatistics(getBloomFilterOpts(statisticsOpts, "b", messageType)));
  }

  private static ColumnStatisticsOpts getBloomFilterOpts(
      StatisticsOpts statisticsOpts,
      String colName,
      MessageType messageType) {
    return statisticsOpts.getStatistics(messageType.getColumnDescription(new String[] { colName }));
  }

  private void checkBloomFilter(Integer[] data, BloomFilterStatistics statistics) {
    int v1 = data[0];
    int v2 = data[1];
    int v3 = data[2];
    Assert.assertFalse(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));
    ((Statistics) statistics).updateStats(v1);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v2);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v3);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertTrue(statistics.test(v3));
  }

  private void checkBloomFilter(Double[] data, BloomFilterStatistics statistics) {
    Double v1 = data[0];
    Double v2 = data[1];
    Double v3 = data[2];
    Assert.assertFalse(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));
    ((Statistics) statistics).updateStats(v1);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v2);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v3);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertTrue(statistics.test(v3));
  }

  private void checkBloomFilter(Long[] data, BloomFilterStatistics statistics) {
    Long v1 = data[0];
    Long v2 = data[1];
    Long v3 = data[2];
    Assert.assertFalse(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));
    ((Statistics) statistics).updateStats(v1);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v2);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v3);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertTrue(statistics.test(v3));
  }


  private void checkBloomFilter(Float[] data, BloomFilterStatistics statistics) {
    Float v1 = data[0];
    Float v2 = data[1];
    Float v3 = data[2];
    Assert.assertFalse(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));
    ((Statistics) statistics).updateStats(v1);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v2);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v3);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertTrue(statistics.test(v3));
  }

  private void checkBloomFilter(Binary[] data, BloomFilterStatistics statistics) {
    Binary v1 = data[0];
    Binary v2 = data[1];
    Binary v3 = data[2];
    Assert.assertFalse(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));
    ((Statistics) statistics).updateStats(v1);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertFalse(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v2);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertFalse(statistics.test(v3));

    ((Statistics) statistics).updateStats(v3);
    Assert.assertTrue(statistics.test(v1));
    Assert.assertTrue(statistics.test(v2));
    Assert.assertTrue(statistics.test(v3));
  }

  @Test
  public void testIntMinMax() {
    // Test basic max/min
    integerArray = new int[] {1, 3, 14, 54, 66, 8, 0, 23, 54};
    IntStatistics stats = new IntStatistics(new ColumnStatisticsOpts(null));

    for (int i: integerArray) {
      stats.updateStats(i);
    }
    assertEquals(stats.getMax(), 66);
    assertEquals(stats.getMin(), 0);

    // Test negative values
    integerArray = new int[] {-11, 3, -14, 54, -66, 8, 0, -23, 54};
    IntStatistics statsNeg = new IntStatistics(new ColumnStatisticsOpts(null));

    for (int i: integerArray) {
      statsNeg.updateStats(i);
    }
    assertEquals(statsNeg.getMax(), 54);
    assertEquals(statsNeg.getMin(), -66);

    // Test converting to and from byte[]
    byte[] intMaxBytes = statsNeg.getMaxBytes();
    byte[] intMinBytes = statsNeg.getMinBytes();

    assertEquals(ByteBuffer.wrap(intMaxBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt(), 54);
    assertEquals(ByteBuffer.wrap(intMinBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt(), -66);

    IntStatistics statsFromBytes = new IntStatistics(new ColumnStatisticsOpts(null));
    statsFromBytes.setMinMaxFromBytes(intMinBytes, intMaxBytes);

    assertEquals(statsFromBytes.getMax(), 54);
    assertEquals(statsFromBytes.getMin(), -66);

    integerArray = new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE};
    IntStatistics minMaxValues = new IntStatistics(new ColumnStatisticsOpts(null));

    for (int i: integerArray) {
      minMaxValues.updateStats(i);
    }
    assertEquals(minMaxValues.getMax(), Integer.MAX_VALUE);
    assertEquals(minMaxValues.getMin(), Integer.MIN_VALUE);

    // Test converting to and from byte[] for large and small values
    byte[] intMaxBytesMinMax = minMaxValues.getMaxBytes();
    byte[] intMinBytesMinMax = minMaxValues.getMinBytes();

    assertEquals(ByteBuffer.wrap(intMaxBytesMinMax).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt(), Integer.MAX_VALUE);
    assertEquals(ByteBuffer.wrap(intMinBytesMinMax).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt(), Integer.MIN_VALUE);

    IntStatistics statsFromBytesMinMax= new IntStatistics(new ColumnStatisticsOpts(null));
    statsFromBytesMinMax.setMinMaxFromBytes(intMinBytesMinMax, intMaxBytesMinMax);

    assertEquals(statsFromBytesMinMax.getMax(), Integer.MAX_VALUE);
    assertEquals(statsFromBytesMinMax.getMin(), Integer.MIN_VALUE);

    // Test print formatting
    assertEquals(stats.toString(), "min: 0, max: 66, num_nulls: 0");
  }

  @Test
  public void testLongMinMax() {
    // Test basic max/min
    longArray = new long[] {9, 39, 99, 3, 0, 12, 1000, 65, 542};
    LongStatistics stats = new LongStatistics(null);

    for (long l: longArray) {
      stats.updateStats(l);
    }
    assertEquals(stats.getMax(), 1000);
    assertEquals(stats.getMin(), 0);

    // Test negative values
    longArray = new long[] {-101, 993, -9914, 54, -9, 89, 0, -23, 90};
    LongStatistics statsNeg = new LongStatistics(null);

    for (long l: longArray) {
      statsNeg.updateStats(l);
    }
    assertEquals(statsNeg.getMax(), 993);
    assertEquals(statsNeg.getMin(), -9914);

    // Test converting to and from byte[]
    byte[] longMaxBytes = statsNeg.getMaxBytes();
    byte[] longMinBytes = statsNeg.getMinBytes();

    assertEquals(ByteBuffer.wrap(longMaxBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong(), 993);
    assertEquals(ByteBuffer.wrap(longMinBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong(), -9914);

    LongStatistics statsFromBytes = new LongStatistics(null);
    statsFromBytes.setMinMaxFromBytes(longMinBytes, longMaxBytes);

    assertEquals(statsFromBytes.getMax(), 993);
    assertEquals(statsFromBytes.getMin(), -9914);

    longArray = new long[] {Long.MAX_VALUE, Long.MIN_VALUE};
    LongStatistics minMaxValues = new LongStatistics(null);

    for (long l: longArray) {
      minMaxValues.updateStats(l);
    }
    assertEquals(minMaxValues.getMax(), Long.MAX_VALUE);
    assertEquals(minMaxValues.getMin(), Long.MIN_VALUE);

    // Test converting to and from byte[] for large and small values
    byte[] longMaxBytesMinMax = minMaxValues.getMaxBytes();
    byte[] longMinBytesMinMax = minMaxValues.getMinBytes();

    assertEquals(ByteBuffer.wrap(longMaxBytesMinMax).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong(), Long.MAX_VALUE);
    assertEquals(ByteBuffer.wrap(longMinBytesMinMax).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong(), Long.MIN_VALUE);

    LongStatistics statsFromBytesMinMax= new LongStatistics(null);
    statsFromBytesMinMax.setMinMaxFromBytes(longMinBytesMinMax, longMaxBytesMinMax);

    assertEquals(statsFromBytesMinMax.getMax(), Long.MAX_VALUE);
    assertEquals(statsFromBytesMinMax.getMin(), Long.MIN_VALUE);

    // Test print formatting
    assertEquals(stats.toString(), "min: 0, max: 1000, num_nulls: 0");
  }

  @Test
  public void testFloatMinMax() {
    // Test basic max/min
    floatArray = new float[] {1.5f, 44.5f, 412.99f, 0.65f, 5.6f, 100.6f, 0.0001f, 23.0f, 553.6f};
    FloatStatistics stats = new FloatStatistics(null);

    for (float f: floatArray) {
      stats.updateStats(f);
    }
    assertEquals(stats.getMax(), 553.6f, 1e-10);
    assertEquals(stats.getMin(), 0.0001f, 1e-10);

    // Test negative values
    floatArray = new float[] {-1.5f, -44.5f, -412.99f, 0.65f, -5.6f, -100.6f, 0.0001f, -23.0f, -3.6f};
    FloatStatistics statsNeg = new FloatStatistics(null);

    for (float f: floatArray) {
      statsNeg.updateStats(f);
    }
    assertEquals(statsNeg.getMax(), 0.65f, 1e-10);
    assertEquals(statsNeg.getMin(), -412.99f, 1e-10);

    // Test converting to and from byte[]
    byte[] floatMaxBytes = statsNeg.getMaxBytes();
    byte[] floatMinBytes = statsNeg.getMinBytes();

    assertEquals(ByteBuffer.wrap(floatMaxBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getFloat(), 0.65f, 1e-10);
    assertEquals(ByteBuffer.wrap(floatMinBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getFloat(), -412.99f, 1e-10);

    FloatStatistics statsFromBytes = new FloatStatistics(null);
    statsFromBytes.setMinMaxFromBytes(floatMinBytes, floatMaxBytes);

    assertEquals(statsFromBytes.getMax(), 0.65f, 1e-10);
    assertEquals(statsFromBytes.getMin(), -412.99f, 1e-10);

    floatArray = new float[] {Float.MAX_VALUE, Float.MIN_VALUE};
    FloatStatistics minMaxValues = new FloatStatistics(null);

    for (float f: floatArray) {
      minMaxValues.updateStats(f);
    }
    assertEquals(minMaxValues.getMax(), Float.MAX_VALUE, 1e-10);
    assertEquals(minMaxValues.getMin(), Float.MIN_VALUE, 1e-10);

    // Test converting to and from byte[] for large and small values
    byte[] floatMaxBytesMinMax = minMaxValues.getMaxBytes();
    byte[] floatMinBytesMinMax = minMaxValues.getMinBytes();

    assertEquals(ByteBuffer.wrap(floatMaxBytesMinMax).order(java.nio.ByteOrder.LITTLE_ENDIAN).getFloat(), Float.MAX_VALUE, 1e-10);
    assertEquals(ByteBuffer.wrap(floatMinBytesMinMax).order(java.nio.ByteOrder.LITTLE_ENDIAN).getFloat(), Float.MIN_VALUE, 1e-10);

    FloatStatistics statsFromBytesMinMax= new FloatStatistics(null);
    statsFromBytesMinMax.setMinMaxFromBytes(floatMinBytesMinMax, floatMaxBytesMinMax);

    assertEquals(statsFromBytesMinMax.getMax(), Float.MAX_VALUE, 1e-10);
    assertEquals(statsFromBytesMinMax.getMin(), Float.MIN_VALUE, 1e-10);

    // Test print formatting
    assertEquals(stats.toString(), "min: 0.00010, max: 553.59998, num_nulls: 0");
  }

  @Test
  public void testDoubleMinMax() {
    // Test basic max/min
    doubleArray = new double[] {81.5d, 944.5f, 2.002d, 334.5d, 5.6d, 0.001d, 0.00001d, 23.0d, 553.6d};
    DoubleStatistics stats = new DoubleStatistics(null);

    for (double d: doubleArray) {
      stats.updateStats(d);
    }
    assertEquals(stats.getMax(), 944.5d, 1e-10);
    assertEquals(stats.getMin(), 0.00001d, 1e-10);

    // Test negative values
    doubleArray = new double[] {-81.5d, -944.5d, 2.002d, -334.5d, -5.6d, -0.001d, -0.00001d, 23.0d, -3.6d};
    DoubleStatistics statsNeg = new DoubleStatistics(null);

    for (double d: doubleArray) {
      statsNeg.updateStats(d);
    }
    assertEquals(statsNeg.getMax(), 23.0d, 1e-10);
    assertEquals(statsNeg.getMin(), -944.5d, 1e-10);

    // Test converting to and from byte[]
    byte[] doubleMaxBytes = statsNeg.getMaxBytes();
    byte[] doubleMinBytes = statsNeg.getMinBytes();

    assertEquals(ByteBuffer.wrap(doubleMaxBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getDouble(), 23.0d, 1e-10);
    assertEquals(ByteBuffer.wrap(doubleMinBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getDouble(), -944.5d, 1e-10);

    DoubleStatistics statsFromBytes = new DoubleStatistics(null);
    statsFromBytes.setMinMaxFromBytes(doubleMinBytes, doubleMaxBytes);

    assertEquals(statsFromBytes.getMax(), 23.0d, 1e-10);
    assertEquals(statsFromBytes.getMin(), -944.5d, 1e-10);

    doubleArray = new double[] {Double.MAX_VALUE, Double.MIN_VALUE};
    DoubleStatistics minMaxValues = new DoubleStatistics(null);

    for (double d: doubleArray) {
      minMaxValues.updateStats(d);
    }
    assertEquals(minMaxValues.getMax(), Double.MAX_VALUE, 1e-10);
    assertEquals(minMaxValues.getMin(), Double.MIN_VALUE, 1e-10);

    // Test converting to and from byte[] for large and small values
    byte[] doubleMaxBytesMinMax = minMaxValues.getMaxBytes();
    byte[] doubleMinBytesMinMax = minMaxValues.getMinBytes();

    assertEquals(ByteBuffer.wrap(doubleMaxBytesMinMax).order(java.nio.ByteOrder.LITTLE_ENDIAN).getDouble(), Double.MAX_VALUE, 1e-10);
    assertEquals(ByteBuffer.wrap(doubleMinBytesMinMax).order(java.nio.ByteOrder.LITTLE_ENDIAN).getDouble(), Double.MIN_VALUE, 1e-10);

    DoubleStatistics statsFromBytesMinMax= new DoubleStatistics(null);
    statsFromBytesMinMax.setMinMaxFromBytes(doubleMinBytesMinMax, doubleMaxBytesMinMax);

    assertEquals(statsFromBytesMinMax.getMax(), Double.MAX_VALUE, 1e-10);
    assertEquals(statsFromBytesMinMax.getMin(), Double.MIN_VALUE, 1e-10);

    // Test print formatting
    assertEquals(stats.toString(), "min: 0.00001, max: 944.50000, num_nulls: 0");
  }

  @Test
  public void testBooleanMinMax() {
    // Test all true
    booleanArray = new boolean[] {true, true, true};
    BooleanStatistics statsTrue = new BooleanStatistics();

    for (boolean i: booleanArray) {
      statsTrue.updateStats(i);
    }
    assertTrue(statsTrue.getMax());
    assertTrue(statsTrue.getMin());

    // Test all false
    booleanArray = new boolean[] {false, false, false};
    BooleanStatistics statsFalse = new BooleanStatistics();

    for (boolean i: booleanArray) {
      statsFalse.updateStats(i);
    }
    assertFalse(statsFalse.getMax());
    assertFalse(statsFalse.getMin());

    booleanArray = new boolean[] {false, true, false};
    BooleanStatistics statsBoth = new BooleanStatistics();

    for (boolean i: booleanArray) {
      statsBoth.updateStats(i);
    }
    assertTrue(statsBoth.getMax());
    assertFalse(statsBoth.getMin());

    // Test converting to and from byte[]
    byte[] boolMaxBytes = statsBoth.getMaxBytes();
    byte[] boolMinBytes = statsBoth.getMinBytes();

    assertEquals((int)(boolMaxBytes[0] & 255), 1);
    assertEquals((int)(boolMinBytes[0] & 255), 0);

    BooleanStatistics statsFromBytes = new BooleanStatistics();
    statsFromBytes.setMinMaxFromBytes(boolMinBytes, boolMaxBytes);

    assertTrue(statsFromBytes.getMax());
    assertFalse(statsFromBytes.getMin());

    // Test print formatting
    assertEquals(statsBoth.toString(), "min: false, max: true, num_nulls: 0");
  }

  @Test
  public void testBinaryMinMax() {
    //Test basic max/min
    stringArray = new String[] {"hello", "world", "this", "is", "a", "test", "of", "the", "stats", "class"};
    BinaryStatistics stats = new BinaryStatistics(null);

    for (String s: stringArray) {
      stats.updateStats(Binary.fromString(s));
    }
    assertEquals(stats.getMax(), Binary.fromString("world"));
    assertEquals(stats.getMin(), Binary.fromString("a"));

    // Test empty string
    stringArray = new String[] {"", "", "", "", ""};
    BinaryStatistics statsEmpty = new BinaryStatistics(null);

    for (String s: stringArray) {
      statsEmpty.updateStats(Binary.fromString(s));
    }
    assertEquals(statsEmpty.getMax(), Binary.fromString(""));
    assertEquals(statsEmpty.getMin(), Binary.fromString(""));

    // Test converting to and from byte[]
    byte[] stringMaxBytes = stats.getMaxBytes();
    byte[] stringMinBytes = stats.getMinBytes();

    assertEquals(new String(stringMaxBytes), "world");
    assertEquals(new String(stringMinBytes), "a");

    BinaryStatistics statsFromBytes = new BinaryStatistics(null);
    statsFromBytes.setMinMaxFromBytes(stringMinBytes, stringMaxBytes);

    assertEquals(statsFromBytes.getMax(), Binary.fromString("world"));
    assertEquals(statsFromBytes.getMin(), Binary.fromString("a"));

    // Test print formatting
    assertEquals(stats.toString(), "min: a, max: world, num_nulls: 0");
  }

  @Test
  public void testBinaryMinMaxForReusedBackingByteArray() {
    BinaryStatistics stats = new BinaryStatistics(null);

    byte[] bytes = new byte[] { 10 };
    final Binary value = Binary.fromReusedByteArray(bytes);
    stats.updateStats(value);

    bytes[0] = 20;
    stats.updateStats(value);

    bytes[0] = 15;
    stats.updateStats(value);

    assertArrayEquals(new byte[] { 20 }, stats.getMaxBytes());
    assertArrayEquals(new byte[] { 10 }, stats.getMinBytes());
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
    IntStatistics intStats = new IntStatistics(new ColumnStatisticsOpts(null));

    for (int s: integerArray) {
      intStats.updateStats(s);
    }

    integerArray = new int[] {0, 3, 3};
    IntStatistics intStats2 = new IntStatistics(new ColumnStatisticsOpts(null));

    for (int s: integerArray) {
      intStats2.updateStats(s);
    }
    intStats.mergeStatistics(intStats2);
    assertEquals(intStats.getMax(), 5);
    assertEquals(intStats.getMin(), 0);

    integerArray = new int[] {-1, -100, 100};
    IntStatistics intStats3 = new IntStatistics(new ColumnStatisticsOpts(null));
    for (int s: integerArray) {
      intStats3.updateStats(s);
    }
    intStats.mergeStatistics(intStats3);

    assertEquals(intStats.getMax(), 100);
    assertEquals(intStats.getMin(), -100);
  }

  private void testMergingLongStats() {
    longArray = new long[] {1l, 2l, 3l, 4l, 5l};
    LongStatistics longStats = new LongStatistics(null);

    for (long s: longArray) {
      longStats.updateStats(s);
    }

    longArray = new long[] {0l, 3l, 3l};
    LongStatistics longStats2 = new LongStatistics(null);

    for (long s: longArray) {
      longStats2.updateStats(s);
    }
    longStats.mergeStatistics(longStats2);
    assertEquals(longStats.getMax(), 5l);
    assertEquals(longStats.getMin(), 0l);

    longArray = new long[] {-1l, -100l, 100l};
    LongStatistics longStats3 = new LongStatistics(null);
    for (long s: longArray) {
      longStats3.updateStats(s);
    }
    longStats.mergeStatistics(longStats3);

    assertEquals(longStats.getMax(), 100l);
    assertEquals(longStats.getMin(), -100l);
  }

  private void testMergingFloatStats() {
    floatArray = new float[] {1.44f, 12.2f, 98.3f, 1.4f, 0.05f};
    FloatStatistics floatStats = new FloatStatistics(null);

    for (float s: floatArray) {
      floatStats.updateStats(s);
    }

    floatArray = new float[] {0.0001f, 9.9f, 3.1f};
    FloatStatistics floatStats2 = new FloatStatistics(null);

    for (float s: floatArray) {
      floatStats2.updateStats(s);
    }
    floatStats.mergeStatistics(floatStats2);
    assertEquals(floatStats.getMax(), 98.3f, 1e-10);
    assertEquals(floatStats.getMin(), 0.0001f, 1e-10);

    floatArray = new float[] {-1.91f, -100.9f, 100.54f};
    FloatStatistics floatStats3 = new FloatStatistics(null);
    for (float s: floatArray) {
      floatStats3.updateStats(s);
    }
    floatStats.mergeStatistics(floatStats3);

    assertEquals(floatStats.getMax(), 100.54f, 1e-10);
    assertEquals(floatStats.getMin(), -100.9f, 1e-10);
  }

  private void testMergingDoubleStats() {
    doubleArray = new double[] {1.44d, 12.2d, 98.3d, 1.4d, 0.05d};
    DoubleStatistics doubleStats = new DoubleStatistics(null);

    for (double s: doubleArray) {
      doubleStats.updateStats(s);
    }

    doubleArray = new double[] {0.0001d, 9.9d, 3.1d};
    DoubleStatistics doubleStats2 = new DoubleStatistics(null);

    for (double s: doubleArray) {
      doubleStats2.updateStats(s);
    }
    doubleStats.mergeStatistics(doubleStats2);
    assertEquals(doubleStats.getMax(), 98.3d, 1e-10);
    assertEquals(doubleStats.getMin(), 0.0001d, 1e-10);

    doubleArray = new double[] {-1.91d, -100.9d, 100.54d};
    DoubleStatistics doubleStats3 = new DoubleStatistics(null);
    for (double s: doubleArray) {
      doubleStats3.updateStats(s);
    }
    doubleStats.mergeStatistics(doubleStats3);

    assertEquals(doubleStats.getMax(), 100.54d, 1e-10);
    assertEquals(doubleStats.getMin(), -100.9d, 1e-10);
  }

  private void testMergingBooleanStats() {
    booleanArray = new boolean[] {true, true, true};
    BooleanStatistics booleanStats = new BooleanStatistics();

    for (boolean s: booleanArray) {
      booleanStats.updateStats(s);
    }

    booleanArray = new boolean[] {true, false};
    BooleanStatistics booleanStats2 = new BooleanStatistics();

    for (boolean s: booleanArray) {
      booleanStats2.updateStats(s);
    }
    booleanStats.mergeStatistics(booleanStats2);
    assertEquals(booleanStats.getMax(), true);
    assertEquals(booleanStats.getMin(), false);

    booleanArray = new boolean[] {false, false, false, false};
    BooleanStatistics booleanStats3 = new BooleanStatistics();
    for (boolean s: booleanArray) {
      booleanStats3.updateStats(s);
    }
    booleanStats.mergeStatistics(booleanStats3);

    assertEquals(booleanStats.getMax(), true);
    assertEquals(booleanStats.getMin(), false);
  }

  private void testMergingStringStats() {
    stringArray = new String[] {"hello", "world", "this", "is", "a", "test", "of", "the", "stats", "class"};
    BinaryStatistics stats = new BinaryStatistics(null);

    for (String s: stringArray) {
      stats.updateStats(Binary.fromString(s));
    }

    stringArray = new String[] {"zzzz", "asdf", "testing"};
    BinaryStatistics stats2 = new BinaryStatistics(null);

    for (String s: stringArray) {
      stats2.updateStats(Binary.fromString(s));
    }
    stats.mergeStatistics(stats2);
    assertEquals(stats.getMax(), Binary.fromString("zzzz"));
    assertEquals(stats.getMin(), Binary.fromString("a"));

    stringArray = new String[] {"", "good", "testing"};
    BinaryStatistics stats3 = new BinaryStatistics(null);
    for (String s: stringArray) {
      stats3.updateStats(Binary.fromString(s));
    }
    stats.mergeStatistics(stats3);

    assertEquals(stats.getMax(), Binary.fromString("zzzz"));
    assertEquals(stats.getMin(), Binary.fromString(""));
  }
}
