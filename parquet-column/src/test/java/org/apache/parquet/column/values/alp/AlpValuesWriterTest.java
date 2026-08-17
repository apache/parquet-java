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
package org.apache.parquet.column.values.alp;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;
import org.apache.parquet.bytes.BytesInput;
import org.junit.Test;

public class AlpValuesWriterTest {

  // ========== Float writer ==========

  @Test
  public void testFloatWriterRoundTrip() throws IOException {
    float[] values = new float[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i * 0.1f;
    }

    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }

    BytesInput bytes = writer.getBytes();
    byte[] compressed = bytes.toByteArray();
    assertTrue(compressed.length >= AlpConstants.HEADER_SIZE);

    float[] output = new float[values.length];
    AlpWrapper.decodeFloats(compressed, compressed.length, output, values.length);

    for (int i = 0; i < values.length; i++) {
      assertEquals("Mismatch at " + i, Float.floatToRawIntBits(values[i]), Float.floatToRawIntBits(output[i]));
    }
  }

  @Test
  public void testFloatWriterMultipleVectors() throws IOException {
    // 2500 values = 2 full vectors + 1 partial
    float[] values = new float[2500];
    Random rng = new Random(42);
    for (int i = 0; i < 2500; i++) {
      values[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }

    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }

    byte[] compressed = writer.getBytes().toByteArray();

    float[] output = new float[values.length];
    AlpWrapper.decodeFloats(compressed, compressed.length, output, values.length);

    for (int i = 0; i < values.length; i++) {
      assertEquals("Mismatch at " + i, Float.floatToRawIntBits(values[i]), Float.floatToRawIntBits(output[i]));
    }
  }

  @Test
  public void testFloatWriterExactVectorSize() throws IOException {
    float[] values = new float[AlpConstants.DEFAULT_VECTOR_SIZE];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.5f;
    }

    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }

    byte[] compressed = writer.getBytes().toByteArray();

    float[] output = new float[values.length];
    AlpWrapper.decodeFloats(compressed, compressed.length, output, values.length);

    for (int i = 0; i < values.length; i++) {
      assertEquals(Float.floatToRawIntBits(values[i]), Float.floatToRawIntBits(output[i]));
    }
  }

  @Test
  public void testFloatWriterSpecialValues() throws IOException {
    float[] values = {
      1.0f, Float.NaN, 2.0f, Float.POSITIVE_INFINITY, 3.0f, Float.NEGATIVE_INFINITY, 4.0f, -0.0f, 5.0f
    };

    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }

    byte[] compressed = writer.getBytes().toByteArray();

    float[] output = new float[values.length];
    AlpWrapper.decodeFloats(compressed, compressed.length, output, values.length);

    for (int i = 0; i < values.length; i++) {
      assertEquals("Mismatch at " + i, Float.floatToRawIntBits(values[i]), Float.floatToRawIntBits(output[i]));
    }
  }

  @Test
  public void testFloatWriterEmpty() throws IOException {
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    byte[] compressed = writer.getBytes().toByteArray();
    assertEquals(AlpConstants.HEADER_SIZE, compressed.length);
  }

  @Test
  public void testFloatWriterReset() throws IOException {
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (int i = 0; i < 100; i++) {
      writer.writeFloat(i * 0.1f);
    }

    byte[] first = writer.getBytes().toByteArray();
    assertTrue(first.length > AlpConstants.HEADER_SIZE);

    writer.reset();

    // Write different data
    for (int i = 0; i < 50; i++) {
      writer.writeFloat(i * 2.0f);
    }

    byte[] second = writer.getBytes().toByteArray();
    assertTrue(second.length > AlpConstants.HEADER_SIZE);

    // Verify second batch round-trips correctly
    float[] output = new float[50];
    AlpWrapper.decodeFloats(second, second.length, output, 50);
    for (int i = 0; i < 50; i++) {
      assertEquals(Float.floatToRawIntBits(i * 2.0f), Float.floatToRawIntBits(output[i]));
    }
  }

  // ========== Double writer ==========

  @Test
  public void testDoubleWriterRoundTrip() throws IOException {
    double[] values = new double[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i * 0.1;
    }

    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    for (double v : values) {
      writer.writeDouble(v);
    }

    byte[] compressed = writer.getBytes().toByteArray();

    double[] output = new double[values.length];
    AlpWrapper.decodeDoubles(compressed, compressed.length, output, values.length);

    for (int i = 0; i < values.length; i++) {
      assertEquals(
          "Mismatch at " + i, Double.doubleToRawLongBits(values[i]), Double.doubleToRawLongBits(output[i]));
    }
  }

  @Test
  public void testDoubleWriterMultipleVectors() throws IOException {
    double[] values = new double[2500];
    Random rng = new Random(42);
    for (int i = 0; i < 2500; i++) {
      values[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }

    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    for (double v : values) {
      writer.writeDouble(v);
    }

    byte[] compressed = writer.getBytes().toByteArray();

    double[] output = new double[values.length];
    AlpWrapper.decodeDoubles(compressed, compressed.length, output, values.length);

    for (int i = 0; i < values.length; i++) {
      assertEquals(
          "Mismatch at " + i, Double.doubleToRawLongBits(values[i]), Double.doubleToRawLongBits(output[i]));
    }
  }

  @Test
  public void testDoubleWriterSpecialValues() throws IOException {
    double[] values = {1.0, Double.NaN, 2.0, Double.POSITIVE_INFINITY, 3.0, Double.NEGATIVE_INFINITY, 4.0, -0.0, 5.0
    };

    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    for (double v : values) {
      writer.writeDouble(v);
    }

    byte[] compressed = writer.getBytes().toByteArray();

    double[] output = new double[values.length];
    AlpWrapper.decodeDoubles(compressed, compressed.length, output, values.length);

    for (int i = 0; i < values.length; i++) {
      assertEquals(
          "Mismatch at " + i, Double.doubleToRawLongBits(values[i]), Double.doubleToRawLongBits(output[i]));
    }
  }

  @Test
  public void testDoubleWriterEmpty() throws IOException {
    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    byte[] compressed = writer.getBytes().toByteArray();
    assertEquals(AlpConstants.HEADER_SIZE, compressed.length);
  }

  // ========== Buffered size / allocated size ==========

  @Test
  public void testBufferedSizeGrowsWithValues() {
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    long initial = writer.getBufferedSize();
    for (int i = 0; i < 10; i++) {
      writer.writeFloat(i * 0.1f);
    }
    assertTrue(writer.getBufferedSize() > initial);
  }

  @Test
  public void testAllocatedSizeNonNegative() {
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    assertTrue(writer.getAllocatedSize() > 0);
  }

  @Test
  public void testMemUsageString() {
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    String usage = writer.memUsageString("TEST");
    assertTrue(usage.startsWith("TEST"));
    assertTrue(usage.contains("ALP"));
  }
}
