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
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.junit.Test;

public class AlpValuesReaderTest {

  private static ByteBufferInputStream toInputStream(byte[] data) {
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(data));
  }

  // ========== Float writer → reader round-trip ==========

  private static void assertFloatWriterReaderRoundTrip(float[] values) throws IOException {
    // Write
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }
    byte[] compressed = writer.getBytes().toByteArray();

    // Read
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(values.length, toInputStream(compressed));

    for (int i = 0; i < values.length; i++) {
      float actual = reader.readFloat();
      assertEquals(
          "Mismatch at " + i,
          Float.floatToRawIntBits(values[i]),
          Float.floatToRawIntBits(actual));
    }
  }

  @Test
  public void testFloatReaderSingleVector() throws IOException {
    float[] values = new float[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i * 0.1f;
    }
    assertFloatWriterReaderRoundTrip(values);
  }

  @Test
  public void testFloatReaderMultipleVectors() throws IOException {
    float[] values = new float[2500];
    Random rng = new Random(42);
    for (int i = 0; i < 2500; i++) {
      values[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }
    assertFloatWriterReaderRoundTrip(values);
  }

  @Test
  public void testFloatReaderExactVectorSize() throws IOException {
    float[] values = new float[AlpConstants.DEFAULT_VECTOR_SIZE];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.5f;
    }
    assertFloatWriterReaderRoundTrip(values);
  }

  @Test
  public void testFloatReaderSpecialValues() throws IOException {
    float[] values = {1.0f, Float.NaN, 2.0f, Float.POSITIVE_INFINITY,
        3.0f, Float.NEGATIVE_INFINITY, 4.0f, -0.0f, 5.0f};
    assertFloatWriterReaderRoundTrip(values);
  }

  // ========== Float skip ==========

  @Test
  public void testFloatReaderSkip() throws IOException {
    float[] values = new float[50];
    for (int i = 0; i < 50; i++) {
      values[i] = i * 0.3f;
    }

    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }
    byte[] compressed = writer.getBytes().toByteArray();

    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(values.length, toInputStream(compressed));

    // Skip first 10, read next 5
    reader.skip(10);
    for (int i = 10; i < 15; i++) {
      assertEquals(
          Float.floatToRawIntBits(values[i]),
          Float.floatToRawIntBits(reader.readFloat()));
    }

    // Skip 20 more, read next
    reader.skip(20);
    assertEquals(
        Float.floatToRawIntBits(values[35]),
        Float.floatToRawIntBits(reader.readFloat()));
  }

  @Test
  public void testFloatReaderSkipAcrossVectors() throws IOException {
    float[] values = new float[2500];
    for (int i = 0; i < 2500; i++) {
      values[i] = i * 0.01f;
    }

    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }
    byte[] compressed = writer.getBytes().toByteArray();

    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(values.length, toInputStream(compressed));

    // Skip into second vector
    reader.skip(1500);
    assertEquals(
        Float.floatToRawIntBits(values[1500]),
        Float.floatToRawIntBits(reader.readFloat()));
  }

  // ========== Double writer → reader round-trip ==========

  private static void assertDoubleWriterReaderRoundTrip(double[] values) throws IOException {
    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    for (double v : values) {
      writer.writeDouble(v);
    }
    byte[] compressed = writer.getBytes().toByteArray();

    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(values.length, toInputStream(compressed));

    for (int i = 0; i < values.length; i++) {
      double actual = reader.readDouble();
      assertEquals(
          "Mismatch at " + i,
          Double.doubleToRawLongBits(values[i]),
          Double.doubleToRawLongBits(actual));
    }
  }

  @Test
  public void testDoubleReaderSingleVector() throws IOException {
    double[] values = new double[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i * 0.1;
    }
    assertDoubleWriterReaderRoundTrip(values);
  }

  @Test
  public void testDoubleReaderMultipleVectors() throws IOException {
    double[] values = new double[2500];
    Random rng = new Random(42);
    for (int i = 0; i < 2500; i++) {
      values[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }
    assertDoubleWriterReaderRoundTrip(values);
  }

  @Test
  public void testDoubleReaderSpecialValues() throws IOException {
    double[] values = {1.0, Double.NaN, 2.0, Double.POSITIVE_INFINITY,
        3.0, Double.NEGATIVE_INFINITY, 4.0, -0.0, 5.0};
    assertDoubleWriterReaderRoundTrip(values);
  }

  @Test
  public void testDoubleReaderSkip() throws IOException {
    double[] values = new double[50];
    for (int i = 0; i < 50; i++) {
      values[i] = i * 0.3;
    }

    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    for (double v : values) {
      writer.writeDouble(v);
    }
    byte[] compressed = writer.getBytes().toByteArray();

    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(values.length, toInputStream(compressed));

    reader.skip(10);
    for (int i = 10; i < 15; i++) {
      assertEquals(
          Double.doubleToRawLongBits(values[i]),
          Double.doubleToRawLongBits(reader.readDouble()));
    }
  }

  // ========== Partial last vector ==========

  @Test
  public void testFloatReaderPartialLastVector() throws IOException {
    // 1030 = 1024 + 6 → 2 vectors, last has 6 elements
    float[] values = new float[1030];
    for (int i = 0; i < 1030; i++) {
      values[i] = i * 0.1f;
    }
    assertFloatWriterReaderRoundTrip(values);
  }

  @Test
  public void testDoubleReaderPartialLastVector() throws IOException {
    double[] values = new double[1030];
    for (int i = 0; i < 1030; i++) {
      values[i] = i * 0.1;
    }
    assertDoubleWriterReaderRoundTrip(values);
  }
}
