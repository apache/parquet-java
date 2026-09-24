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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.junit.Test;

public class AlpWrapperTest {

  // ========== Float round-trip ==========

  private static void assertFloatPageRoundTrip(float[] input) {
    AlpCompression.AlpEncodingPreset preset = AlpWrapper.createFloatSamplingPreset(input, input.length);
    byte[] compressed = new byte[(int) AlpWrapper.maxCompressedSizeFloat(input.length)];
    int compSize = AlpWrapper.encodeFloats(input, input.length, compressed, preset);
    assertTrue(compSize > 0);
    assertTrue(compSize <= compressed.length);

    float[] output = new float[input.length];
    AlpWrapper.decodeFloats(compressed, compSize, output, input.length);

    for (int i = 0; i < input.length; i++) {
      assertEquals(
          "Mismatch at index " + i, Float.floatToRawIntBits(input[i]), Float.floatToRawIntBits(output[i]));
    }
  }

  @Test
  public void testFloatSingleVector() {
    float[] input = new float[100];
    for (int i = 0; i < 100; i++) {
      input[i] = i * 0.1f;
    }
    assertFloatPageRoundTrip(input);
  }

  @Test
  public void testFloatMultipleVectors() {
    // 2500 values = 2 full vectors (1024) + 1 partial (452)
    float[] input = new float[2500];
    for (int i = 0; i < 2500; i++) {
      input[i] = i * 0.01f;
    }
    assertFloatPageRoundTrip(input);
  }

  @Test
  public void testFloatExactVectorSize() {
    float[] input = new float[AlpConstants.DEFAULT_VECTOR_SIZE];
    for (int i = 0; i < input.length; i++) {
      input[i] = i * 0.5f;
    }
    assertFloatPageRoundTrip(input);
  }

  @Test
  public void testFloatExactTwoVectors() {
    float[] input = new float[2 * AlpConstants.DEFAULT_VECTOR_SIZE];
    for (int i = 0; i < input.length; i++) {
      input[i] = (i % 100) * 0.3f;
    }
    assertFloatPageRoundTrip(input);
  }

  @Test
  public void testFloatSpecialValues() {
    float[] input = new float[20];
    for (int i = 0; i < 20; i++) {
      input[i] = i * 1.5f;
    }
    input[3] = Float.NaN;
    input[7] = Float.POSITIVE_INFINITY;
    input[11] = Float.NEGATIVE_INFINITY;
    input[15] = -0.0f;
    assertFloatPageRoundTrip(input);
  }

  @Test
  public void testFloatEmptyInput() {
    byte[] compressed = new byte[AlpConstants.HEADER_SIZE];
    int compSize = AlpWrapper.encodeFloats(
        new float[0], 0, compressed, new AlpCompression.AlpEncodingPreset(new int[][] {{0, 0}}));
    assertEquals(AlpConstants.HEADER_SIZE, compSize);
  }

  @Test
  public void testFloatRandomLargeDataset() {
    Random rng = new Random(42);
    float[] input = new float[5000];
    for (int i = 0; i < 5000; i++) {
      input[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }
    assertFloatPageRoundTrip(input);
  }

  // ========== Double round-trip ==========

  private static void assertDoublePageRoundTrip(double[] input) {
    AlpCompression.AlpEncodingPreset preset = AlpWrapper.createDoubleSamplingPreset(input, input.length);
    byte[] compressed = new byte[(int) AlpWrapper.maxCompressedSizeDouble(input.length)];
    int compSize = AlpWrapper.encodeDoubles(input, input.length, compressed, preset);
    assertTrue(compSize > 0);

    double[] output = new double[input.length];
    AlpWrapper.decodeDoubles(compressed, compSize, output, input.length);

    for (int i = 0; i < input.length; i++) {
      assertEquals(
          "Mismatch at index " + i,
          Double.doubleToRawLongBits(input[i]),
          Double.doubleToRawLongBits(output[i]));
    }
  }

  @Test
  public void testDoubleSingleVector() {
    double[] input = new double[100];
    for (int i = 0; i < 100; i++) {
      input[i] = i * 0.1;
    }
    assertDoublePageRoundTrip(input);
  }

  @Test
  public void testDoubleMultipleVectors() {
    double[] input = new double[2500];
    for (int i = 0; i < 2500; i++) {
      input[i] = i * 0.01;
    }
    assertDoublePageRoundTrip(input);
  }

  @Test
  public void testDoubleSpecialValues() {
    double[] input = new double[20];
    for (int i = 0; i < 20; i++) {
      input[i] = i * 1.5;
    }
    input[3] = Double.NaN;
    input[7] = Double.POSITIVE_INFINITY;
    input[11] = Double.NEGATIVE_INFINITY;
    input[15] = -0.0;
    assertDoublePageRoundTrip(input);
  }

  @Test
  public void testDoubleRandomLargeDataset() {
    Random rng = new Random(42);
    double[] input = new double[5000];
    for (int i = 0; i < 5000; i++) {
      input[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }
    assertDoublePageRoundTrip(input);
  }

  // ========== Wire format verification ==========

  @Test
  public void testHeaderFormat() {
    float[] input = {1.0f, 2.0f, 3.0f};
    AlpCompression.AlpEncodingPreset preset = AlpWrapper.createFloatSamplingPreset(input, input.length);
    byte[] compressed = new byte[(int) AlpWrapper.maxCompressedSizeFloat(input.length)];
    int compSize = AlpWrapper.encodeFloats(input, input.length, compressed, preset);

    // Verify 7-byte header
    ByteBuffer header =
        ByteBuffer.wrap(compressed, 0, AlpConstants.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(AlpConstants.COMPRESSION_MODE_ALP, header.get() & 0xFF);
    assertEquals(AlpConstants.INTEGER_ENCODING_FOR, header.get() & 0xFF);
    assertEquals(AlpConstants.DEFAULT_VECTOR_SIZE_LOG, header.get() & 0xFF);
    assertEquals(3, header.getInt()); // num_elements
  }

  @Test
  public void testOffsetLayout() {
    // 2048 elements = 2 vectors
    float[] input = new float[2048];
    for (int i = 0; i < 2048; i++) {
      input[i] = i * 0.5f;
    }
    AlpCompression.AlpEncodingPreset preset = AlpWrapper.createFloatSamplingPreset(input, input.length);
    byte[] compressed = new byte[(int) AlpWrapper.maxCompressedSizeFloat(input.length)];
    AlpWrapper.encodeFloats(input, input.length, compressed, preset);

    // After header (7B), offsets section should have 2 int offsets (8 bytes)
    ByteBuffer body = ByteBuffer.wrap(
            compressed, AlpConstants.HEADER_SIZE, compressed.length - AlpConstants.HEADER_SIZE)
        .order(ByteOrder.LITTLE_ENDIAN);
    int offset0 = body.getInt();
    int offset1 = body.getInt();

    // First vector starts right after offsets (2 * 4 = 8)
    assertEquals(8, offset0);
    // Second vector starts after first vector's data
    assertTrue(offset1 > offset0);
  }

  // ========== Max compressed size ==========

  @Test
  public void testMaxCompressedSize() {
    assertTrue(AlpWrapper.maxCompressedSizeFloat(0) >= AlpConstants.HEADER_SIZE);
    assertTrue(AlpWrapper.maxCompressedSizeFloat(1024) > AlpConstants.HEADER_SIZE);
    assertTrue(AlpWrapper.maxCompressedSizeDouble(0) >= AlpConstants.HEADER_SIZE);
    assertTrue(AlpWrapper.maxCompressedSizeDouble(1024) > AlpConstants.HEADER_SIZE);
  }
}
