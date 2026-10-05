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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class AlpPageCodecTest {

  private static void assertFloatPageRoundTrip(float[] input) {
    AlpCompression.AlpEncodingPreset preset = AlpPageCodec.createFloatSamplingPreset(input, input.length);
    byte[] compressed = new byte[(int) AlpPageCodec.maxCompressedSizeFloat(input.length)];
    int compressedSize = AlpPageCodec.encodeFloats(input, input.length, compressed, preset);
    assertThat(compressedSize).isPositive().isLessThanOrEqualTo(compressed.length);

    float[] output = new float[input.length];
    AlpPageCodec.decodeFloats(compressed, compressedSize, output, input.length);

    for (int i = 0; i < input.length; i++) {
      assertThat(Float.floatToRawIntBits(output[i]))
          .as("Mismatch at index " + i)
          .isEqualTo(Float.floatToRawIntBits(input[i]));
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
    int compressedSize = AlpPageCodec.encodeFloats(
        new float[0], 0, compressed, new AlpCompression.AlpEncodingPreset(new int[][] {{0, 0}}));
    assertThat(compressedSize).isEqualTo(AlpConstants.HEADER_SIZE);
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

  private static void assertDoublePageRoundTrip(double[] input) {
    AlpCompression.AlpEncodingPreset preset = AlpPageCodec.createDoubleSamplingPreset(input, input.length);
    byte[] compressed = new byte[(int) AlpPageCodec.maxCompressedSizeDouble(input.length)];
    int compressedSize = AlpPageCodec.encodeDoubles(input, input.length, compressed, preset);
    assertThat(compressedSize).isPositive();

    double[] output = new double[input.length];
    AlpPageCodec.decodeDoubles(compressed, compressedSize, output, input.length);

    for (int i = 0; i < input.length; i++) {
      assertThat(Double.doubleToRawLongBits(output[i]))
          .as("Mismatch at index " + i)
          .isEqualTo(Double.doubleToRawLongBits(input[i]));
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

  @Test
  public void testHeaderFormat() {
    float[] input = {1.0f, 2.0f, 3.0f};
    AlpCompression.AlpEncodingPreset preset = AlpPageCodec.createFloatSamplingPreset(input, input.length);
    byte[] compressed = new byte[(int) AlpPageCodec.maxCompressedSizeFloat(input.length)];
    int compressedSize = AlpPageCodec.encodeFloats(input, input.length, compressed, preset);

    ByteBuffer header =
        ByteBuffer.wrap(compressed, 0, AlpConstants.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    assertThat(header.get() & 0xFF).isEqualTo(AlpConstants.COMPRESSION_MODE_ALP);
    assertThat(header.get() & 0xFF).isEqualTo(AlpConstants.INTEGER_ENCODING_FOR);
    assertThat(header.get() & 0xFF).isEqualTo(AlpConstants.DEFAULT_VECTOR_SIZE_LOG);
    assertThat(header.getInt()).isEqualTo(3);
  }

  @Test
  public void testOffsetLayout() {
    float[] input = new float[2048];
    for (int i = 0; i < 2048; i++) {
      input[i] = i * 0.5f;
    }
    AlpCompression.AlpEncodingPreset preset = AlpPageCodec.createFloatSamplingPreset(input, input.length);
    byte[] compressed = new byte[(int) AlpPageCodec.maxCompressedSizeFloat(input.length)];
    AlpPageCodec.encodeFloats(input, input.length, compressed, preset);

    ByteBuffer body = ByteBuffer.wrap(
            compressed, AlpConstants.HEADER_SIZE, compressed.length - AlpConstants.HEADER_SIZE)
        .order(ByteOrder.LITTLE_ENDIAN);
    int offset0 = body.getInt();
    int offset1 = body.getInt();

    assertThat(offset0).isEqualTo(8);
    assertThat(offset1).isGreaterThan(offset0);
  }

  @Test
  public void testMaxCompressedSize() {
    assertThat(AlpPageCodec.maxCompressedSizeFloat(0)).isGreaterThanOrEqualTo(AlpConstants.HEADER_SIZE);
    assertThat(AlpPageCodec.maxCompressedSizeFloat(1024)).isGreaterThan(AlpConstants.HEADER_SIZE);
    assertThat(AlpPageCodec.maxCompressedSizeDouble(0)).isGreaterThanOrEqualTo(AlpConstants.HEADER_SIZE);
    assertThat(AlpPageCodec.maxCompressedSizeDouble(1024)).isGreaterThan(AlpConstants.HEADER_SIZE);
  }
}
