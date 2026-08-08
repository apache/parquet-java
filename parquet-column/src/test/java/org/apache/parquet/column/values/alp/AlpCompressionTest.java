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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;

public class AlpCompressionTest {

  // ========== Helpers ==========

  private static AlpCompression.AlpEncodingPreset allFloatCombos() {
    List<int[]> combos = new ArrayList<>();
    for (int e = 0; e <= AlpConstants.FLOAT_MAX_EXPONENT; e++) {
      for (int f = 0; f <= e; f++) {
        combos.add(new int[] {e, f});
      }
    }
    return new AlpCompression.AlpEncodingPreset(combos.toArray(new int[0][]));
  }

  private static AlpCompression.AlpEncodingPreset allDoubleCombos() {
    List<int[]> combos = new ArrayList<>();
    for (int e = 0; e <= AlpConstants.DOUBLE_MAX_EXPONENT; e++) {
      for (int f = 0; f <= e; f++) {
        combos.add(new int[] {e, f});
      }
    }
    return new AlpCompression.AlpEncodingPreset(combos.toArray(new int[0][]));
  }

  private static void assertFloatRoundTrip(float[] input) {
    AlpCompression.AlpEncodingPreset preset = allFloatCombos();
    AlpCompression.FloatCompressedVector cv = AlpCompression.compressFloatVector(input, input.length, preset);
    float[] output = new float[input.length];
    AlpCompression.decompressFloatVector(cv, output);
    for (int i = 0; i < input.length; i++) {
      assertEquals(
          "Mismatch at index " + i, Float.floatToRawIntBits(input[i]), Float.floatToRawIntBits(output[i]));
    }
  }

  private static void assertDoubleRoundTrip(double[] input) {
    AlpCompression.AlpEncodingPreset preset = allDoubleCombos();
    AlpCompression.DoubleCompressedVector cv = AlpCompression.compressDoubleVector(input, input.length, preset);
    double[] output = new double[input.length];
    AlpCompression.decompressDoubleVector(cv, output);
    for (int i = 0; i < input.length; i++) {
      assertEquals(
          "Mismatch at index " + i,
          Double.doubleToRawLongBits(input[i]),
          Double.doubleToRawLongBits(output[i]));
    }
  }

  // ========== Float compress/decompress ==========

  @Test
  public void testFloatConstantValues() {
    float[] input = new float[100];
    for (int i = 0; i < 100; i++) {
      input[i] = 3.14f;
    }
    assertFloatRoundTrip(input);

    // Constant → bitWidth should be 0
    AlpCompression.FloatCompressedVector cv =
        AlpCompression.compressFloatVector(input, input.length, allFloatCombos());
    assertEquals(0, cv.bitWidth);
  }

  @Test
  public void testFloatDecimalValues() {
    float[] input = new float[100];
    for (int i = 0; i < 100; i++) {
      input[i] = i * 0.1f;
    }
    assertFloatRoundTrip(input);
  }

  @Test
  public void testFloatIntegerValues() {
    float[] input = new float[100];
    for (int i = 0; i < 100; i++) {
      input[i] = i;
    }
    assertFloatRoundTrip(input);
  }

  @Test
  public void testFloatRandomValues() {
    Random rng = new Random(42);
    float[] input = new float[200];
    for (int i = 0; i < 200; i++) {
      input[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }
    assertFloatRoundTrip(input);
  }

  @Test
  public void testFloatSpecialValues() {
    float[] input = {
      1.0f, Float.NaN, 2.0f, Float.POSITIVE_INFINITY, 3.0f, Float.NEGATIVE_INFINITY, 4.0f, -0.0f, 5.0f
    };
    assertFloatRoundTrip(input);

    AlpCompression.FloatCompressedVector cv =
        AlpCompression.compressFloatVector(input, input.length, allFloatCombos());
    // NaN, +Inf, -Inf, -0.0 should be exceptions
    assertTrue(cv.numExceptions >= 4);
  }

  @Test
  public void testFloatSingleElement() {
    assertFloatRoundTrip(new float[] {42.5f});
  }

  @Test
  public void testFloatEmptyVector() {
    AlpCompression.FloatCompressedVector cv = AlpCompression.compressFloatVector(new float[0], 0, allFloatCombos());
    assertEquals(0, cv.numElements);
    assertEquals(0, cv.numExceptions);
  }

  @Test
  public void testFloatAllExceptions() {
    float[] input = new float[16];
    for (int i = 0; i < 16; i++) {
      input[i] = Float.NaN;
    }
    assertFloatRoundTrip(input);

    AlpCompression.FloatCompressedVector cv =
        AlpCompression.compressFloatVector(input, input.length, allFloatCombos());
    assertEquals(16, cv.numExceptions);
    assertEquals(0, cv.bitWidth);
  }

  @Test
  public void testFloatExactVectorSize() {
    float[] input = new float[AlpConstants.DEFAULT_VECTOR_SIZE];
    for (int i = 0; i < input.length; i++) {
      input[i] = i * 0.01f;
    }
    assertFloatRoundTrip(input);
  }

  @Test
  public void testFloatNonMultipleOf8() {
    // 13 elements — tests tail handling in pack/unpack
    float[] input = new float[13];
    for (int i = 0; i < 13; i++) {
      input[i] = i * 1.5f;
    }
    assertFloatRoundTrip(input);
  }

  // ========== Float store/load ==========

  @Test
  public void testFloatStoreLoadRoundTrip() {
    float[] input = new float[50];
    for (int i = 0; i < 50; i++) {
      input[i] = i * 0.3f;
    }

    AlpCompression.FloatCompressedVector cv =
        AlpCompression.compressFloatVector(input, input.length, allFloatCombos());

    byte[] buf = new byte[cv.storedSize()];
    cv.store(buf, 0);

    AlpCompression.FloatCompressedVector loaded = AlpCompression.FloatCompressedVector.load(buf, 0, input.length);

    float[] output = new float[input.length];
    AlpCompression.decompressFloatVector(loaded, output);

    for (int i = 0; i < input.length; i++) {
      assertEquals(
          "Mismatch at index " + i, Float.floatToRawIntBits(input[i]), Float.floatToRawIntBits(output[i]));
    }
  }

  @Test
  public void testFloatStoreLoadMetadata() {
    float[] input = {1.1f, 2.2f, 3.3f, Float.NaN, 5.5f};
    AlpCompression.FloatCompressedVector cv =
        AlpCompression.compressFloatVector(input, input.length, allFloatCombos());

    byte[] buf = new byte[cv.storedSize()];
    cv.store(buf, 0);

    AlpCompression.FloatCompressedVector loaded = AlpCompression.FloatCompressedVector.load(buf, 0, input.length);

    assertEquals(cv.exponent, loaded.exponent);
    assertEquals(cv.factor, loaded.factor);
    assertEquals(cv.numExceptions, loaded.numExceptions);
    assertEquals(cv.frameOfReference, loaded.frameOfReference);
    assertEquals(cv.bitWidth, loaded.bitWidth);
    assertEquals(cv.numElements, loaded.numElements);
  }

  // ========== Double compress/decompress ==========

  @Test
  public void testDoubleConstantValues() {
    double[] input = new double[100];
    for (int i = 0; i < 100; i++) {
      input[i] = 3.14;
    }
    assertDoubleRoundTrip(input);

    AlpCompression.DoubleCompressedVector cv =
        AlpCompression.compressDoubleVector(input, input.length, allDoubleCombos());
    assertEquals(0, cv.bitWidth);
  }

  @Test
  public void testDoubleDecimalValues() {
    double[] input = new double[100];
    for (int i = 0; i < 100; i++) {
      input[i] = i * 0.01;
    }
    assertDoubleRoundTrip(input);
  }

  @Test
  public void testDoubleRandomValues() {
    Random rng = new Random(42);
    double[] input = new double[200];
    for (int i = 0; i < 200; i++) {
      input[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }
    assertDoubleRoundTrip(input);
  }

  @Test
  public void testDoubleSpecialValues() {
    double[] input = {1.0, Double.NaN, 2.0, Double.POSITIVE_INFINITY, 3.0, Double.NEGATIVE_INFINITY, 4.0, -0.0, 5.0
    };
    assertDoubleRoundTrip(input);

    AlpCompression.DoubleCompressedVector cv =
        AlpCompression.compressDoubleVector(input, input.length, allDoubleCombos());
    assertTrue(cv.numExceptions >= 4);
  }

  @Test
  public void testDoubleSingleElement() {
    assertDoubleRoundTrip(new double[] {42.5});
  }

  @Test
  public void testDoubleAllExceptions() {
    double[] input = new double[16];
    for (int i = 0; i < 16; i++) {
      input[i] = Double.NaN;
    }
    assertDoubleRoundTrip(input);

    AlpCompression.DoubleCompressedVector cv =
        AlpCompression.compressDoubleVector(input, input.length, allDoubleCombos());
    assertEquals(16, cv.numExceptions);
  }

  @Test
  public void testDoubleNonMultipleOf8() {
    double[] input = new double[13];
    for (int i = 0; i < 13; i++) {
      input[i] = i * 1.5;
    }
    assertDoubleRoundTrip(input);
  }

  // ========== Double store/load ==========

  @Test
  public void testDoubleStoreLoadRoundTrip() {
    double[] input = new double[50];
    for (int i = 0; i < 50; i++) {
      input[i] = i * 0.3;
    }

    AlpCompression.DoubleCompressedVector cv =
        AlpCompression.compressDoubleVector(input, input.length, allDoubleCombos());

    byte[] buf = new byte[cv.storedSize()];
    cv.store(buf, 0);

    AlpCompression.DoubleCompressedVector loaded = AlpCompression.DoubleCompressedVector.load(buf, 0, input.length);

    double[] output = new double[input.length];
    AlpCompression.decompressDoubleVector(loaded, output);

    for (int i = 0; i < input.length; i++) {
      assertEquals(
          "Mismatch at index " + i,
          Double.doubleToRawLongBits(input[i]),
          Double.doubleToRawLongBits(output[i]));
    }
  }

  // ========== Bit packing helpers ==========

  @Test
  public void testPackUnpackInts() {
    int[] values = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    int bitWidth = 4;
    byte[] packed = new byte[AlpEncoderDecoder.bitPackedSize(values.length, bitWidth)];
    AlpCompression.packInts(values, values.length, bitWidth, packed);

    int[] unpacked = new int[values.length];
    AlpCompression.unpackInts(packed, values.length, bitWidth, unpacked);
    assertArrayEquals(values, unpacked);
  }

  @Test
  public void testPackUnpackIntsNonMultipleOf8() {
    int[] values = {5, 10, 15, 20, 25};
    int bitWidth = 5;
    byte[] packed = new byte[AlpEncoderDecoder.bitPackedSize(values.length, bitWidth)];
    AlpCompression.packInts(values, values.length, bitWidth, packed);

    int[] unpacked = new int[values.length];
    AlpCompression.unpackInts(packed, values.length, bitWidth, unpacked);
    assertArrayEquals(values, unpacked);
  }

  @Test
  public void testPackUnpackLongs() {
    long[] values = {0, 1, 2, 3, 4, 5, 6, 7, 100, 200, 300, 400, 500, 600, 700, 800};
    int bitWidth = 10;
    byte[] packed = new byte[AlpEncoderDecoder.bitPackedSize(values.length, bitWidth)];
    AlpCompression.packLongs(values, values.length, bitWidth, packed);

    long[] unpacked = new long[values.length];
    AlpCompression.unpackLongs(packed, values.length, bitWidth, unpacked);
    assertArrayEquals(values, unpacked);
  }

  @Test
  public void testPackUnpackLongsNonMultipleOf8() {
    long[] values = {10, 20, 30};
    int bitWidth = 6;
    byte[] packed = new byte[AlpEncoderDecoder.bitPackedSize(values.length, bitWidth)];
    AlpCompression.packLongs(values, values.length, bitWidth, packed);

    long[] unpacked = new long[values.length];
    AlpCompression.unpackLongs(packed, values.length, bitWidth, unpacked);
    assertArrayEquals(values, unpacked);
  }
}
