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

import org.junit.Test;

/**
 * Tests for the core ALP encoder/decoder logic.
 */
public class AlpEncoderDecoderTest {

  // ========== Float Encoding/Decoding Tests ==========

  @Test
  public void testFloatRoundTrip() {
    float[] testValues = {0.0f, 1.0f, -1.0f, 3.14159f, 100.5f, 0.001f, 1234567.0f};

    for (float value : testValues) {
      for (int exponent = 0; exponent <= AlpConstants.FLOAT_MAX_EXPONENT; exponent++) {
        for (int factor = 0; factor <= exponent; factor++) {
          if (!AlpEncoderDecoder.isFloatException(value, exponent, factor)) {
            int encoded = AlpEncoderDecoder.encodeFloat(value, exponent, factor);
            float decoded = AlpEncoderDecoder.decodeFloat(encoded, exponent, factor);
            assertEquals(
                "Round-trip failed for value=" + value + ", exponent=" + exponent + ", factor="
                    + factor,
                Float.floatToRawIntBits(value),
                Float.floatToRawIntBits(decoded));
          }
        }
      }
    }
  }

  @Test
  public void testFloatExceptionDetection() {
    assertTrue("NaN should be an exception", AlpEncoderDecoder.isFloatException(Float.NaN));
    assertTrue(
        "Positive infinity should be an exception",
        AlpEncoderDecoder.isFloatException(Float.POSITIVE_INFINITY));
    assertTrue(
        "Negative infinity should be an exception",
        AlpEncoderDecoder.isFloatException(Float.NEGATIVE_INFINITY));
    assertTrue("Negative zero should be an exception", AlpEncoderDecoder.isFloatException(-0.0f));

    assertFalse("1.0f should not be a basic exception", AlpEncoderDecoder.isFloatException(1.0f));
    assertFalse("0.0f should not be a basic exception", AlpEncoderDecoder.isFloatException(0.0f));
  }

  @Test
  public void testFloatEncoding() {
    assertEquals(123, AlpEncoderDecoder.encodeFloat(1.23f, 2, 0));
    assertEquals(123, AlpEncoderDecoder.encodeFloat(12.3f, 2, 1));
    assertEquals(0, AlpEncoderDecoder.encodeFloat(0.0f, 5, 0));
  }

  @Test
  public void testFloatDecoding() {
    assertEquals(1.23f, AlpEncoderDecoder.decodeFloat(123, 2, 0), 1e-6f);
    assertEquals(12.3f, AlpEncoderDecoder.decodeFloat(123, 2, 1), 1e-6f);
    assertEquals(0.0f, AlpEncoderDecoder.decodeFloat(0, 5, 0), 0.0f);
  }

  @Test
  public void testFastRoundFloat() {
    assertEquals(5, AlpEncoderDecoder.fastRoundFloat(5.4f));
    assertEquals(6, AlpEncoderDecoder.fastRoundFloat(5.5f));
    assertEquals(6, AlpEncoderDecoder.fastRoundFloat(5.6f));
    assertEquals(-5, AlpEncoderDecoder.fastRoundFloat(-5.4f));
    assertEquals(-6, AlpEncoderDecoder.fastRoundFloat(-5.5f));
    assertEquals(-6, AlpEncoderDecoder.fastRoundFloat(-5.6f));
    assertEquals(0, AlpEncoderDecoder.fastRoundFloat(0.0f));
  }

  @Test
  public void testFloatMultiplier() {
    assertEquals(1.0f, AlpEncoderDecoder.getFloatMultiplier(0, 0), 0.0f);
    assertEquals(100.0f, AlpEncoderDecoder.getFloatMultiplier(2, 0), 0.0f);
    assertEquals(10.0f, AlpEncoderDecoder.getFloatMultiplier(2, 1), 1e-6f);
    assertEquals(1.0f, AlpEncoderDecoder.getFloatMultiplier(2, 2), 1e-6f);
  }

  // ========== Double Encoding/Decoding Tests ==========

  @Test
  public void testDoubleRoundTrip() {
    double[] testValues = {0.0, 1.0, -1.0, 3.14159265358979, 100.5, 0.001, 12345678901234.0};

    for (double value : testValues) {
      for (int exponent = 0; exponent <= Math.min(AlpConstants.DOUBLE_MAX_EXPONENT, 10); exponent++) {
        for (int factor = 0; factor <= exponent; factor++) {
          if (!AlpEncoderDecoder.isDoubleException(value, exponent, factor)) {
            long encoded = AlpEncoderDecoder.encodeDouble(value, exponent, factor);
            double decoded = AlpEncoderDecoder.decodeDouble(encoded, exponent, factor);
            assertEquals(
                "Round-trip failed for value=" + value + ", exponent=" + exponent + ", factor="
                    + factor,
                Double.doubleToRawLongBits(value),
                Double.doubleToRawLongBits(decoded));
          }
        }
      }
    }
  }

  @Test
  public void testDoubleExceptionDetection() {
    assertTrue("NaN should be an exception", AlpEncoderDecoder.isDoubleException(Double.NaN));
    assertTrue(
        "Positive infinity should be an exception",
        AlpEncoderDecoder.isDoubleException(Double.POSITIVE_INFINITY));
    assertTrue(
        "Negative infinity should be an exception",
        AlpEncoderDecoder.isDoubleException(Double.NEGATIVE_INFINITY));
    assertTrue("Negative zero should be an exception", AlpEncoderDecoder.isDoubleException(-0.0));

    assertFalse("1.0 should not be a basic exception", AlpEncoderDecoder.isDoubleException(1.0));
    assertFalse("0.0 should not be a basic exception", AlpEncoderDecoder.isDoubleException(0.0));
  }

  @Test
  public void testDoubleEncoding() {
    assertEquals(123L, AlpEncoderDecoder.encodeDouble(1.23, 2, 0));
    assertEquals(123L, AlpEncoderDecoder.encodeDouble(12.3, 2, 1));
    assertEquals(0L, AlpEncoderDecoder.encodeDouble(0.0, 5, 0));
  }

  @Test
  public void testDoubleDecoding() {
    assertEquals(1.23, AlpEncoderDecoder.decodeDouble(123, 2, 0), 1e-10);
    assertEquals(12.3, AlpEncoderDecoder.decodeDouble(123, 2, 1), 1e-10);
    assertEquals(0.0, AlpEncoderDecoder.decodeDouble(0, 5, 0), 0.0);
  }

  @Test
  public void testFastRoundDouble() {
    assertEquals(5L, AlpEncoderDecoder.fastRoundDouble(5.4));
    assertEquals(6L, AlpEncoderDecoder.fastRoundDouble(5.5));
    assertEquals(6L, AlpEncoderDecoder.fastRoundDouble(5.6));
    assertEquals(-5L, AlpEncoderDecoder.fastRoundDouble(-5.4));
    assertEquals(-6L, AlpEncoderDecoder.fastRoundDouble(-5.5));
    assertEquals(-6L, AlpEncoderDecoder.fastRoundDouble(-5.6));
    assertEquals(0L, AlpEncoderDecoder.fastRoundDouble(0.0));
  }

  @Test
  public void testDoubleMultiplier() {
    assertEquals(1.0, AlpEncoderDecoder.getDoubleMultiplier(0, 0), 0.0);
    assertEquals(100.0, AlpEncoderDecoder.getDoubleMultiplier(2, 0), 0.0);
    assertEquals(10.0, AlpEncoderDecoder.getDoubleMultiplier(2, 1), 1e-10);
    assertEquals(1.0, AlpEncoderDecoder.getDoubleMultiplier(2, 2), 1e-10);
  }

  // ========== Bit Width Tests (renamed methods) ==========

  @Test
  public void testBitWidthForInt() {
    assertEquals(0, AlpEncoderDecoder.bitWidthForInt(0));
    assertEquals(1, AlpEncoderDecoder.bitWidthForInt(1));
    assertEquals(2, AlpEncoderDecoder.bitWidthForInt(2));
    assertEquals(2, AlpEncoderDecoder.bitWidthForInt(3));
    assertEquals(3, AlpEncoderDecoder.bitWidthForInt(4));
    assertEquals(8, AlpEncoderDecoder.bitWidthForInt(255));
    assertEquals(9, AlpEncoderDecoder.bitWidthForInt(256));
    assertEquals(16, AlpEncoderDecoder.bitWidthForInt(65535));
    assertEquals(31, AlpEncoderDecoder.bitWidthForInt(Integer.MAX_VALUE));
  }

  @Test
  public void testBitWidthForLong() {
    assertEquals(0, AlpEncoderDecoder.bitWidthForLong(0L));
    assertEquals(1, AlpEncoderDecoder.bitWidthForLong(1L));
    assertEquals(2, AlpEncoderDecoder.bitWidthForLong(2L));
    assertEquals(2, AlpEncoderDecoder.bitWidthForLong(3L));
    assertEquals(3, AlpEncoderDecoder.bitWidthForLong(4L));
    assertEquals(8, AlpEncoderDecoder.bitWidthForLong(255L));
    assertEquals(9, AlpEncoderDecoder.bitWidthForLong(256L));
    assertEquals(16, AlpEncoderDecoder.bitWidthForLong(65535L));
    assertEquals(31, AlpEncoderDecoder.bitWidthForLong((long) Integer.MAX_VALUE));
    assertEquals(63, AlpEncoderDecoder.bitWidthForLong(Long.MAX_VALUE));
  }

  // ========== Best Parameters Tests ==========

  @Test
  public void testFindBestFloatParams() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    assertNotNull(params);
    assertTrue(params.exponent >= 0 && params.exponent <= AlpConstants.FLOAT_MAX_EXPONENT);
    assertTrue(params.factor >= 0 && params.factor <= params.exponent);

    for (float v : values) {
      if (!AlpEncoderDecoder.isFloatException(v, params.exponent, params.factor)) {
        int encoded = AlpEncoderDecoder.encodeFloat(v, params.exponent, params.factor);
        float decoded = AlpEncoderDecoder.decodeFloat(encoded, params.exponent, params.factor);
        assertEquals(Float.floatToRawIntBits(v), Float.floatToRawIntBits(decoded));
      }
    }
  }

  @Test
  public void testFindBestFloatParamsWithAllExceptions() {
    float[] values = {Float.NaN, Float.NaN, Float.NaN};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    assertNotNull(params);
    assertEquals(values.length, params.numExceptions);
  }

  @Test
  public void testFindBestDoubleParams() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);

    assertNotNull(params);
    assertTrue(params.exponent >= 0 && params.exponent <= AlpConstants.DOUBLE_MAX_EXPONENT);
    assertTrue(params.factor >= 0 && params.factor <= params.exponent);

    for (double v : values) {
      if (!AlpEncoderDecoder.isDoubleException(v, params.exponent, params.factor)) {
        long encoded = AlpEncoderDecoder.encodeDouble(v, params.exponent, params.factor);
        double decoded = AlpEncoderDecoder.decodeDouble(encoded, params.exponent, params.factor);
        assertEquals(Double.doubleToRawLongBits(v), Double.doubleToRawLongBits(decoded));
      }
    }
  }

  @Test
  public void testFindBestDoubleParamsWithAllExceptions() {
    double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);

    assertNotNull(params);
    assertEquals(values.length, params.numExceptions);
  }

  @Test
  public void testFindBestParamsWithOffset() {
    float[] values = {Float.NaN, Float.NaN, 1.23f, 4.56f, 7.89f, Float.NaN};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 2, 3);

    assertNotNull(params);
    assertEquals(0, params.numExceptions);
  }

  // ========== Preset-Based Parameter Search Tests ==========

  @Test
  public void testFindBestFloatParamsWithPresets() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    int[][] presets = {{2, 0}, {3, 0}, {4, 1}};
    AlpEncoderDecoder.EncodingParams params =
        AlpEncoderDecoder.findBestFloatParamsWithPresets(values, 0, values.length, presets);

    assertNotNull(params);
    // Should select one of the preset combinations
    boolean foundMatch = false;
    for (int[] preset : presets) {
      if (params.exponent == preset[0] && params.factor == preset[1]) {
        foundMatch = true;
        break;
      }
    }
    assertTrue("Result should be one of the preset combinations", foundMatch);
  }

  @Test
  public void testFindBestDoubleParamsWithPresets() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    int[][] presets = {{2, 0}, {3, 0}, {4, 1}};
    AlpEncoderDecoder.EncodingParams params =
        AlpEncoderDecoder.findBestDoubleParamsWithPresets(values, 0, values.length, presets);

    assertNotNull(params);
    boolean foundMatch = false;
    for (int[] preset : presets) {
      if (params.exponent == preset[0] && params.factor == preset[1]) {
        foundMatch = true;
        break;
      }
    }
    assertTrue("Result should be one of the preset combinations", foundMatch);
  }

  @Test
  public void testPresetsProduceSameResultAsFullSearch() {
    float[] values = {1.23f, 4.56f, 7.89f};
    AlpEncoderDecoder.EncodingParams fullResult = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    // Include the best params in presets
    int[][] presets = {{fullResult.exponent, fullResult.factor}, {0, 0}, {1, 0}};
    AlpEncoderDecoder.EncodingParams presetResult =
        AlpEncoderDecoder.findBestFloatParamsWithPresets(values, 0, values.length, presets);

    assertTrue(
        "Preset result should be at least as good as full search",
        presetResult.numExceptions <= fullResult.numExceptions);
  }
}
