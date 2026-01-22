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
    // NaN should be an exception
    assertTrue("NaN should be an exception", AlpEncoderDecoder.isFloatException(Float.NaN));

    // Infinity should be an exception
    assertTrue(
        "Positive infinity should be an exception",
        AlpEncoderDecoder.isFloatException(Float.POSITIVE_INFINITY));
    assertTrue(
        "Negative infinity should be an exception",
        AlpEncoderDecoder.isFloatException(Float.NEGATIVE_INFINITY));

    // Negative zero should be an exception
    assertTrue("Negative zero should be an exception", AlpEncoderDecoder.isFloatException(-0.0f));

    // Regular values should not be exceptions (basic check)
    assertFalse("1.0f should not be a basic exception", AlpEncoderDecoder.isFloatException(1.0f));
    assertFalse("0.0f should not be a basic exception", AlpEncoderDecoder.isFloatException(0.0f));
  }

  @Test
  public void testFloatEncoding() {
    // Test known encoding: 1.23 * 10^2 = 123
    int encoded = AlpEncoderDecoder.encodeFloat(1.23f, 2, 0);
    assertEquals(123, encoded);

    // Test with factor: 12.3 * 10^(2-1) = 12.3 * 10 = 123
    encoded = AlpEncoderDecoder.encodeFloat(12.3f, 2, 1);
    assertEquals(123, encoded);

    // Test zero
    encoded = AlpEncoderDecoder.encodeFloat(0.0f, 5, 0);
    assertEquals(0, encoded);
  }

  @Test
  public void testFloatDecoding() {
    // Test known decoding: 123 / 10^2 = 1.23
    float decoded = AlpEncoderDecoder.decodeFloat(123, 2, 0);
    assertEquals(1.23f, decoded, 1e-6f);

    // Test with factor: 123 / 10^(2-1) = 123 / 10 = 12.3
    decoded = AlpEncoderDecoder.decodeFloat(123, 2, 1);
    assertEquals(12.3f, decoded, 1e-6f);

    // Test zero
    decoded = AlpEncoderDecoder.decodeFloat(0, 5, 0);
    assertEquals(0.0f, decoded, 0.0f);
  }

  @Test
  public void testFastRoundFloat() {
    // Positive values
    assertEquals(5, AlpEncoderDecoder.fastRoundFloat(5.4f));
    assertEquals(6, AlpEncoderDecoder.fastRoundFloat(5.5f));
    assertEquals(6, AlpEncoderDecoder.fastRoundFloat(5.6f));

    // Negative values
    assertEquals(-5, AlpEncoderDecoder.fastRoundFloat(-5.4f));
    assertEquals(-6, AlpEncoderDecoder.fastRoundFloat(-5.5f));
    assertEquals(-6, AlpEncoderDecoder.fastRoundFloat(-5.6f));

    // Zero
    assertEquals(0, AlpEncoderDecoder.fastRoundFloat(0.0f));
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
    // NaN should be an exception
    assertTrue("NaN should be an exception", AlpEncoderDecoder.isDoubleException(Double.NaN));

    // Infinity should be an exception
    assertTrue(
        "Positive infinity should be an exception",
        AlpEncoderDecoder.isDoubleException(Double.POSITIVE_INFINITY));
    assertTrue(
        "Negative infinity should be an exception",
        AlpEncoderDecoder.isDoubleException(Double.NEGATIVE_INFINITY));

    // Negative zero should be an exception
    assertTrue("Negative zero should be an exception", AlpEncoderDecoder.isDoubleException(-0.0));

    // Regular values should not be exceptions (basic check)
    assertFalse("1.0 should not be a basic exception", AlpEncoderDecoder.isDoubleException(1.0));
    assertFalse("0.0 should not be a basic exception", AlpEncoderDecoder.isDoubleException(0.0));
  }

  @Test
  public void testDoubleEncoding() {
    // Test known encoding: 1.23 * 10^2 = 123
    long encoded = AlpEncoderDecoder.encodeDouble(1.23, 2, 0);
    assertEquals(123L, encoded);

    // Test with factor: 12.3 * 10^(2-1) = 12.3 * 10 = 123
    encoded = AlpEncoderDecoder.encodeDouble(12.3, 2, 1);
    assertEquals(123L, encoded);

    // Test zero
    encoded = AlpEncoderDecoder.encodeDouble(0.0, 5, 0);
    assertEquals(0L, encoded);
  }

  @Test
  public void testDoubleDecoding() {
    // Test known decoding: 123 / 10^2 = 1.23
    double decoded = AlpEncoderDecoder.decodeDouble(123, 2, 0);
    assertEquals(1.23, decoded, 1e-10);

    // Test with factor: 123 / 10^(2-1) = 123 / 10 = 12.3
    decoded = AlpEncoderDecoder.decodeDouble(123, 2, 1);
    assertEquals(12.3, decoded, 1e-10);

    // Test zero
    decoded = AlpEncoderDecoder.decodeDouble(0, 5, 0);
    assertEquals(0.0, decoded, 0.0);
  }

  @Test
  public void testFastRoundDouble() {
    // Positive values
    assertEquals(5L, AlpEncoderDecoder.fastRoundDouble(5.4));
    assertEquals(6L, AlpEncoderDecoder.fastRoundDouble(5.5));
    assertEquals(6L, AlpEncoderDecoder.fastRoundDouble(5.6));

    // Negative values
    assertEquals(-5L, AlpEncoderDecoder.fastRoundDouble(-5.4));
    assertEquals(-6L, AlpEncoderDecoder.fastRoundDouble(-5.5));
    assertEquals(-6L, AlpEncoderDecoder.fastRoundDouble(-5.6));

    // Zero
    assertEquals(0L, AlpEncoderDecoder.fastRoundDouble(0.0));
  }

  // ========== Bit Width Tests ==========

  @Test
  public void testBitWidthInt() {
    assertEquals(0, AlpEncoderDecoder.bitWidth(0));
    assertEquals(1, AlpEncoderDecoder.bitWidth(1));
    assertEquals(2, AlpEncoderDecoder.bitWidth(2));
    assertEquals(2, AlpEncoderDecoder.bitWidth(3));
    assertEquals(3, AlpEncoderDecoder.bitWidth(4));
    assertEquals(8, AlpEncoderDecoder.bitWidth(255));
    assertEquals(9, AlpEncoderDecoder.bitWidth(256));
    assertEquals(16, AlpEncoderDecoder.bitWidth(65535));
    assertEquals(31, AlpEncoderDecoder.bitWidth(Integer.MAX_VALUE));
  }

  @Test
  public void testBitWidthLong() {
    assertEquals(0, AlpEncoderDecoder.bitWidth(0L));
    assertEquals(1, AlpEncoderDecoder.bitWidth(1L));
    assertEquals(2, AlpEncoderDecoder.bitWidth(2L));
    assertEquals(2, AlpEncoderDecoder.bitWidth(3L));
    assertEquals(3, AlpEncoderDecoder.bitWidth(4L));
    assertEquals(8, AlpEncoderDecoder.bitWidth(255L));
    assertEquals(9, AlpEncoderDecoder.bitWidth(256L));
    assertEquals(16, AlpEncoderDecoder.bitWidth(65535L));
    assertEquals(31, AlpEncoderDecoder.bitWidth((long) Integer.MAX_VALUE));
    assertEquals(63, AlpEncoderDecoder.bitWidth(Long.MAX_VALUE));
  }

  // ========== Best Parameters Tests ==========

  @Test
  public void testFindBestFloatParams() {
    // Test with values that have a clear best exponent/factor
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    assertNotNull(params);
    assertTrue(params.exponent >= 0 && params.exponent <= AlpConstants.FLOAT_MAX_EXPONENT);
    assertTrue(params.factor >= 0 && params.factor <= params.exponent);

    // Verify these params work for the values
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
    // All NaN values - should all be exceptions
    float[] values = {Float.NaN, Float.NaN, Float.NaN};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);

    assertNotNull(params);
    assertEquals(values.length, params.numExceptions);
  }

  @Test
  public void testFindBestDoubleParams() {
    // Test with values that have a clear best exponent/factor
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);

    assertNotNull(params);
    assertTrue(params.exponent >= 0 && params.exponent <= AlpConstants.DOUBLE_MAX_EXPONENT);
    assertTrue(params.factor >= 0 && params.factor <= params.exponent);

    // Verify these params work for the values
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
    // All NaN/Infinity values - should all be exceptions
    double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);

    assertNotNull(params);
    assertEquals(values.length, params.numExceptions);
  }

  @Test
  public void testFindBestParamsWithOffset() {
    // Test that offset works correctly
    float[] values = {Float.NaN, Float.NaN, 1.23f, 4.56f, 7.89f, Float.NaN};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 2, 3);

    assertNotNull(params);
    // Should be able to encode values[2..4] without exceptions
    assertEquals(0, params.numExceptions);
  }
}
