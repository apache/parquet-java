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

public class AlpEncoderDecoderTest {

  @Test
  public void testFloatRoundTrip() {
    float[] testValues = {0.0f, 1.0f, -1.0f, 3.14159f, 100.5f, 0.001f, 1234567.0f};
    for (float value : testValues) {
      for (int e = 0; e <= AlpConstants.FLOAT_MAX_EXPONENT; e++) {
        for (int f = 0; f <= e; f++) {
          if (!AlpEncoderDecoder.isFloatException(value, e, f)) {
            int encoded = AlpEncoderDecoder.encodeFloat(value, e, f);
            float decoded = AlpEncoderDecoder.decodeFloat(encoded, e, f);
            assertEquals(Float.floatToRawIntBits(value), Float.floatToRawIntBits(decoded));
          }
        }
      }
    }
  }

  @Test
  public void testFloatExceptionDetection() {
    assertTrue(AlpEncoderDecoder.isFloatException(Float.NaN));
    assertTrue(AlpEncoderDecoder.isFloatException(Float.POSITIVE_INFINITY));
    assertTrue(AlpEncoderDecoder.isFloatException(Float.NEGATIVE_INFINITY));
    assertTrue(AlpEncoderDecoder.isFloatException(-0.0f));
    assertFalse(AlpEncoderDecoder.isFloatException(1.0f));
    assertFalse(AlpEncoderDecoder.isFloatException(0.0f));
  }

  @Test
  public void testFloatEncoding() {
    assertEquals(123, AlpEncoderDecoder.encodeFloat(1.23f, 2, 0));
    assertEquals(123, AlpEncoderDecoder.encodeFloat(12.3f, 2, 1));
    assertEquals(0, AlpEncoderDecoder.encodeFloat(0.0f, 5, 0));
  }

  @Test
  public void testFastRoundFloat() {
    assertEquals(5, AlpEncoderDecoder.fastRoundFloat(5.4f));
    assertEquals(6, AlpEncoderDecoder.fastRoundFloat(5.5f));
    assertEquals(-5, AlpEncoderDecoder.fastRoundFloat(-5.4f));
    assertEquals(-6, AlpEncoderDecoder.fastRoundFloat(-5.5f));
    assertEquals(0, AlpEncoderDecoder.fastRoundFloat(0.0f));
  }

  @Test
  public void testDoubleRoundTrip() {
    double[] testValues = {0.0, 1.0, -1.0, 3.14159265358979, 100.5, 0.001};
    for (double value : testValues) {
      for (int e = 0; e <= Math.min(AlpConstants.DOUBLE_MAX_EXPONENT, 10); e++) {
        for (int f = 0; f <= e; f++) {
          if (!AlpEncoderDecoder.isDoubleException(value, e, f)) {
            long encoded = AlpEncoderDecoder.encodeDouble(value, e, f);
            double decoded = AlpEncoderDecoder.decodeDouble(encoded, e, f);
            assertEquals(Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(decoded));
          }
        }
      }
    }
  }

  @Test
  public void testDoubleExceptionDetection() {
    assertTrue(AlpEncoderDecoder.isDoubleException(Double.NaN));
    assertTrue(AlpEncoderDecoder.isDoubleException(Double.POSITIVE_INFINITY));
    assertTrue(AlpEncoderDecoder.isDoubleException(Double.NEGATIVE_INFINITY));
    assertTrue(AlpEncoderDecoder.isDoubleException(-0.0));
    assertFalse(AlpEncoderDecoder.isDoubleException(1.0));
    assertFalse(AlpEncoderDecoder.isDoubleException(0.0));
  }

  @Test
  public void testBitWidthForInt() {
    assertEquals(0, AlpEncoderDecoder.bitWidthForInt(0));
    assertEquals(1, AlpEncoderDecoder.bitWidthForInt(1));
    assertEquals(8, AlpEncoderDecoder.bitWidthForInt(255));
    assertEquals(9, AlpEncoderDecoder.bitWidthForInt(256));
    assertEquals(31, AlpEncoderDecoder.bitWidthForInt(Integer.MAX_VALUE));
  }

  @Test
  public void testBitWidthForLong() {
    assertEquals(0, AlpEncoderDecoder.bitWidthForLong(0L));
    assertEquals(1, AlpEncoderDecoder.bitWidthForLong(1L));
    assertEquals(63, AlpEncoderDecoder.bitWidthForLong(Long.MAX_VALUE));
  }

  @Test
  public void testBitPackedSize() {
    assertEquals(0, AlpEncoderDecoder.bitPackedSize(1024, 0));
    assertEquals(128, AlpEncoderDecoder.bitPackedSize(1024, 1));
    assertEquals(1024, AlpEncoderDecoder.bitPackedSize(1024, 8));
    assertEquals(1, AlpEncoderDecoder.bitPackedSize(3, 2)); // ceil(6/8)=1
  }

  @Test
  public void testFindBestFloatParams() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);
    assertNotNull(params);
    assertEquals(0, params.numExceptions);
  }

  @Test
  public void testFindBestFloatParamsAllExceptions() {
    float[] values = {Float.NaN, Float.NaN, Float.NaN};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);
    assertEquals(values.length, params.numExceptions);
  }

  @Test
  public void testFindBestDoubleParams() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);
    assertNotNull(params);
    assertEquals(0, params.numExceptions);
  }

  @Test
  public void testFindBestParamsWithPresets() {
    float[] values = {1.23f, 4.56f, 7.89f};
    AlpEncoderDecoder.EncodingParams fullResult = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);
    int[][] presets = {{fullResult.exponent, fullResult.factor}, {0, 0}, {1, 0}};
    AlpEncoderDecoder.EncodingParams presetResult =
        AlpEncoderDecoder.findBestFloatParamsWithPresets(values, 0, values.length, presets);
    assertTrue(presetResult.numExceptions <= fullResult.numExceptions);
  }
}
