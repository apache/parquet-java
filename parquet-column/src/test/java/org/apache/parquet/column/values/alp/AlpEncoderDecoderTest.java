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

import org.junit.jupiter.api.Test;

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
            assertThat(Float.floatToRawIntBits(decoded)).isEqualTo(Float.floatToRawIntBits(value));
          }
        }
      }
    }
  }

  @Test
  public void testFloatExceptionDetection() {
    assertThat(AlpEncoderDecoder.isFloatException(Float.NaN)).isTrue();
    assertThat(AlpEncoderDecoder.isFloatException(Float.POSITIVE_INFINITY)).isTrue();
    assertThat(AlpEncoderDecoder.isFloatException(Float.NEGATIVE_INFINITY)).isTrue();
    assertThat(AlpEncoderDecoder.isFloatException(-0.0f)).isTrue();
    assertThat(AlpEncoderDecoder.isFloatException(1.0f)).isFalse();
    assertThat(AlpEncoderDecoder.isFloatException(0.0f)).isFalse();
  }

  @Test
  public void testFloatEncoding() {
    assertThat(AlpEncoderDecoder.encodeFloat(1.23f, 2, 0)).isEqualTo(123);
    assertThat(AlpEncoderDecoder.encodeFloat(12.3f, 2, 1)).isEqualTo(123);
    assertThat(AlpEncoderDecoder.encodeFloat(0.0f, 5, 0)).isEqualTo(0);
  }

  @Test
  public void testFastRoundFloat() {
    assertThat(AlpEncoderDecoder.fastRoundFloat(5.4f)).isEqualTo(5);
    assertThat(AlpEncoderDecoder.fastRoundFloat(5.5f)).isEqualTo(6);
    assertThat(AlpEncoderDecoder.fastRoundFloat(-5.4f)).isEqualTo(-5);
    assertThat(AlpEncoderDecoder.fastRoundFloat(-5.5f)).isEqualTo(-6);
    assertThat(AlpEncoderDecoder.fastRoundFloat(0.0f)).isEqualTo(0);
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
            assertThat(Double.doubleToRawLongBits(decoded)).isEqualTo(Double.doubleToRawLongBits(value));
          }
        }
      }
    }
  }

  @Test
  public void testDoubleExceptionDetection() {
    assertThat(AlpEncoderDecoder.isDoubleException(Double.NaN)).isTrue();
    assertThat(AlpEncoderDecoder.isDoubleException(Double.POSITIVE_INFINITY))
        .isTrue();
    assertThat(AlpEncoderDecoder.isDoubleException(Double.NEGATIVE_INFINITY))
        .isTrue();
    assertThat(AlpEncoderDecoder.isDoubleException(-0.0)).isTrue();
    assertThat(AlpEncoderDecoder.isDoubleException(1.0)).isFalse();
    assertThat(AlpEncoderDecoder.isDoubleException(0.0)).isFalse();
  }

  @Test
  public void testBitWidthForInt() {
    assertThat(AlpEncoderDecoder.bitWidthForInt(0)).isEqualTo(0);
    assertThat(AlpEncoderDecoder.bitWidthForInt(1)).isEqualTo(1);
    assertThat(AlpEncoderDecoder.bitWidthForInt(255)).isEqualTo(8);
    assertThat(AlpEncoderDecoder.bitWidthForInt(256)).isEqualTo(9);
    assertThat(AlpEncoderDecoder.bitWidthForInt(Integer.MAX_VALUE)).isEqualTo(31);
  }

  @Test
  public void testBitWidthForLong() {
    assertThat(AlpEncoderDecoder.bitWidthForLong(0L)).isEqualTo(0);
    assertThat(AlpEncoderDecoder.bitWidthForLong(1L)).isEqualTo(1);
    assertThat(AlpEncoderDecoder.bitWidthForLong(Long.MAX_VALUE)).isEqualTo(63);
  }

  @Test
  public void testBitPackedSize() {
    assertThat(AlpEncoderDecoder.bitPackedSize(1024, 0)).isEqualTo(0);
    assertThat(AlpEncoderDecoder.bitPackedSize(1024, 1)).isEqualTo(128);
    assertThat(AlpEncoderDecoder.bitPackedSize(1024, 8)).isEqualTo(1024);
    assertThat(AlpEncoderDecoder.bitPackedSize(3, 2)).isEqualTo(1); // ceil(6/8)=1
  }

  @Test
  public void testFindBestFloatParams() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);
    assertThat(params).isNotNull();
    assertThat(params.numExceptions).isEqualTo(0);
  }

  @Test
  public void testFindBestFloatParamsAllExceptions() {
    float[] values = {Float.NaN, Float.NaN, Float.NaN};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);
    assertThat(params.numExceptions).isEqualTo(values.length);
  }

  @Test
  public void testFindBestDoubleParams() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, values.length);
    assertThat(params).isNotNull();
    assertThat(params.numExceptions).isEqualTo(0);
  }

  @Test
  public void testFindBestParamsWithPresets() {
    float[] values = {1.23f, 4.56f, 7.89f};
    AlpEncoderDecoder.EncodingParams fullResult = AlpEncoderDecoder.findBestFloatParams(values, 0, values.length);
    int[][] presets = {{fullResult.exponent, fullResult.factor}, {0, 0}, {1, 0}};
    AlpEncoderDecoder.EncodingParams presetResult =
        AlpEncoderDecoder.findBestFloatParamsWithPresets(values, 0, values.length, presets);
    assertThat(presetResult.numExceptions).isLessThanOrEqualTo(fullResult.numExceptions);
  }
}
