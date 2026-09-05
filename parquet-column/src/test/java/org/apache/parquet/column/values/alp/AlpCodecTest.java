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
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;

/**
 * Tests for the core ALP encoder/decoder logic.
 */
public class AlpCodecTest {

  // ========== Float Encoding/Decoding Tests ==========

  @Test
  public void testFloatRoundTrip() {
    float[] testValues = {0.0f, 1.0f, -1.0f, 3.14159f, 100.5f, 0.001f, 1234567.0f};

    for (float value : testValues) {
      for (int exponent = 0; exponent <= AlpConstants.FLOAT_MAX_EXPONENT; exponent++) {
        for (int factor = 0; factor <= exponent; factor++) {
          if (!AlpCodec.isFloatException(value, exponent, factor)) {
            int encoded = AlpCodec.encodeFloat(value, exponent, factor);
            float decoded = AlpCodec.decodeFloat(encoded, exponent, factor);
            assertThat(Float.floatToRawIntBits(decoded))
                .as("Round-trip failed for value=" + value + ", exponent=" + exponent + ", factor="
                    + factor)
                .isEqualTo(Float.floatToRawIntBits(value));
          }
        }
      }
    }
  }

  @Test
  public void testFloatExceptionDetection() {
    assertThat(AlpCodec.isIntrinsicFloatException(Float.NaN))
        .as("NaN should be an exception")
        .isTrue();
    assertThat(AlpCodec.isIntrinsicFloatException(Float.POSITIVE_INFINITY))
        .as("Positive infinity should be an exception")
        .isTrue();
    assertThat(AlpCodec.isIntrinsicFloatException(Float.NEGATIVE_INFINITY))
        .as("Negative infinity should be an exception")
        .isTrue();
    assertThat(AlpCodec.isIntrinsicFloatException(-0.0f))
        .as("Negative zero should be an exception")
        .isTrue();

    assertThat(AlpCodec.isIntrinsicFloatException(1.0f))
        .as("1.0f should not be a basic exception")
        .isFalse();
    assertThat(AlpCodec.isIntrinsicFloatException(0.0f))
        .as("0.0f should not be a basic exception")
        .isFalse();
  }

  @Test
  public void testFloatEncoding() {
    assertThat(AlpCodec.encodeFloat(1.23f, 2, 0)).isEqualTo(123);
    assertThat(AlpCodec.encodeFloat(12.3f, 2, 1)).isEqualTo(123);
    assertThat(AlpCodec.encodeFloat(0.0f, 5, 0)).isEqualTo(0);
  }

  @Test
  public void testFloatDecoding() {
    assertThat(AlpCodec.decodeFloat(123, 2, 0)).isCloseTo(1.23f, within(1e-6f));
    assertThat(AlpCodec.decodeFloat(123, 2, 1)).isCloseTo(12.3f, within(1e-6f));
    assertThat(AlpCodec.decodeFloat(0, 5, 0)).isEqualTo(0.0f);
  }

  @Test
  public void testFloatEncodeRounding() {
    // Verify rounding behavior (magic number trick rounds to nearest)
    assertThat(AlpCodec.encodeFloat(5.4f, 0, 0)).isEqualTo(5);
    assertThat(AlpCodec.encodeFloat(5.6f, 0, 0)).isEqualTo(6);
    assertThat(AlpCodec.encodeFloat(-5.4f, 0, 0)).isEqualTo(-5);
    assertThat(AlpCodec.encodeFloat(-5.6f, 0, 0)).isEqualTo(-6);
    assertThat(AlpCodec.encodeFloat(0.0f, 0, 0)).isEqualTo(0);
  }

  @Test
  public void testFloatEncodeDecodeWithFactor() {
    // Verify that encode/decode with non-zero factor works correctly.
    // The key correctness property: encode uses value * POW10[e] * POW10_NEGATIVE[f],
    // and decode uses encoded * POW10[f] * POW10_NEGATIVE[e].
    float value = 12.3f;
    int encoded = AlpCodec.encodeFloat(value, 2, 1);
    assertThat(encoded).isEqualTo(123); // 12.3 * 100 * 0.1 = 123
    float decoded = AlpCodec.decodeFloat(encoded, 2, 1);
    assertThat(Float.floatToRawIntBits(decoded)).isEqualTo(Float.floatToRawIntBits(value));
  }

  // ========== Double Encoding/Decoding Tests ==========

  @Test
  public void testDoubleRoundTrip() {
    double[] testValues = {0.0, 1.0, -1.0, 3.14159265358979, 100.5, 0.001, 12345678901234.0};

    for (double value : testValues) {
      for (int exponent = 0; exponent <= Math.min(AlpConstants.DOUBLE_MAX_EXPONENT, 10); exponent++) {
        for (int factor = 0; factor <= exponent; factor++) {
          if (!AlpCodec.isDoubleException(value, exponent, factor)) {
            long encoded = AlpCodec.encodeDouble(value, exponent, factor);
            double decoded = AlpCodec.decodeDouble(encoded, exponent, factor);
            assertThat(Double.doubleToRawLongBits(decoded))
                .as("Round-trip failed for value=" + value + ", exponent=" + exponent + ", factor="
                    + factor)
                .isEqualTo(Double.doubleToRawLongBits(value));
          }
        }
      }
    }
  }

  @Test
  public void testDoubleExceptionDetection() {
    assertThat(AlpCodec.isIntrinsicDoubleException(Double.NaN))
        .as("NaN should be an exception")
        .isTrue();
    assertThat(AlpCodec.isIntrinsicDoubleException(Double.POSITIVE_INFINITY))
        .as("Positive infinity should be an exception")
        .isTrue();
    assertThat(AlpCodec.isIntrinsicDoubleException(Double.NEGATIVE_INFINITY))
        .as("Negative infinity should be an exception")
        .isTrue();
    assertThat(AlpCodec.isIntrinsicDoubleException(-0.0))
        .as("Negative zero should be an exception")
        .isTrue();

    assertThat(AlpCodec.isIntrinsicDoubleException(1.0))
        .as("1.0 should not be a basic exception")
        .isFalse();
    assertThat(AlpCodec.isIntrinsicDoubleException(0.0))
        .as("0.0 should not be a basic exception")
        .isFalse();
  }

  @Test
  public void testDoubleEncoding() {
    assertThat(AlpCodec.encodeDouble(1.23, 2, 0)).isEqualTo(123L);
    assertThat(AlpCodec.encodeDouble(12.3, 2, 1)).isEqualTo(123L);
    assertThat(AlpCodec.encodeDouble(0.0, 5, 0)).isEqualTo(0L);
  }

  @Test
  public void testDoubleDecoding() {
    assertThat(AlpCodec.decodeDouble(123, 2, 0)).isCloseTo(1.23, within(1e-10));
    assertThat(AlpCodec.decodeDouble(123, 2, 1)).isCloseTo(12.3, within(1e-10));
    assertThat(AlpCodec.decodeDouble(0, 5, 0)).isEqualTo(0.0);
  }

  @Test
  public void testDoubleEncodeRounding() {
    // Verify rounding behavior (magic number trick rounds to nearest)
    assertThat(AlpCodec.encodeDouble(5.4, 0, 0)).isEqualTo(5L);
    assertThat(AlpCodec.encodeDouble(5.6, 0, 0)).isEqualTo(6L);
    assertThat(AlpCodec.encodeDouble(-5.4, 0, 0)).isEqualTo(-5L);
    assertThat(AlpCodec.encodeDouble(-5.6, 0, 0)).isEqualTo(-6L);
    assertThat(AlpCodec.encodeDouble(0.0, 0, 0)).isEqualTo(0L);
  }

  @Test
  public void testDoubleEncodeDecodeWithFactor() {
    // Verify that encode/decode with non-zero factor works correctly.
    // The key correctness property: encode uses value * POW10[e] * POW10_NEGATIVE[f],
    // and decode uses encoded * POW10[f] * POW10_NEGATIVE[e].
    double value = 12.3;
    long encoded = AlpCodec.encodeDouble(value, 2, 1);
    assertThat(encoded).isEqualTo(123L); // 12.3 * 100 * 0.1 = 123
    double decoded = AlpCodec.decodeDouble(encoded, 2, 1);
    assertThat(Double.doubleToRawLongBits(decoded)).isEqualTo(Double.doubleToRawLongBits(value));
  }

  @Test
  public void testDoubleEncodeDecodeArithmeticOrder() {
    // This test verifies that the exact order of operations in encode/decode
    // is critical for IEEE 754 correctness. The encode uses
    // fastRound(value * POW10[e] * POW10_NEGATIVE[f]) and decode uses
    // (encoded * POW10[f] * POW10_NEGATIVE[e]), both as single expressions.
    // Splitting the multiplies or reordering changes rounding.
    double[] testValues = {0.123456789, 1.23456789, 12.3456789, 123.456789, 1234.56789};
    for (double value : testValues) {
      for (int e = 0; e <= 10; e++) {
        for (int f = 0; f <= e; f++) {
          if (!AlpCodec.isDoubleException(value, e, f)) {
            long encoded = AlpCodec.encodeDouble(value, e, f);
            double decoded = AlpCodec.decodeDouble(encoded, e, f);
            assertThat(Double.doubleToRawLongBits(decoded))
                .as("Roundtrip failed for " + value + " (e=" + e + ", f=" + f + ")")
                .isEqualTo(Double.doubleToRawLongBits(value));
          }
        }
      }
    }
  }

  // ========== Best Parameters Tests ==========

  @Test
  public void testFindBestFloatParams() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    AlpCodec.EncodingParams params = AlpCodec.findBestFloatParams(values, 0, values.length);

    assertThat(params).isNotNull();
    assertThat(params.exponent >= 0 && params.exponent <= AlpConstants.FLOAT_MAX_EXPONENT)
        .isTrue();
    assertThat(params.factor >= 0 && params.factor <= params.exponent).isTrue();

    for (float v : values) {
      if (!AlpCodec.isFloatException(v, params.exponent, params.factor)) {
        int encoded = AlpCodec.encodeFloat(v, params.exponent, params.factor);
        float decoded = AlpCodec.decodeFloat(encoded, params.exponent, params.factor);
        assertThat(Float.floatToRawIntBits(decoded)).isEqualTo(Float.floatToRawIntBits(v));
      }
    }
  }

  @Test
  public void testFindBestFloatParamsWithAllExceptions() {
    float[] values = {Float.NaN, Float.NaN, Float.NaN};
    AlpCodec.EncodingParams params = AlpCodec.findBestFloatParams(values, 0, values.length);

    assertThat(params).isNotNull();
    assertThat(params.numExceptions).isEqualTo(values.length);
  }

  @Test
  public void testFindBestDoubleParams() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    AlpCodec.EncodingParams params = AlpCodec.findBestDoubleParams(values, 0, values.length);

    assertThat(params).isNotNull();
    assertThat(params.exponent >= 0 && params.exponent <= AlpConstants.DOUBLE_MAX_EXPONENT)
        .isTrue();
    assertThat(params.factor >= 0 && params.factor <= params.exponent).isTrue();

    for (double v : values) {
      if (!AlpCodec.isDoubleException(v, params.exponent, params.factor)) {
        long encoded = AlpCodec.encodeDouble(v, params.exponent, params.factor);
        double decoded = AlpCodec.decodeDouble(encoded, params.exponent, params.factor);
        assertThat(Double.doubleToRawLongBits(decoded)).isEqualTo(Double.doubleToRawLongBits(v));
      }
    }
  }

  @Test
  public void testFindBestDoubleParamsWithAllExceptions() {
    double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
    AlpCodec.EncodingParams params = AlpCodec.findBestDoubleParams(values, 0, values.length);

    assertThat(params).isNotNull();
    assertThat(params.numExceptions).isEqualTo(values.length);
  }

  @Test
  public void testFindBestParamsWithOffset() {
    float[] values = {Float.NaN, Float.NaN, 1.23f, 4.56f, 7.89f, Float.NaN};
    AlpCodec.EncodingParams params = AlpCodec.findBestFloatParams(values, 2, 3);

    assertThat(params).isNotNull();
    assertThat(params.numExceptions).isEqualTo(0);
  }

  // ========== Preset-Based Parameter Search Tests ==========

  @Test
  public void testFindBestFloatParamsWithPresets() {
    float[] values = {1.23f, 4.56f, 7.89f, 10.11f, 12.13f};
    int[][] presets = {{2, 0}, {3, 0}, {4, 1}};
    AlpCodec.EncodingParams params = AlpCodec.findBestFloatParamsWithPresets(values, 0, values.length, presets);

    assertThat(params).isNotNull();
    // Should select one of the preset combinations
    boolean foundMatch = false;
    for (int[] preset : presets) {
      if (params.exponent == preset[0] && params.factor == preset[1]) {
        foundMatch = true;
        break;
      }
    }
    assertThat(foundMatch)
        .as("Result should be one of the preset combinations")
        .isTrue();
  }

  @Test
  public void testFindBestDoubleParamsWithPresets() {
    double[] values = {1.23, 4.56, 7.89, 10.11, 12.13};
    int[][] presets = {{2, 0}, {3, 0}, {4, 1}};
    AlpCodec.EncodingParams params = AlpCodec.findBestDoubleParamsWithPresets(values, 0, values.length, presets);

    assertThat(params).isNotNull();
    boolean foundMatch = false;
    for (int[] preset : presets) {
      if (params.exponent == preset[0] && params.factor == preset[1]) {
        foundMatch = true;
        break;
      }
    }
    assertThat(foundMatch)
        .as("Result should be one of the preset combinations")
        .isTrue();
  }

  @Test
  public void testPresetsProduceSameResultAsFullSearch() {
    float[] values = {1.23f, 4.56f, 7.89f};
    AlpCodec.EncodingParams fullResult = AlpCodec.findBestFloatParams(values, 0, values.length);

    // Include the best params in presets
    int[][] presets = {{fullResult.exponent, fullResult.factor}, {0, 0}, {1, 0}};
    AlpCodec.EncodingParams presetResult =
        AlpCodec.findBestFloatParamsWithPresets(values, 0, values.length, presets);

    assertThat(presetResult.numExceptions <= fullResult.numExceptions)
        .as("Preset result should be at least as good as full search")
        .isTrue();
  }

  @Test
  public void findBestFloatParamsMixedSignUsesSignedDeltaRange() {
    // Mixed-sign, non-integer values are losslessly encodable at some (e, f). A buggy estimator
    // that computes the FOR range with unsigned subtraction inflates every mixed-sign combo to a
    // 64-bit width, making it prefer combos that turn values into exceptions. With the correct
    // signed range (max - min), the estimator finds the lossless encoding with zero exceptions.
    float[] values = {-3.14f, 2.71f, 100.5f, -50.25f};
    AlpCodec.EncodingParams params = AlpCodec.findBestFloatParams(values, 0, values.length);
    assertThat(params.numExceptions).isEqualTo(0);
  }

  @Test
  public void findBestFloatParamsWithPresetsMixedSignUsesSignedDeltaRange() {
    // Restricting to two presets removes the equivalent-scaling ties, so the chosen exponent is
    // deterministic: e=0,f=0 has the smallest signed FOR span and wins. A buggy unsigned estimator
    // would rate both presets at 64 bits, tie, and pick the higher exponent (e=1) instead.
    float[] values = {-1.0f, 1.0f};
    int[][] presets = {{0, 0}, {1, 0}}; // e=0,f=0 has the smallest signed FOR span
    AlpCodec.EncodingParams params = AlpCodec.findBestFloatParamsWithPresets(values, 0, values.length, presets);
    assertThat(params.exponent).isEqualTo(0);
    assertThat(params.factor).isEqualTo(0);
    assertThat(params.numExceptions).isEqualTo(0);
  }
}
