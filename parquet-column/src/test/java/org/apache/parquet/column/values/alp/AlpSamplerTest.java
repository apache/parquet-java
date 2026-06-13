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

import java.util.Random;
import org.junit.Test;

public class AlpSamplerTest {

  // ========== Float sampler ==========

  @Test
  public void testFloatDecimalData() {
    // 2-decimal-place data should pick exponent=2, factor=0
    float[] data = new float[5000];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 0.01f;
    }
    AlpSampler.FloatSampler sampler = new AlpSampler.FloatSampler();
    sampler.addSample(data, data.length);
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();

    assertNotNull(preset.combinations);
    assertTrue(preset.combinations.length >= 1);
    assertTrue(preset.combinations.length <= AlpConstants.MAX_COMBINATIONS);
  }

  @Test
  public void testFloatIntegerData() {
    float[] data = new float[5000];
    for (int i = 0; i < data.length; i++) {
      data[i] = i;
    }
    AlpSampler.FloatSampler sampler = new AlpSampler.FloatSampler();
    sampler.addSample(data, data.length);
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();

    assertNotNull(preset.combinations);
    assertTrue(preset.combinations.length >= 1);
    // For integer data, any (e,e) combo works since multiplier = 10^e/10^e = 1.
    // Tiebreaker prefers bigger exponents, matching C++ behavior.
    int bestE = preset.combinations[0][0];
    int bestF = preset.combinations[0][1];
    assertEquals("Integer data: exponent should equal factor", bestE, bestF);
  }

  @Test
  public void testFloatPresetProducesValidRoundTrip() {
    float[] data = new float[5000];
    Random rng = new Random(42);
    for (int i = 0; i < data.length; i++) {
      data[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }

    AlpSampler.FloatSampler sampler = new AlpSampler.FloatSampler();
    sampler.addSample(data, data.length);
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();

    // Compress and decompress a vector using the preset
    int vectorSize = Math.min(1024, data.length);
    float[] vector = new float[vectorSize];
    System.arraycopy(data, 0, vector, 0, vectorSize);

    AlpCompression.FloatCompressedVector cv = AlpCompression.compressFloatVector(vector, vectorSize, preset);
    float[] output = new float[vectorSize];
    AlpCompression.decompressFloatVector(cv, output);

    for (int i = 0; i < vectorSize; i++) {
      assertEquals("Mismatch at " + i, Float.floatToRawIntBits(vector[i]), Float.floatToRawIntBits(output[i]));
    }
  }

  @Test
  public void testFloatSmallDataset() {
    // Fewer values than SAMPLER_VECTOR_SIZE
    float[] data = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
    AlpSampler.FloatSampler sampler = new AlpSampler.FloatSampler();
    sampler.addSample(data, data.length);
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();

    assertNotNull(preset.combinations);
    assertTrue(preset.combinations.length >= 1);
  }

  @Test
  public void testFloatMultipleSamples() {
    AlpSampler.FloatSampler sampler = new AlpSampler.FloatSampler();
    for (int batch = 0; batch < 10; batch++) {
      float[] data = new float[1000];
      for (int i = 0; i < 1000; i++) {
        data[i] = (batch * 1000 + i) * 0.1f;
      }
      sampler.addSample(data, data.length);
    }
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();
    assertNotNull(preset.combinations);
    assertTrue(preset.combinations.length >= 1);
  }

  // ========== Double sampler ==========

  @Test
  public void testDoubleDecimalData() {
    double[] data = new double[5000];
    for (int i = 0; i < data.length; i++) {
      data[i] = i * 0.01;
    }
    AlpSampler.DoubleSampler sampler = new AlpSampler.DoubleSampler();
    sampler.addSample(data, data.length);
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();

    assertNotNull(preset.combinations);
    assertTrue(preset.combinations.length >= 1);
    assertTrue(preset.combinations.length <= AlpConstants.MAX_COMBINATIONS);
  }

  @Test
  public void testDoubleIntegerData() {
    double[] data = new double[5000];
    for (int i = 0; i < data.length; i++) {
      data[i] = i;
    }
    AlpSampler.DoubleSampler sampler = new AlpSampler.DoubleSampler();
    sampler.addSample(data, data.length);
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();

    assertNotNull(preset.combinations);
    assertTrue(preset.combinations.length >= 1);
    int bestE = preset.combinations[0][0];
    int bestF = preset.combinations[0][1];
    assertEquals("Integer data: exponent should equal factor", bestE, bestF);
  }

  @Test
  public void testDoublePresetProducesValidRoundTrip() {
    double[] data = new double[5000];
    Random rng = new Random(42);
    for (int i = 0; i < data.length; i++) {
      data[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }

    AlpSampler.DoubleSampler sampler = new AlpSampler.DoubleSampler();
    sampler.addSample(data, data.length);
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();

    int vectorSize = Math.min(1024, data.length);
    double[] vector = new double[vectorSize];
    System.arraycopy(data, 0, vector, 0, vectorSize);

    AlpCompression.DoubleCompressedVector cv = AlpCompression.compressDoubleVector(vector, vectorSize, preset);
    double[] output = new double[vectorSize];
    AlpCompression.decompressDoubleVector(cv, output);

    for (int i = 0; i < vectorSize; i++) {
      assertEquals(
          "Mismatch at " + i, Double.doubleToRawLongBits(vector[i]), Double.doubleToRawLongBits(output[i]));
    }
  }

  @Test
  public void testDoubleSmallDataset() {
    double[] data = {1.1, 2.2, 3.3, 4.4, 5.5};
    AlpSampler.DoubleSampler sampler = new AlpSampler.DoubleSampler();
    sampler.addSample(data, data.length);
    AlpCompression.AlpEncodingPreset preset = sampler.finalizeSampling();

    assertNotNull(preset.combinations);
    assertTrue(preset.combinations.length >= 1);
  }
}
