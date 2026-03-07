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

import static org.apache.parquet.column.values.alp.AlpConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ALP sampler that collects representative samples and generates encoding presets.
 *
 * <p>Usage: call {@code addSample()} to feed data, then {@code finalize()} to get the preset.
 * Mirrors C++ {@code AlpSampler<T>} with separate inner classes for float and double.
 */
final class AlpSampler {

  private AlpSampler() {}

  // ========== FloatSampler ==========

  static final class FloatSampler {
    private final long sampleVectorSize = SAMPLER_VECTOR_SIZE;
    private final long rowgroupSize = SAMPLER_ROWGROUP_SIZE;
    private final long samplesPerVector = SAMPLER_SAMPLES_PER_VECTOR;
    private final long sampleVectorsPerRowgroup = SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP;
    private final long rowgroupSampleJump;

    private long vectorsSampledCount;
    private long totalValuesCount;
    private long vectorsCount;
    private final List<float[]> rowgroupSample = new ArrayList<>();

    FloatSampler() {
      rowgroupSampleJump = (rowgroupSize / sampleVectorsPerRowgroup) / sampleVectorSize;
    }

    /** Add a sample of arbitrary size (split into vectors internally). */
    void addSample(float[] data, int count) {
      for (int i = 0; i < count; i += (int) sampleVectorSize) {
        int elements = (int) Math.min(count - i, sampleVectorSize);
        addSampleVector(data, i, elements);
      }
    }

    private void addSampleVector(float[] data, int offset, int length) {
      boolean mustSkip = mustSkipSamplingFromCurrentVector(vectorsCount, vectorsSampledCount, length);
      vectorsCount++;
      totalValuesCount += length;
      if (mustSkip) {
        return;
      }

      int numLookup = (int) Math.min(length, DEFAULT_VECTOR_SIZE);
      int increment = (int) Math.max(1, (int) Math.ceil((double) numLookup / samplesPerVector));

      // Take equidistant subsample
      List<Float> sample = new ArrayList<>();
      for (int i = 0; i < numLookup; i += increment) {
        sample.add(data[offset + i]);
      }

      float[] sampleArray = new float[sample.size()];
      for (int i = 0; i < sample.size(); i++) {
        sampleArray[i] = sample.get(i);
      }

      rowgroupSample.add(sampleArray);
      vectorsSampledCount++;
    }

    private boolean mustSkipSamplingFromCurrentVector(
        long vectorsCount, long vectorsSampledCount, int currentVectorSize) {
      if ((vectorsCount % rowgroupSampleJump) != 0) {
        return true;
      }
      return currentVectorSize < SAMPLER_SAMPLES_PER_VECTOR && vectorsSampledCount != 0;
    }

    /** Finalize sampling and return the encoding preset. */
    AlpCompression.AlpEncodingPreset finalizeSampling() {
      return createFloatEncodingPreset(rowgroupSample);
    }
  }

  // ========== DoubleSampler ==========

  static final class DoubleSampler {
    private final long sampleVectorSize = SAMPLER_VECTOR_SIZE;
    private final long rowgroupSize = SAMPLER_ROWGROUP_SIZE;
    private final long samplesPerVector = SAMPLER_SAMPLES_PER_VECTOR;
    private final long sampleVectorsPerRowgroup = SAMPLER_SAMPLE_VECTORS_PER_ROWGROUP;
    private final long rowgroupSampleJump;

    private long vectorsSampledCount;
    private long totalValuesCount;
    private long vectorsCount;
    private final List<double[]> rowgroupSample = new ArrayList<>();

    DoubleSampler() {
      rowgroupSampleJump = (rowgroupSize / sampleVectorsPerRowgroup) / sampleVectorSize;
    }

    void addSample(double[] data, int count) {
      for (int i = 0; i < count; i += (int) sampleVectorSize) {
        int elements = (int) Math.min(count - i, sampleVectorSize);
        addSampleVector(data, i, elements);
      }
    }

    private void addSampleVector(double[] data, int offset, int length) {
      boolean mustSkip = mustSkipSamplingFromCurrentVector(vectorsCount, vectorsSampledCount, length);
      vectorsCount++;
      totalValuesCount += length;
      if (mustSkip) {
        return;
      }

      int numLookup = (int) Math.min(length, DEFAULT_VECTOR_SIZE);
      int increment = (int) Math.max(1, (int) Math.ceil((double) numLookup / samplesPerVector));

      List<Double> sample = new ArrayList<>();
      for (int i = 0; i < numLookup; i += increment) {
        sample.add(data[offset + i]);
      }

      double[] sampleArray = new double[sample.size()];
      for (int i = 0; i < sample.size(); i++) {
        sampleArray[i] = sample.get(i);
      }

      rowgroupSample.add(sampleArray);
      vectorsSampledCount++;
    }

    private boolean mustSkipSamplingFromCurrentVector(
        long vectorsCount, long vectorsSampledCount, int currentVectorSize) {
      if ((vectorsCount % rowgroupSampleJump) != 0) {
        return true;
      }
      return currentVectorSize < SAMPLER_SAMPLES_PER_VECTOR && vectorsSampledCount != 0;
    }

    AlpCompression.AlpEncodingPreset finalizeSampling() {
      return createDoubleEncodingPreset(rowgroupSample);
    }
  }

  // ========== CreateEncodingPreset (float) ==========

  /**
   * Estimate compressed size in bits for a given (exponent, factor) on sample data.
   * Returns -1 if the combination yields almost all exceptions (< 2 non-exceptions).
   */
  private static long estimateFloatCompressedSize(
      float[] sample, int exponent, int factor, boolean penalizeExceptions) {
    int minEncoded = Integer.MAX_VALUE;
    int maxEncoded = Integer.MIN_VALUE;
    int numExceptions = 0;
    int numNonExceptions = 0;

    for (float value : sample) {
      int encoded = AlpEncoderDecoder.encodeFloat(value, exponent, factor);
      float decoded = AlpEncoderDecoder.decodeFloat(encoded, exponent, factor);
      if (Float.floatToRawIntBits(decoded) == Float.floatToRawIntBits(value)) {
        numNonExceptions++;
        if (encoded < minEncoded) minEncoded = encoded;
        if (encoded > maxEncoded) maxEncoded = encoded;
      } else {
        numExceptions++;
      }
    }

    if (penalizeExceptions && numNonExceptions < 2) {
      return -1;
    }

    long delta;
    if (numNonExceptions >= 2) {
      // Unsigned difference
      delta = Integer.toUnsignedLong(maxEncoded) - Integer.toUnsignedLong(minEncoded);
    } else {
      delta = 0;
    }
    int bitsPerValue = (delta == 0) ? 0 : (64 - Long.numberOfLeadingZeros(delta));
    long estimatedSize = (long) sample.length * bitsPerValue;
    estimatedSize += (long) numExceptions * (32 + POSITION_SIZE * 8);
    return estimatedSize;
  }

  static AlpCompression.AlpEncodingPreset createFloatEncodingPreset(List<float[]> vectorsSampled) {
    // For each sampled vector, find the best (e,f) combo by estimated compressed size.
    // Count how many times each best combo appears across all sampled vectors.
    Map<Long, int[]> bestCombosCount = new HashMap<>(); // key = e<<8|f, value = [count]

    for (float[] sample : vectorsSampled) {
      long bestSize = Long.MAX_VALUE;
      int bestE = FLOAT_MAX_EXPONENT;
      int bestF = FLOAT_MAX_EXPONENT;

      for (int e = 0; e <= FLOAT_MAX_EXPONENT; e++) {
        for (int f = 0; f <= e; f++) {
          long size = estimateFloatCompressedSize(sample, e, f, true);
          if (size < 0) continue;
          if (size < bestSize
              || (size == bestSize && e > bestE)
              || (size == bestSize && e == bestE && f > bestF)) {
            bestSize = size;
            bestE = e;
            bestF = f;
          }
        }
      }
      long key = ((long) bestE << 8) | bestF;
      bestCombosCount.computeIfAbsent(key, k -> new int[1])[0]++;
    }

    // Sort by appearance count (descending), then by exponent/factor (descending)
    List<Map.Entry<Long, int[]>> sorted = new ArrayList<>(bestCombosCount.entrySet());
    sorted.sort((a, b) -> {
      int cmpCount = Integer.compare(b.getValue()[0], a.getValue()[0]);
      if (cmpCount != 0) return cmpCount;
      int eA = (int) (a.getKey() >> 8);
      int fA = (int) (a.getKey() & 0xFF);
      int eB = (int) (b.getKey() >> 8);
      int fB = (int) (b.getKey() & 0xFF);
      if (eA != eB) return Integer.compare(eB, eA);
      return Integer.compare(fB, fA);
    });

    int k = Math.min(MAX_COMBINATIONS, sorted.size());
    int[][] combinations = new int[k][2];
    for (int i = 0; i < k; i++) {
      long key = sorted.get(i).getKey();
      combinations[i][0] = (int) (key >> 8);
      combinations[i][1] = (int) (key & 0xFF);
    }
    return new AlpCompression.AlpEncodingPreset(combinations);
  }

  // ========== CreateEncodingPreset (double) ==========

  private static long estimateDoubleCompressedSize(
      double[] sample, int exponent, int factor, boolean penalizeExceptions) {
    long minEncoded = Long.MAX_VALUE;
    long maxEncoded = Long.MIN_VALUE;
    int numExceptions = 0;
    int numNonExceptions = 0;

    for (double value : sample) {
      long encoded = AlpEncoderDecoder.encodeDouble(value, exponent, factor);
      double decoded = AlpEncoderDecoder.decodeDouble(encoded, exponent, factor);
      if (Double.doubleToRawLongBits(decoded) == Double.doubleToRawLongBits(value)) {
        numNonExceptions++;
        if (encoded < minEncoded) minEncoded = encoded;
        if (encoded > maxEncoded) maxEncoded = encoded;
      } else {
        numExceptions++;
      }
    }

    if (penalizeExceptions && numNonExceptions < 2) {
      return -1;
    }

    // For bit width: unsigned difference. Use Long.compareUnsigned logic.
    int bitsPerValue;
    if (numNonExceptions < 2) {
      bitsPerValue = 0;
    } else {
      // Unsigned subtraction: maxEncoded - minEncoded as unsigned
      long delta = maxEncoded - minEncoded;
      bitsPerValue = (delta == 0) ? 0 : (64 - Long.numberOfLeadingZeros(delta));
    }
    long estimatedSize = (long) sample.length * bitsPerValue;
    estimatedSize += (long) numExceptions * (64 + POSITION_SIZE * 8);
    return estimatedSize;
  }

  static AlpCompression.AlpEncodingPreset createDoubleEncodingPreset(List<double[]> vectorsSampled) {
    Map<Long, int[]> bestCombosCount = new HashMap<>();

    for (double[] sample : vectorsSampled) {
      long bestSize = Long.MAX_VALUE;
      int bestE = DOUBLE_MAX_EXPONENT;
      int bestF = DOUBLE_MAX_EXPONENT;

      for (int e = 0; e <= DOUBLE_MAX_EXPONENT; e++) {
        for (int f = 0; f <= e; f++) {
          long size = estimateDoubleCompressedSize(sample, e, f, true);
          if (size < 0) continue;
          if (size < bestSize
              || (size == bestSize && e > bestE)
              || (size == bestSize && e == bestE && f > bestF)) {
            bestSize = size;
            bestE = e;
            bestF = f;
          }
        }
      }
      long key = ((long) bestE << 8) | bestF;
      bestCombosCount.computeIfAbsent(key, k -> new int[1])[0]++;
    }

    List<Map.Entry<Long, int[]>> sorted = new ArrayList<>(bestCombosCount.entrySet());
    sorted.sort((a, b) -> {
      int cmpCount = Integer.compare(b.getValue()[0], a.getValue()[0]);
      if (cmpCount != 0) return cmpCount;
      int eA = (int) (a.getKey() >> 8);
      int fA = (int) (a.getKey() & 0xFF);
      int eB = (int) (b.getKey() >> 8);
      int fB = (int) (b.getKey() & 0xFF);
      if (eA != eB) return Integer.compare(eB, eA);
      return Integer.compare(fB, fA);
    });

    int k = Math.min(MAX_COMBINATIONS, sorted.size());
    int[][] combinations = new int[k][2];
    for (int i = 0; i < k; i++) {
      long key = sorted.get(i).getKey();
      combinations[i][0] = (int) (key >> 8);
      combinations[i][1] = (int) (key & 0xFF);
    }
    return new AlpCompression.AlpEncodingPreset(combinations);
  }
}
