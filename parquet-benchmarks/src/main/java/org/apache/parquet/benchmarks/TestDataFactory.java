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
package org.apache.parquet.benchmarks;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.parquet.io.api.Binary;

/**
 * Utility class for generating test data for dictionary encoding benchmarks.
 */
public final class TestDataFactory {

  /** Number of distinct values for low-cardinality data patterns. */
  public static final int LOW_CARDINALITY_DISTINCT = 100;

  /** Default RNG seed used across benchmarks for deterministic data. */
  public static final long DEFAULT_SEED = 42L;

  private TestDataFactory() {}

  // ---- Integer data generation for encoding benchmarks ----

  /**
   * Generates uniformly random integers using the given seed.
   */
  public static int[] generateRandomInts(int count, long seed) {
    Random random = new Random(seed);
    int[] data = new int[count];
    for (int i = 0; i < count; i++) {
      data[i] = random.nextInt();
    }
    return data;
  }

  /**
   * Generates low-cardinality integers (values drawn from a small set) using the given seed.
   */
  public static int[] generateLowCardinalityInts(int count, int distinctValues, long seed) {
    Random random = new Random(seed);
    int[] data = new int[count];
    for (int i = 0; i < count; i++) {
      data[i] = random.nextInt(distinctValues);
    }
    return data;
  }

  // ---- Long data generation for encoding benchmarks ----

  /**
   * Generates uniformly random longs using the given seed.
   */
  public static long[] generateRandomLongs(int count, long seed) {
    Random random = new Random(seed);
    long[] data = new long[count];
    for (int i = 0; i < count; i++) {
      data[i] = random.nextLong();
    }
    return data;
  }

  /**
   * Generates low-cardinality longs (values drawn from a small set).
   */
  public static long[] generateLowCardinalityLongs(int count, int distinctValues, long seed) {
    Random random = new Random(seed);
    long[] palette = new long[distinctValues];
    for (int i = 0; i < distinctValues; i++) {
      palette[i] = random.nextLong();
    }
    long[] data = new long[count];
    for (int i = 0; i < count; i++) {
      data[i] = palette[random.nextInt(distinctValues)];
    }
    return data;
  }

  // ---- Float data generation for encoding benchmarks ----

  /**
   * Generates uniformly random floats using the given seed.
   */
  public static float[] generateRandomFloats(int count, long seed) {
    Random random = new Random(seed);
    float[] data = new float[count];
    for (int i = 0; i < count; i++) {
      data[i] = random.nextFloat() * 1000.0f;
    }
    return data;
  }

  /**
   * Generates low-cardinality floats (values drawn from a small set).
   */
  public static float[] generateLowCardinalityFloats(int count, int distinctValues, long seed) {
    Random random = new Random(seed);
    float[] palette = new float[distinctValues];
    for (int i = 0; i < distinctValues; i++) {
      palette[i] = random.nextFloat() * 1000.0f;
    }
    float[] data = new float[count];
    for (int i = 0; i < count; i++) {
      data[i] = palette[random.nextInt(distinctValues)];
    }
    return data;
  }

  // ---- Double data generation for encoding benchmarks ----

  /**
   * Generates uniformly random doubles using the given seed.
   */
  public static double[] generateRandomDoubles(int count, long seed) {
    Random random = new Random(seed);
    double[] data = new double[count];
    for (int i = 0; i < count; i++) {
      data[i] = random.nextDouble() * 1000.0;
    }
    return data;
  }

  /**
   * Generates low-cardinality doubles (values drawn from a small set).
   */
  public static double[] generateLowCardinalityDoubles(int count, int distinctValues, long seed) {
    Random random = new Random(seed);
    double[] palette = new double[distinctValues];
    for (int i = 0; i < distinctValues; i++) {
      palette[i] = random.nextDouble() * 1000.0;
    }
    double[] data = new double[count];
    for (int i = 0; i < count; i++) {
      data[i] = palette[random.nextInt(distinctValues)];
    }
    return data;
  }

  // ---- Fixed-length byte array data generation for encoding benchmarks ----

  /**
   * Generates fixed-length byte arrays with the specified cardinality.
   *
   * @param count      number of values
   * @param length     byte length of each value
   * @param distinct   number of distinct values (0 means all unique)
   * @param seed       RNG seed
   */
  public static Binary[] generateFixedLenByteArrays(int count, int length, int distinct, long seed) {
    Random random = new Random(seed);
    if (distinct > 0) {
      Binary[] palette = new Binary[distinct];
      for (int i = 0; i < distinct; i++) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        palette[i] = Binary.fromConstantByteArray(bytes);
      }
      Binary[] data = new Binary[count];
      for (int i = 0; i < count; i++) {
        data[i] = palette[random.nextInt(distinct)];
      }
      return data;
    } else {
      Binary[] data = new Binary[count];
      for (int i = 0; i < count; i++) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        data[i] = Binary.fromConstantByteArray(bytes);
      }
      return data;
    }
  }

  // ---- Binary data generation for encoding benchmarks ----

  /**
   * Generates binary strings of the given length with the specified cardinality.
   *
   * @param count         number of values
   * @param stringLength  length of each string
   * @param distinct      number of distinct values (0 means all unique)
   * @param seed          RNG seed
   * @return array of Binary values
   */
  public static Binary[] generateBinaryData(int count, int stringLength, int distinct, long seed) {
    Random random = new Random(seed);
    Binary[] data = new Binary[count];
    if (distinct > 0) {
      // Pre-generate the distinct values
      Binary[] dictionary = new Binary[distinct];
      for (int i = 0; i < distinct; i++) {
        dictionary[i] = Binary.fromConstantByteArray(
            randomString(stringLength, random).getBytes(StandardCharsets.UTF_8));
      }
      for (int i = 0; i < count; i++) {
        data[i] = dictionary[random.nextInt(distinct)];
      }
    } else {
      // All unique
      for (int i = 0; i < count; i++) {
        data[i] = Binary.fromConstantByteArray(
            randomString(stringLength, random).getBytes(StandardCharsets.UTF_8));
      }
    }
    return data;
  }

  private static String randomString(int length, Random random) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append((char) ('a' + random.nextInt(26)));
    }
    return sb.toString();
  }
}
