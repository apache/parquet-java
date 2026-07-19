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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

/**
 * Utility class for generating test schemas and data for benchmarks.
 */
public final class TestDataFactory {

  /** Default number of rows for file-level benchmarks. */
  public static final int DEFAULT_ROW_COUNT = 100_000;

  /** Number of distinct values for low-cardinality data patterns. */
  public static final int LOW_CARDINALITY_DISTINCT = 100;

  /** Default RNG seed used across benchmarks for deterministic data. */
  public static final long DEFAULT_SEED = 42L;

  /** Byte length of the fixed-length byte array field in the benchmark schema. */
  public static final int FLBA_LENGTH = 12;

  /** A standard multi-type schema used by file-level benchmarks. */
  public static final MessageType FILE_BENCHMARK_SCHEMA = Types.buildMessage()
      .required(INT32)
      .named("int32_field")
      .required(INT64)
      .named("int64_field")
      .required(FLOAT)
      .named("float_field")
      .required(DOUBLE)
      .named("double_field")
      .required(BOOLEAN)
      .named("boolean_field")
      .required(BINARY)
      .named("binary_field")
      .required(FIXED_LEN_BYTE_ARRAY)
      .length(FLBA_LENGTH)
      .named("flba_field")
      .named("benchmark_record");

  private TestDataFactory() {}

  /**
   * Creates a {@link SimpleGroupFactory} for the standard benchmark schema.
   */
  public static SimpleGroupFactory newGroupFactory() {
    return new SimpleGroupFactory(FILE_BENCHMARK_SCHEMA);
  }

  /**
   * Generates a single row of benchmark data.
   *
   * @param factory the group factory
   * @param index   the row index (used for deterministic data)
   * @param random  the random source
   * @return a populated Group
   */
  public static Group generateRow(SimpleGroupFactory factory, int index, Random random) {
    byte[] flbaBytes = new byte[FLBA_LENGTH];
    random.nextBytes(flbaBytes);
    return factory.newGroup()
        .append("int32_field", index)
        .append("int64_field", (long) index * 100)
        .append("float_field", random.nextFloat())
        .append("double_field", random.nextDouble())
        .append("boolean_field", index % 2 == 0)
        .append("binary_field", "value_" + (index % 1000))
        .append("flba_field", Binary.fromConstantByteArray(flbaBytes));
  }

  /**
   * Generates a deterministic set of rows for file-level benchmarks.
   */
  public static Group[] generateRows(SimpleGroupFactory factory, int rowCount, long seed) {
    Group[] rows = new Group[rowCount];
    Random random = new Random(seed);
    for (int i = 0; i < rowCount; i++) {
      rows[i] = generateRow(factory, i, random);
    }
    return rows;
  }

  // ---- Integer data generation for encoding benchmarks ----

  /**
   * Generates sequential integers: 0, 1, 2, ...
   */
  public static int[] generateSequentialInts(int count) {
    int[] data = new int[count];
    for (int i = 0; i < count; i++) {
      data[i] = i;
    }
    return data;
  }

  /**
   * Generates uniformly random integers using the given seed.
   */
  public static int[] generateRandomInts(int count, long seed) {
    return generateRandomInts(count, new Random(seed));
  }

  /**
   * Generates uniformly random integers.
   *
   * <p>Note: prefer {@link #generateRandomInts(int, long)} when call ordering between
   * generators in the same setup must not influence the produced data.
   */
  public static int[] generateRandomInts(int count, Random random) {
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
    return generateLowCardinalityInts(count, distinctValues, new Random(seed));
  }

  /**
   * Generates low-cardinality integers (values drawn from a small set).
   */
  public static int[] generateLowCardinalityInts(int count, int distinctValues, Random random) {
    int[] data = new int[count];
    for (int i = 0; i < count; i++) {
      data[i] = random.nextInt(distinctValues);
    }
    return data;
  }

  /**
   * Generates high-cardinality integers (all unique in randomized order) using the given seed.
   */
  public static int[] generateHighCardinalityInts(int count, long seed) {
    return generateHighCardinalityInts(count, new Random(seed));
  }

  /**
   * Generates high-cardinality integers (all unique in randomized order).
   */
  public static int[] generateHighCardinalityInts(int count, Random random) {
    int[] data = generateSequentialInts(count);
    for (int i = count - 1; i > 0; i--) {
      int swapIndex = random.nextInt(i + 1);
      int tmp = data[i];
      data[i] = data[swapIndex];
      data[swapIndex] = tmp;
    }
    return data;
  }

  // ---- Long data generation for encoding benchmarks ----

  /**
   * Generates sequential longs: 0, 1, 2, ...
   */
  public static long[] generateSequentialLongs(int count) {
    long[] data = new long[count];
    for (int i = 0; i < count; i++) {
      data[i] = i;
    }
    return data;
  }

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

  /**
   * Generates high-cardinality longs (all unique, shuffled).
   */
  public static long[] generateHighCardinalityLongs(int count, long seed) {
    Random random = new Random(seed);
    long[] data = generateSequentialLongs(count);
    for (int i = count - 1; i > 0; i--) {
      int swapIndex = random.nextInt(i + 1);
      long tmp = data[i];
      data[i] = data[swapIndex];
      data[swapIndex] = tmp;
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
   * Generates binary strings of the given length with the specified cardinality, using
   * a deterministic seed.
   */
  public static Binary[] generateBinaryData(int count, int stringLength, int distinct, long seed) {
    return generateBinaryData(count, stringLength, distinct, new Random(seed));
  }

  /**
   * Generates binary strings of the given length with the specified cardinality.
   *
   * @param count         number of values
   * @param stringLength  length of each string
   * @param distinct      number of distinct values (0 means all unique)
   * @param random        random source
   * @return array of Binary values
   */
  public static Binary[] generateBinaryData(int count, int stringLength, int distinct, Random random) {
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

  // ---- Sorted data generation for delta encoding benchmarks ----

  /**
   * Generates all-unique binary strings of the given length, sorted in natural
   * ({@link Binary#compareTo}) order. Useful for benchmarking DELTA_BYTE_ARRAY
   * encoding, which benefits from prefix sharing between consecutive values.
   */
  public static Binary[] generateSortedBinaryData(int count, int stringLength, long seed) {
    Binary[] data = generateBinaryData(count, stringLength, 0, seed);
    Arrays.sort(data);
    return data;
  }

  /**
   * Generates all-unique fixed-length byte arrays, sorted in natural
   * ({@link Binary#compareTo}) order. Useful for benchmarking DELTA_BYTE_ARRAY
   * encoding with FIXED_LEN_BYTE_ARRAY values.
   */
  public static Binary[] generateSortedFixedLenByteArrays(int count, int fixedLength, long seed) {
    Binary[] data = generateFixedLenByteArrays(count, fixedLength, 0, seed);
    Arrays.sort(data);
    return data;
  }

  // ---- Variable-length data generation for delta encoding benchmarks ----

  /**
   * Generates all-unique binary strings with lengths uniformly distributed in
   * {@code [1, maxLength]}. Useful for benchmarking DELTA_LENGTH_BYTE_ARRAY
   * encoding, where non-zero length deltas exercise the DELTA_BINARY_PACKED
   * sub-encoding of lengths (unlike uniform-length data where deltas are all zero).
   *
   * @param count     number of values
   * @param maxLength maximum string length (inclusive)
   * @param seed      RNG seed
   */
  public static Binary[] generateVariableLengthBinaryData(int count, int maxLength, long seed) {
    Random random = new Random(seed);
    Binary[] data = new Binary[count];
    for (int i = 0; i < count; i++) {
      int length = 1 + random.nextInt(maxLength);
      data[i] = Binary.fromConstantByteArray(randomString(length, random).getBytes(StandardCharsets.UTF_8));
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
