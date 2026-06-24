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

import java.util.Random;
import org.apache.parquet.io.api.Binary;

/**
 * Utility class for generating test data for encoding benchmarks.
 */
public final class TestDataFactory {

  /** Default RNG seed used across benchmarks for deterministic data. */
  public static final long DEFAULT_SEED = 42L;

  private TestDataFactory() {}

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
}
