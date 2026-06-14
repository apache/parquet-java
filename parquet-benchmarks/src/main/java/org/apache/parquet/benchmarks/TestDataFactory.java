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
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.util.Random;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

/**
 * Utility class for generating test schemas and data for benchmarks.
 */
public final class TestDataFactory {

  /** Default number of rows for file-level benchmarks. */
  public static final int DEFAULT_ROW_COUNT = 100_000;

  /** Default RNG seed used across benchmarks for deterministic data. */
  public static final long DEFAULT_SEED = 42L;

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
    return factory.newGroup()
        .append("int32_field", index)
        .append("int64_field", (long) index * 100)
        .append("float_field", random.nextFloat())
        .append("double_field", random.nextDouble())
        .append("boolean_field", index % 2 == 0)
        .append("binary_field", "value_" + (index % 1000));
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
}
