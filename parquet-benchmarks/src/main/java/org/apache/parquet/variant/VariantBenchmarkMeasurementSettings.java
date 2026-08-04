/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.variant;

/**
 * Common measurement settings.
 */
class VariantBenchmarkMeasurementSettings {

  /**
   * How long to warm up. Goal: JIT compiler has done its work.
   * If flamegraphs show compilation: increase.
   */
  static final int SMALL_BENCHMARK_WARMUP = 500;

  /**
   * Meaurement for the small benchmarks.
   */
  static final int SMALL_BENCHMARK_MEASUREMENTS = 500;
}
