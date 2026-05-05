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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.parquet.variant.VariantBenchmarkMeasurementSettings.SMALL_BENCHMARK_MEASUREMENTS;
import static org.apache.parquet.variant.VariantBenchmarkMeasurementSettings.SMALL_BENCHMARK_WARMUP;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark {@link VariantConverters}. These converters are used
 * when reconstructing shredded variants, so their performance
 * and memory consumption is on the critical path of queries reading
 * variants.
 * <p>Run:
 * <pre>
 *   ./run.sh all org.apache.parquet.variant.VariantConverterBenchmark \
 *       -f 1 -foe true -rf json -rff target/results.json
 * </pre>
 * <p>Profile
 * <pre>
 *   java -jar target/parquet-benchmarks.jar VariantProjectionBenchmark \
 *    -prof "async:output=flamegraph;dir=target/perf" -rf json -rff target/results.json
 * </pre>
 * */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = SMALL_BENCHMARK_WARMUP)
@Measurement(iterations = SMALL_BENCHMARK_MEASUREMENTS)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(MICROSECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class VariantConverterBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(VariantConverterBenchmark.class);

  /** Number of iterations within each benchmark.
   * This compensates for the low resolution of ARM CPUs,
   * while also ensuring profile graphs are filled out in detail. */
  private static final int ITERATIONS = 100_000;

  /** How long is the string to convert? */
  @Param({"16", "512", "2048"})
  int stringLength;

  /**
   * An input string, created in setup from random ASCII characters of length {@link #stringLength}
   */
  private String inputString;

  /** {@link #inputString} as a binary. */
  private Binary inputBinary;

  @Setup
  public void setup() {
    byte[] bytes = new byte[stringLength];
    Random random = new Random(42);
    for (int i = 0; i < stringLength; i++) {
      bytes[i] = (byte) ('a' + random.nextInt(26));
    }
    inputString = new String(bytes, StandardCharsets.UTF_8);
    inputBinary = Binary.fromConstantByteArray(bytes);
    LOG.info("Setup: stringLength={}", stringLength);
  }

  /**
   * Benchmark converting a {@link Binary} as a string value and building a {@link Variant}.
   * This exercises the path taken by {@link VariantConverters} when decoding a shredded
   * {@code typed_value} string column.
   */
  @Benchmark
  @OperationsPerInvocation(ITERATIONS)
  public void appendStringAsString(Blackhole blackhole) {
    VariantBuilder builder = new VariantBuilder();
    builder.appendString(inputString);
    blackhole.consume(builder.build());
  }
  /**
   * Benchmark appending a {@link Binary} as a binary value and building a {@link Variant}.
   * This exercises the path taken by {@link VariantConverters} when decoding a shredded
   * {@code typed_value} binary column.
   */
  @Benchmark
  @OperationsPerInvocation(ITERATIONS)
  public void appendBinary(Blackhole blackhole) {
    VariantBuilder builder = new VariantBuilder();
    builder.appendBinary(inputBinary.toByteBuffer());
    blackhole.consume(builder.build());
  }
}
