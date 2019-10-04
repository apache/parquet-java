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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import static org.apache.parquet.benchmarks.BenchmarkConstants.ONE_K;
import static org.apache.parquet.benchmarks.BenchmarkConstants.ONE_MILLION;
import static org.apache.parquet.benchmarks.BenchmarkFiles.configuration;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_CHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_CHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_CHECKSUMS_SNAPPY;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_CHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_CHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_CHECKSUMS_SNAPPY;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_CHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_CHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_CHECKSUMS_SNAPPY;

import java.io.IOException;

@State(Scope.Thread)
public class PageChecksumReadBenchmarks {

  private PageChecksumDataGenerator pageChecksumDataGenerator = new PageChecksumDataGenerator();

  /**
   * This needs to be done exactly once.  To avoid needlessly regenerating the files for reading, they aren't cleaned
   * as part of the benchmark.  If the files exist, a message will be printed and they will not be regenerated.
   */
  @Setup(Level.Trial)
  public void setup() {
    pageChecksumDataGenerator.generateAll();
  }

  private void readFile(Path file, int nRows, boolean verifyChecksums, Blackhole blackhole)
    throws IOException {
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(configuration)
        .usePageChecksumVerification(verifyChecksums)
        .build()) {
      for (int i = 0; i < nRows; i++) {
        Group group = reader.read();
        blackhole.consume(group.getLong("long_field", 0));
        blackhole.consume(group.getBinary("binary_field", 0));
        Group subgroup = group.getGroup("group", 0);
        blackhole.consume(subgroup.getInteger("int_field", 0));
        blackhole.consume(subgroup.getInteger("int_field", 1));
        blackhole.consume(subgroup.getInteger("int_field", 2));
        blackhole.consume(subgroup.getInteger("int_field", 3));
      }
    }
  }

  // 100k rows, uncompressed, GZIP, Snappy

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read100KRowsUncompressedWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_100K_CHECKSUMS_UNCOMPRESSED, 100 * ONE_K, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read100KRowsUncompressedWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_100K_CHECKSUMS_UNCOMPRESSED, 100 * ONE_K, true, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read100KRowsGzipWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_100K_CHECKSUMS_GZIP, 100 * ONE_K, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read100KRowsGzipWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_100K_CHECKSUMS_GZIP, 100 * ONE_K, true, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read100KRowsSnappyWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_100K_CHECKSUMS_SNAPPY, 100 * ONE_K, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read100KRowsSnappyWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_100K_CHECKSUMS_SNAPPY, 100 * ONE_K, true, blackhole);
  }

  // 1M rows, uncompressed, GZIP, Snappy

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsUncompressedWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_1M_CHECKSUMS_UNCOMPRESSED, ONE_MILLION, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsUncompressedWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_1M_CHECKSUMS_UNCOMPRESSED, ONE_MILLION, true, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsGzipWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_1M_CHECKSUMS_GZIP, ONE_MILLION, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsGzipWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_1M_CHECKSUMS_GZIP, ONE_MILLION, true, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsSnappyWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_1M_CHECKSUMS_SNAPPY, ONE_MILLION, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsSnappyWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_1M_CHECKSUMS_SNAPPY, ONE_MILLION, true, blackhole);
  }

  // 10M rows, uncompressed, GZIP, Snappy

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read10MRowsUncompressedWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_10M_CHECKSUMS_UNCOMPRESSED, 10 * ONE_MILLION, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read10MRowsUncompressedWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_10M_CHECKSUMS_UNCOMPRESSED, 10 * ONE_MILLION, true, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read10MRowsGzipWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_10M_CHECKSUMS_GZIP, 10 * ONE_MILLION, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read10MRowsGzipWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_10M_CHECKSUMS_GZIP, 10 * ONE_MILLION, true, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read10MRowsSnappyWithoutVerification(Blackhole blackhole) throws IOException {
    readFile(file_10M_CHECKSUMS_SNAPPY, 10 * ONE_MILLION, false, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read10MRowsSnappyWithVerification(Blackhole blackhole) throws IOException {
    readFile(file_10M_CHECKSUMS_SNAPPY, 10 * ONE_MILLION, true, blackhole);
  }

}
