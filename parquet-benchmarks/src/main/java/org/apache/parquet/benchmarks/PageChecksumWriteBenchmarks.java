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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import static org.apache.parquet.benchmarks.BenchmarkConstants.ONE_K;
import static org.apache.parquet.benchmarks.BenchmarkConstants.ONE_MILLION;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_CHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_NOCHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_CHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_NOCHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_CHECKSUMS_SNAPPY;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_100K_NOCHECKSUMS_SNAPPY;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_CHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_NOCHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_CHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_NOCHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_CHECKSUMS_SNAPPY;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_NOCHECKSUMS_SNAPPY;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_CHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_NOCHECKSUMS_UNCOMPRESSED;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_CHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_NOCHECKSUMS_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_CHECKSUMS_SNAPPY;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_10M_NOCHECKSUMS_SNAPPY;

import java.io.IOException;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.*;

@State(Scope.Thread)
public class PageChecksumWriteBenchmarks {

  private PageChecksumDataGenerator pageChecksumDataGenerator = new PageChecksumDataGenerator();

  @Setup(Level.Iteration)
  public void setup() {
    pageChecksumDataGenerator.cleanup();
  }

  // 100k rows, uncompressed, GZIP, Snappy

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write100KRowsUncompressedWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_100K_NOCHECKSUMS_UNCOMPRESSED, 100 * ONE_K, false, UNCOMPRESSED);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write100KRowsUncompressedWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_100K_CHECKSUMS_UNCOMPRESSED, 100 * ONE_K, true, UNCOMPRESSED);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write100KRowsGzipWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_100K_NOCHECKSUMS_GZIP, 100 * ONE_K, false, GZIP);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write100KRowsGzipWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_100K_CHECKSUMS_GZIP, 100 * ONE_K, true, GZIP);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write100KRowsSnappyWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_100K_NOCHECKSUMS_SNAPPY, 100 * ONE_K, false, SNAPPY);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write100KRowsSnappyWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_100K_CHECKSUMS_SNAPPY, 100 * ONE_K, true, SNAPPY);
  }

  // 1M rows, uncompressed, GZIP, Snappy

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write1MRowsUncompressedWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_1M_NOCHECKSUMS_UNCOMPRESSED, ONE_MILLION, false, UNCOMPRESSED);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write1MRowsUncompressedWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_1M_CHECKSUMS_UNCOMPRESSED, ONE_MILLION, true, UNCOMPRESSED);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write1MRowsGzipWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_1M_NOCHECKSUMS_GZIP, ONE_MILLION, false, GZIP);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write1MRowsGzipWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_1M_CHECKSUMS_GZIP, ONE_MILLION, true, GZIP);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write1MRowsSnappyWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_1M_NOCHECKSUMS_SNAPPY, ONE_MILLION, false, SNAPPY);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write1MRowsSnappyWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_1M_CHECKSUMS_SNAPPY, ONE_MILLION, true, SNAPPY);
  }

  // 10M rows, uncompressed, GZIP, Snappy

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write10MRowsUncompressedWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_10M_NOCHECKSUMS_UNCOMPRESSED, 10 * ONE_MILLION, false, UNCOMPRESSED);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write10MRowsUncompressedWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_10M_CHECKSUMS_UNCOMPRESSED, 10 * ONE_MILLION, true, UNCOMPRESSED);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write10MRowsGzipWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_10M_NOCHECKSUMS_GZIP, 10 * ONE_MILLION, false, GZIP);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write10MRowsGzipWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_10M_CHECKSUMS_GZIP, 10 * ONE_MILLION, true, GZIP);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write10MRowsSnappyWithoutChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_10M_NOCHECKSUMS_SNAPPY, 10 * ONE_MILLION, false, SNAPPY);
  }

  @Benchmark @BenchmarkMode(Mode.SingleShotTime)
  public void write10MRowsSnappyWithChecksums() throws IOException {
    pageChecksumDataGenerator.generateData(file_10M_CHECKSUMS_SNAPPY, 10 * ONE_MILLION, true, SNAPPY);
  }

}
