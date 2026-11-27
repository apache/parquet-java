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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.codec.Lz4RawCodec;
import org.apache.parquet.hadoop.codec.SnappyCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares the pre-fix streaming decompression path ({@code NonBlockedDecompressorStream} reading
 * compressed bytes through Hadoop's generic streaming {@code Decompressor} API) against the
 * one-shot {@code DirectDecompressor} path {@code CodecFactory.HeapBytesDecompressor} now uses
 * (GH-3996 / GH-3997), for SNAPPY and LZ4_RAW across a range of page sizes.
 *
 * <p>{@code oldStreamingPath} reproduces the exact pre-fix behavior by directly driving the
 * still-present, unmodified {@code SnappyCodec}/{@code Lz4RawCodec}
 * {@code createInputStream}/{@code Decompressor} machinery the same way
 * {@code CodecFactory.HeapBytesDecompressor} used to before this fix. {@code newOneShotPath} calls
 * the real, current {@code CodecFactory} API.
 *
 * <p>Page sizes span typical small pages (8KB-1MB) up to the large per-row binary values (8-20MB
 * images) that originally motivated this fix (see #3478, and the Spark benchmark against ORC that
 * surfaced it).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@State(Scope.Thread)
public class SnappyLz4RawDecompressBenchmark {

  @Param({"SNAPPY", "LZ4_RAW"})
  public String codecName;

  @Param({"8192", "65536", "1048576", "8388608", "20971520"})
  public int pageSize;

  private byte[] compressed;
  private int compressedSize;

  private CompressionCodec hadoopCodec;
  private Decompressor oldDecompressor;

  private CodecFactory codecFactory;
  private BytesInputDecompressor newPathDecompressor;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    byte[] uncompressed = new byte[pageSize];
    new Random(TestDataFactory.DEFAULT_SEED).nextBytes(uncompressed);

    CompressionCodecName codecEnum = CompressionCodecName.valueOf(codecName);
    codecFactory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = codecFactory.getCompressor(codecEnum);
    compressed = compressor.compress(BytesInput.from(uncompressed)).toByteArray();
    compressedSize = compressed.length;

    hadoopCodec = "SNAPPY".equals(codecName) ? new SnappyCodec() : new Lz4RawCodec();
    ((Configurable) hadoopCodec).setConf(new Configuration());
    oldDecompressor = hadoopCodec.createDecompressor();

    newPathDecompressor = codecFactory.getDecompressor(codecEnum);
  }

  /**
   * Pre-fix behavior: streams compressed bytes through NonBlockedDecompressorStream. Allocates a
   * fresh destination array per call -- matching what the real pre-fix CodecFactory path actually
   * did via {@code StreamBytesInput.toByteArray()}'s {@code new byte[byteCount]} -- so this isn't
   * unfairly advantaged over newOneShotPath, which must also return an independently-owned array
   * each call.
   */
  @Benchmark
  @OperationsPerInvocation(1)
  public void oldStreamingPath(Blackhole bh) throws Exception {
    oldDecompressor.reset();
    InputStream is =
        hadoopCodec.createInputStream(new ByteArrayInputStream(compressed, 0, compressedSize), oldDecompressor);
    byte[] destination = new byte[pageSize];
    new DataInputStream(is).readFully(destination);
    bh.consume(destination);
  }

  /**
   * Current behavior: CodecFactory's one-shot DirectDecompressor path. Consumes the returned
   * BytesInput directly rather than calling toByteArray() on it: the decompressed bytes are
   * already materialized into an owned byte[] inside decompress() itself (see
   * CodecFactory.HeapBytesDecompressor), so calling toByteArray() again here would charge this
   * benchmark for an extra, avoidable copy that oldStreamingPath's direct readFully(destination)
   * never pays either.
   */
  @Benchmark
  @OperationsPerInvocation(1)
  public void newOneShotPath(Blackhole bh) throws Exception {
    bh.consume(newPathDecompressor.decompress(BytesInput.from(compressed, 0, compressedSize), pageSize));
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    codecFactory.release();
  }
}
