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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Three-way decompression throughput comparison under concurrency, at a fixed 64KB page size (the
 * size used in the design discussion for this fix):
 *
 * <ul>
 *   <li>{@code oldStreamingPath}: the pre-fix {@code NonBlockedDecompressorStream}-based streaming
 *       path.
 *   <li>{@code freshAllocateEachCallPath}: an alternative one-shot design that was considered and
 *       rejected -- decompresses via the standard {@code DirectDecompressor} contract like the
 *       chosen fix, but allocates fresh direct buffers with {@code ByteBuffer.allocateDirect()} on
 *       every call instead of reusing them.
 *   <li>{@code newOneShotPath}: the path actually shipped in this fix -- one-shot via {@code
 *       DirectDecompressor}, with buffers held in a per-decompressor-instance {@code
 *       ReusingByteBufferAllocator} (see {@code CodecFactory.HeapBytesDecompressor}).
 * </ul>
 *
 * <p>Each {@code @Benchmark} method is run at both 1 and 8 threads (JMH {@code @State(Scope
 * .Thread)} gives each thread its own decompressor instance, mirroring how {@code CodecPool} hands
 * out one instance per concurrent task in a real executor). {@code ByteBuffer.allocateDirect()}
 * contends on a JVM-wide accounting lock (effectively {@code Bits.reserveMemory}), so
 * {@code freshAllocateEachCallPath} -- which calls it on every single invocation, on every thread
 * -- is expected to degrade sharply as thread count rises, unlike the other two paths, which each
 * allocate their direct buffers only once per decompressor instance and then reuse them.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@State(Scope.Thread)
public class SnappyLz4RawDecompressConcurrencyBenchmark {

  private static final int PAGE_SIZE = 65536;

  @Param({"SNAPPY", "LZ4_RAW"})
  public String codecName;

  private byte[] compressed;
  private int compressedSize;

  private CompressionCodec hadoopCodec;
  private Decompressor oldDecompressor;

  private CodecFactory codecFactory;
  private BytesInputDecompressor newPathDecompressor;

  private Decompressor freshAllocateDecompressor;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    byte[] uncompressed = new byte[PAGE_SIZE];
    new Random(TestDataFactory.DEFAULT_SEED).nextBytes(uncompressed);

    CompressionCodecName codecEnum = CompressionCodecName.valueOf(codecName);
    codecFactory = new CodecFactory(new Configuration(), PAGE_SIZE);
    BytesInputCompressor compressor = codecFactory.getCompressor(codecEnum);
    compressed = compressor.compress(BytesInput.from(uncompressed)).toByteArray();
    compressedSize = compressed.length;

    hadoopCodec = "SNAPPY".equals(codecName) ? new SnappyCodec() : new Lz4RawCodec();
    ((Configurable) hadoopCodec).setConf(new Configuration());
    oldDecompressor = hadoopCodec.createDecompressor();
    freshAllocateDecompressor = hadoopCodec.createDecompressor();

    newPathDecompressor = codecFactory.getDecompressor(codecEnum);
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(1)
  public void oldStreamingPath_t1(Blackhole bh) throws Exception {
    oldStreamingPath(bh);
  }

  @Benchmark
  @Threads(8)
  @OperationsPerInvocation(1)
  public void oldStreamingPath_t8(Blackhole bh) throws Exception {
    oldStreamingPath(bh);
  }

  private void oldStreamingPath(Blackhole bh) throws Exception {
    oldDecompressor.reset();
    InputStream is =
        hadoopCodec.createInputStream(new ByteArrayInputStream(compressed, 0, compressedSize), oldDecompressor);
    byte[] destination = new byte[PAGE_SIZE];
    new DataInputStream(is).readFully(destination);
    bh.consume(destination);
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(1)
  public void freshAllocateEachCallPath_t1(Blackhole bh) throws Exception {
    freshAllocateEachCallPath(bh);
  }

  @Benchmark
  @Threads(8)
  @OperationsPerInvocation(1)
  public void freshAllocateEachCallPath_t8(Blackhole bh) throws Exception {
    freshAllocateEachCallPath(bh);
  }

  private void freshAllocateEachCallPath(Blackhole bh) throws Exception {
    freshAllocateDecompressor.reset();
    ByteBuffer compressedBuf = ByteBuffer.allocateDirect(compressedSize);
    compressedBuf.put(compressed, 0, compressedSize);
    compressedBuf.flip();
    ByteBuffer uncompressedBuf = ByteBuffer.allocateDirect(PAGE_SIZE);
    ((DirectDecompressor) freshAllocateDecompressor).decompress(compressedBuf, uncompressedBuf);
    byte[] destination = new byte[PAGE_SIZE];
    uncompressedBuf.rewind();
    uncompressedBuf.get(destination);
    bh.consume(destination);
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(1)
  public void newOneShotPath_t1(Blackhole bh) throws Exception {
    newOneShotPath(bh);
  }

  @Benchmark
  @Threads(8)
  @OperationsPerInvocation(1)
  public void newOneShotPath_t8(Blackhole bh) throws Exception {
    newOneShotPath(bh);
  }

  private void newOneShotPath(Blackhole bh) throws Exception {
    bh.consume(newPathDecompressor.decompress(BytesInput.from(compressed, 0, compressedSize), PAGE_SIZE));
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    codecFactory.release();
  }
}
