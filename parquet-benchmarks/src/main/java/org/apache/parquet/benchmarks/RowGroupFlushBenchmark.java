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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark measuring row group flush performance and peak buffer memory.
 *
 * <p>Uses a wide schema (20 BINARY columns, 200 bytes each) to produce
 * substantial per-column page buffers. A {@link PeakTrackingAllocator}
 * wraps the heap allocator to precisely track the peak bytes outstanding
 * across all parquet-managed ByteBuffers (independent of JVM GC behavior).
 *
 * <p>The key metric is {@code peakAllocatorMB}: with the interleaved flush
 * optimization, each column's pages are finalized, written, and released
 * before the next column is processed, so peak buffer memory is roughly
 * 1/N of the total row group size (N = number of columns).
 *
 * <p>Writes to {@link BlackHoleOutputFile} to isolate flush cost from
 * filesystem I/O.
 */
@BenchmarkMode({Mode.AverageTime})
@Fork(
    value = 1,
    jvmArgs = {"-Xms512m", "-Xmx1g"})
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class RowGroupFlushBenchmark {

  private static final int COLUMN_COUNT = 20;
  private static final int BINARY_VALUE_LENGTH = 200;
  private static final int ROW_COUNT = 100_000;

  /** Row group sizes: 8MB and 64MB. */
  @Param({"8388608", "67108864"})
  public int rowGroupSize;

  /** Wide schema: 20 required BINARY columns. */
  private static final MessageType WIDE_SCHEMA;

  static {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (int c = 0; c < COLUMN_COUNT; c++) {
      builder.required(PrimitiveTypeName.BINARY).named("col_" + c);
    }
    WIDE_SCHEMA = builder.named("wide_record");
  }

  /** Pre-generated column values (one unique value per column). */
  private Binary[] columnValues;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    columnValues = new Binary[COLUMN_COUNT];
    for (int c = 0; c < COLUMN_COUNT; c++) {
      byte[] value = new byte[BINARY_VALUE_LENGTH];
      random.nextBytes(value);
      columnValues[c] = Binary.fromConstantByteArray(value);
    }
  }

  /**
   * Auxiliary counters reported alongside timing. JMH collects these after
   * each iteration.
   */
  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class MemoryCounters {
    /** Peak bytes outstanding in the parquet ByteBufferAllocator. */
    public long peakAllocatorBytes;

    /** Convenience: peak in MB (peakAllocatorBytes / 1048576). */
    public double peakAllocatorMB;

    @Setup(Level.Iteration)
    public void reset() {
      peakAllocatorBytes = 0;
      peakAllocatorMB = 0;
    }
  }

  /**
   * ByteBufferAllocator wrapper that tracks current and peak allocated bytes.
   * Thread-safe (uses AtomicLong) although the write path is single-threaded.
   */
  static class PeakTrackingAllocator implements ByteBufferAllocator {
    private final ByteBufferAllocator delegate = new HeapByteBufferAllocator();
    private final AtomicLong currentBytes = new AtomicLong();
    private final AtomicLong peakBytes = new AtomicLong();

    @Override
    public ByteBuffer allocate(int size) {
      ByteBuffer buf = delegate.allocate(size);
      long current = currentBytes.addAndGet(buf.capacity());
      peakBytes.accumulateAndGet(current, Math::max);
      return buf;
    }

    @Override
    public void release(ByteBuffer buf) {
      currentBytes.addAndGet(-buf.capacity());
      delegate.release(buf);
    }

    @Override
    public boolean isDirect() {
      return delegate.isDirect();
    }

    long getPeakBytes() {
      return peakBytes.get();
    }
  }

  @Benchmark
  public void writeWithFlush(MemoryCounters counters) throws IOException {
    PeakTrackingAllocator allocator = new PeakTrackingAllocator();
    SimpleGroupFactory factory = new SimpleGroupFactory(WIDE_SCHEMA);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BlackHoleOutputFile.INSTANCE)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(WIDE_SCHEMA)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriterVersion(WriterVersion.PARQUET_1_0)
        .withRowGroupSize(rowGroupSize)
        .withDictionaryEncoding(false)
        .withAllocator(allocator)
        .build()) {
      for (int i = 0; i < ROW_COUNT; i++) {
        Group group = factory.newGroup();
        for (int c = 0; c < COLUMN_COUNT; c++) {
          group.append("col_" + c, columnValues[c]);
        }
        writer.write(group);
      }
    }

    counters.peakAllocatorBytes = allocator.getPeakBytes();
    counters.peakAllocatorMB = allocator.getPeakBytes() / (1024.0 * 1024.0);
  }
}
