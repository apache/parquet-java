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
package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verify MemoryManager could adjust its writers' allocated memory size.
 */
public class TestMemoryManager {

  Configuration conf = new Configuration();
  String writeSchema = "message example {\n" + "required int32 line;\n" + "required binary content;\n" + "}";
  long expectedPoolSize;
  ParquetOutputFormat parquetOutputFormat;
  int counter = 0;

  @BeforeEach
  public void setUp() throws Exception {
    parquetOutputFormat = new ParquetOutputFormat(new GroupWriteSupport());

    GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), conf);
    expectedPoolSize = Math.round((double)
            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()
        * MemoryManager.DEFAULT_MEMORY_POOL_RATIO);

    long rowGroupSize = expectedPoolSize / 2;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    // the memory manager is not initialized until a writer is created
    createWriter(0).close(null);
  }

  @Test
  public void testMemoryManagerUpperLimit() {
    // Verify the memory pool size
    // this value tends to change a little between setup and tests, so this
    // validates that it is within 15% of the expected value
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    assertThat(Math.abs(expectedPoolSize - poolSize))
        .as("Pool size should be within 15% of the expected value" + " (expected = " + expectedPoolSize
            + " actual = " + poolSize + ")")
        .isLessThan((long) (expectedPoolSize * 0.15));
  }

  @Test
  public void testMemoryManager() throws Exception {
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    long rowGroupSize = poolSize / 2;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    assertThat(2 * rowGroupSize).as("Pool should hold 2 full row groups").isLessThanOrEqualTo(poolSize);
    assertThat(poolSize).as("Pool should not hold 3 full row groups").isLessThan(3 * rowGroupSize);

    assertThat(getTotalAllocation()).as("Allocations should start out at 0").isEqualTo(0);

    RecordWriter writer1 = createWriter(1);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);
    assertThat(getTotalAllocation())
        .as("First writer should be limited by row group size")
        .isEqualTo(rowGroupSize);

    RecordWriter writer2 = createWriter(2);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);
    assertThat(getTotalAllocation())
        .as("Second writer should be limited by row group size")
        .isEqualTo(2 * rowGroupSize);

    RecordWriter writer3 = createWriter(3);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);

    writer1.close(null);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);
    assertThat(getTotalAllocation())
        .as("Allocations should be increased to the row group size")
        .isEqualTo(2 * rowGroupSize);

    writer2.close(null);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);
    assertThat(getTotalAllocation())
        .as("Allocations should be increased to the row group size")
        .isEqualTo(rowGroupSize);

    writer3.close(null);
    assertThat(getTotalAllocation())
        .as("Allocations should be increased to the row group size")
        .isEqualTo(0);
  }

  @Test
  public void testReallocationCallback() throws Exception {
    // validate assumptions
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    long rowGroupSize = poolSize / 2;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    assertThat(2 * rowGroupSize).as("Pool should hold 2 full row groups").isLessThanOrEqualTo(poolSize);
    assertThat(poolSize).as("Pool should not hold 3 full row groups").isLessThan(3 * rowGroupSize);

    Runnable callback = () -> counter++;

    // first-time registration should succeed
    ParquetOutputFormat.getMemoryManager().registerScaleCallBack("increment-test-counter", callback);

    assertThatThrownBy(() -> ParquetOutputFormat.getMemoryManager()
            .registerScaleCallBack("increment-test-counter", callback))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The callBackName increment-test-counter is duplicated and has been registered already.");

    // hit the limit once and clean up
    RecordWriter writer1 = createWriter(1);
    RecordWriter writer2 = createWriter(2);
    RecordWriter writer3 = createWriter(3);
    writer1.close(null);
    writer2.close(null);
    writer3.close(null);

    // Verify Callback mechanism
    assertThat(counter).as("Allocations should be adjusted once").isEqualTo(1);
    assertThat(ParquetOutputFormat.getMemoryManager().getScaleCallBacks())
        .as("Should not allow duplicate callbacks")
        .hasSize(1);
  }

  /**
   * Regression test for PARQUET-2412: many threads concurrently opening and closing writers
   * (as Spark's dynamic partition writer does) must not corrupt MemoryManager's bookkeeping,
   * deadlock, or spuriously trip the duplicate-writer / minimum-allocation guards, even while
   * the pool is sustained well past its limit for most of the run.
   *
   * <p>Note that no records are ever written through these writers: the single GroupWriteSupport
   * instance shared by {@code parquetOutputFormat} is not safe for concurrent record writes (its
   * {@code schema}/{@code groupWriter} fields race), so this test must stay open/close-only.
   */
  @Test
  public void testConcurrentAddRemoveWriters() throws Exception {
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    // Small enough that a handful of concurrently open writers comfortably exceeds the pool,
    // so the run spends most of its time with scale < 1.0, not just the healthy fast path.
    long rowGroupSize = poolSize / 4;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    int threadCount = 20;
    int opsPerThread = 200;
    AtomicInteger nextFileIndex = new AtomicInteger(10_000);
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    AtomicReference<Double> minObservedScale = new AtomicReference<>(1.0);

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threadCount; t++) {
      futures.add(executor.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            RecordWriter writer = createWriter(nextFileIndex.incrementAndGet());
            try {
              // Give other threads a chance to overlap so several writers are open at once,
              // and sample the scale while this writer is live.
              Thread.yield();
              minObservedScale.accumulateAndGet(
                  ParquetOutputFormat.getMemoryManager().getScale(), Math::min);
            } finally {
              writer.close(null);
            }
          }
        } catch (Throwable t2) {
          failures.add(t2);
        }
      }));
    }

    for (Future<?> f : futures) {
      f.get(120, TimeUnit.SECONDS);
    }
    executor.shutdown();

    assertThat(failures)
        .as("No exceptions expected from concurrent add/remove")
        .isEmpty();
    assertThat(ParquetOutputFormat.getMemoryManager().getWriterList())
        .as("All writers should have been removed once every thread finished")
        .isEmpty();
    assertThat(minObservedScale.get())
        .as("The run should have actually exercised the degraded (scale < 1.0) path")
        .isLessThan(1.0);
  }

  /**
   * Regression test for the TreeMap.firstKey()-based minMemoryAllocation guard in
   * MemoryManager#recomputeScale(), which replaced a full scan over every writer. The
   * equivalence (the smallest live allocation is always the first to breach the floor, since
   * scale is a single multiplier) is only argued in a code comment; this pins it down with
   * both a negative and a positive case.
   *
   * <p>It also pins the failure handling around that guard: a writer rejected by the guard
   * never escapes its constructor, so nothing can ever close it — the manager must roll its
   * registration back instead of leaking the allocation, and it must reset {@code scale} to
   * 1.0 once the last writer is removed.
   */
  @Test
  public void testMinimumAllocationGuard() throws Exception {
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    // The shared MemoryManager singleton is created lazily on first use and never
    // reconfigured afterward, so reset it to pick up a custom MIN_MEMORY_ALLOCATION here,
    // and again afterward so this test's custom configuration can't leak into whichever
    // test happens to run next.
    resetStaticMemoryManager();
    try {
      // minAllocation is comfortably below a single writer's own request, so one writer
      // alone is always safe; it only gets breached once enough writers are open at once
      // that scale has to shrink past minAllocation / rowGroupSize (= 0.8) to fit them all.
      long minAllocation = poolSize / 10;
      long rowGroupSize = poolSize / 8;
      conf.setLong(ParquetOutputFormat.MIN_MEMORY_ALLOCATION, minAllocation);
      conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

      // Negative case: a couple of writers stay comfortably under the pool (scale == 1.0),
      // so the floor is never approached and no exception should be thrown.
      RecordWriter safe1 = createWriter(901);
      RecordWriter safe2 = createWriter(902);
      safe1.close(null);
      safe2.close(null);

      // Positive case: enough concurrently open writers that scale must drop below 0.8 to
      // fit them all, which pushes the smallest (here, every) allocation under minAllocation.
      List<RecordWriter> writers = new ArrayList<>();
      try {
        assertThatThrownBy(() -> {
              for (int i = 0; i < 20; i++) {
                writers.add(createWriter(1000 + i));
              }
            })
            .isInstanceOf(ParquetMemoryManagerRuntimeException.class)
            .hasMessageContaining("is smaller than the minimum allocation size");
      } finally {
        for (RecordWriter writer : writers) {
          writer.close(null);
        }
      }

      // The rejected writer never escaped its constructor, so no caller can ever close it;
      // rejecting it must therefore roll the registration back instead of leaking it into
      // this JVM-global manager. A leaked allocation would keep every later writer
      // over-allocated and re-tripping the same exception on this JVM.
      assertThat(ParquetOutputFormat.getMemoryManager().getWriterList())
          .as("The rejected writer must not be leaked in the manager's bookkeeping")
          .isEmpty();

      // The manager must stay usable for later writers instead of being wedged by a leak.
      RecordWriter recovery = createWriter(2000);
      recovery.close(null);

      assertThat(ParquetOutputFormat.getMemoryManager().getScale())
          .as("Scale should return to 1.0 once every writer has been removed")
          .isEqualTo(1.0);
    } finally {
      resetStaticMemoryManager();
    }
  }

  /**
   * ParquetOutputFormat#memoryManager is a process-wide singleton created lazily on the first
   * writer and never reconfigured afterward, so tests that need a specific
   * MIN_MEMORY_ALLOCATION/MEMORY_POOL_RATIO must force it to be rebuilt.
   */
  private static void resetStaticMemoryManager() throws Exception {
    Field field = ParquetOutputFormat.class.getDeclaredField("memoryManager");
    field.setAccessible(true);
    field.set(null, null);
  }

  @TempDir
  private java.nio.file.Path tempDir;

  private RecordWriter createWriter(int index) throws Exception {
    RecordWriter writer = parquetOutputFormat.getRecordWriter(
        conf, new Path(tempDir.resolve(index + ".parquet").toUri()), CompressionCodecName.UNCOMPRESSED);

    return writer;
  }

  private long getTotalAllocation() {
    Set<InternalParquetRecordWriter<?>> writers =
        ParquetOutputFormat.getMemoryManager().getWriterList().keySet();
    long total = 0;
    for (InternalParquetRecordWriter<?> writer : writers) {
      total += writer.getRowGroupSizeThreshold();
    }
    return total;
  }
}
