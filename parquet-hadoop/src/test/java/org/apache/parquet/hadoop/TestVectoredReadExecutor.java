/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
public class TestVectoredReadExecutor {
  private final List<ExecutorService> pools = new ArrayList<>();

  @AfterEach
  public void stopPools() throws InterruptedException {
    for (ExecutorService pool : pools) {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  public void testConstructorRequiresExecutorAndPositiveConcurrency() {
    assertThatThrownBy(() -> new VectoredReadExecutor(null, 1))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("executor");
    assertThatThrownBy(() -> new VectoredReadExecutor(Runnable::run, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maximumConcurrentReads must be positive");
    assertThatThrownBy(() -> new VectoredReadExecutor(Runnable::run, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maximumConcurrentReads must be positive");
  }

  @Test
  public void testInlineRejectionRestoresExactlyOnePermit() throws Exception {
    ThreadPoolExecutor pool = VectoredReadExecutor.newThreadPool(1);
    pools.add(pool);
    AtomicBoolean inline = new AtomicBoolean(true);
    VectoredReadExecutor executor = new VectoredReadExecutor(
        task -> {
          if (inline.get()) {
            task.run();
          } else {
            pool.execute(task);
          }
        },
        1);
    AtomicInteger calls = new AtomicInteger();
    for (int retry = 0; retry < 2; retry++) {
      assertThatThrownBy(() -> executor.execute(calls::incrementAndGet, 0))
          .isInstanceOf(RejectedExecutionException.class)
          .hasMessage("Vectored read executor must not execute tasks inline");
    }
    assertThat(calls.get()).isZero();

    inline.set(false);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    try {
      executor.execute(
          () -> {
            calls.incrementAndGet();
            started.countDown();
            awaitUnchecked(finish);
          },
          0);
      await(started);
      assertThatThrownBy(() -> executor.execute(calls::incrementAndGet, 0))
          .isInstanceOf(TimeoutException.class)
          .hasMessage("Timed out waiting for vectored read worker capacity");
      assertThat(calls.get()).isEqualTo(1);
      assertThat(pool.isShutdown()).isFalse();
    } finally {
      finish.countDown();
      pool.submit(() -> null).get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testExecutorCannotHideInlineRejectionInTaskFuture() {
    Executor capturesTaskFailure = task -> new FutureTask<>(task, null).run();
    VectoredReadExecutor executor = new VectoredReadExecutor(capturesTaskFailure, 1);
    AtomicInteger calls = new AtomicInteger();
    for (int retry = 0; retry < 2; retry++) {
      assertThatThrownBy(() -> executor.execute(calls::incrementAndGet, 0))
          .isInstanceOf(RejectedExecutionException.class)
          .hasMessage("Vectored read executor must not execute tasks inline");
    }
    assertThat(calls.get()).isZero();
  }

  @Test
  public void testSaturatedCallerRunsPolicyIsRejected() throws Exception {
    ThreadPoolExecutor pool = new ThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());
    pools.add(pool);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    Future<?> blockingTask = pool.submit(() -> {
      started.countDown();
      awaitUnchecked(finish);
    });
    AtomicInteger calls = new AtomicInteger();
    VectoredReadExecutor executor = new VectoredReadExecutor(pool, 1);
    try {
      await(started);
      for (int retry = 0; retry < 2; retry++) {
        assertThatThrownBy(() -> executor.execute(calls::incrementAndGet, 0))
            .isInstanceOf(RejectedExecutionException.class)
            .hasMessage("Vectored read executor must not execute tasks inline");
      }
      assertThat(calls.get()).isZero();
      assertThat(pool.isShutdown()).isFalse();
    } finally {
      finish.countDown();
      blockingTask.get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testQueuedTaskCanLaterRunOnFormerSubmittingThread() throws Exception {
    AtomicReference<Runnable> queued = new AtomicReference<>();
    VectoredReadExecutor executor = new VectoredReadExecutor(queued::set, 1);
    AtomicInteger calls = new AtomicInteger();
    executor.execute(calls::incrementAndGet, 0);
    assertThat(calls.get()).isZero();
    assertThatThrownBy(() -> executor.execute(calls::incrementAndGet, 0))
        .isInstanceOf(TimeoutException.class)
        .hasMessage("Timed out waiting for vectored read worker capacity");

    queued.get().run();
    assertThat(calls.get()).isEqualTo(1);
    executor.execute(calls::incrementAndGet, 0);
    queued.get().run();
    assertThat(calls.get()).isEqualTo(2);
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
  }

  private static void awaitUnchecked(CountDownLatch latch) {
    try {
      await(latch);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Test worker interrupted before it was released", e);
    }
  }
}
