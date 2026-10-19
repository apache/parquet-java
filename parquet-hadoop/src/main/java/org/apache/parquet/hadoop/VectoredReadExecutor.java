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

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shares bounded vectored-read admission across readers using a caller-owned executor.
 *
 * <p>Reuse the same instance for readers which should share a concurrency limit. An accepted operation retains its
 * capacity until submission and the reader's ownership decision finish, including failure cleanup. A backend which
 * ignores interruption therefore continues to consume capacity after the reader times out.
 *
 * <p>The executor must dispatch asynchronously and return promptly from {@link Executor#execute(Runnable)}, either
 * accepting a task or throwing {@link RejectedExecutionException} without accepting it. It must not silently discard
 * accepted tasks. Inline execution, including a saturated
 * {@link ThreadPoolExecutor.CallerRunsPolicy}, is rejected because a worker waits for the calling reader to finish.
 * Reader calls must not occupy all the threads of the executor used for their vectored reads.
 *
 * <p>Parquet never shuts down a supplied executor. Its owner must allow accepted tasks, including deferred cleanup,
 * to finish; use orderly shutdown rather than discarding queued tasks or assuming that interruption ends backend IO.
 */
public final class VectoredReadExecutor {
  private final Executor executor;
  private final Semaphore capacity;

  /**
   * Wraps an executor with a concurrency limit shared by all readers using this instance.
   *
   * @param executor caller-owned executor satisfying the asynchronous execution and lifecycle requirements above
   * @param maximumConcurrentReads maximum number of accepted operations, including queued work and deferred cleanup
   * @throws NullPointerException if {@code executor} is null
   * @throws IllegalArgumentException if {@code maximumConcurrentReads} is not positive
   */
  public VectoredReadExecutor(Executor executor, int maximumConcurrentReads) {
    this.executor = Objects.requireNonNull(executor, "executor");
    if (maximumConcurrentReads <= 0) {
      throw new IllegalArgumentException("maximumConcurrentReads must be positive");
    }
    capacity = new Semaphore(maximumConcurrentReads, true);
  }

  VectoredReadExecutor(int maximumThreads) {
    this(newThreadPool(maximumThreads), maximumThreads);
  }

  static ThreadPoolExecutor newThreadPool(int maximumThreads) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        maximumThreads,
        maximumThreads,
        60L,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(maximumThreads),
        task -> {
          Thread thread = new Thread(task, "parquet-vectored-read");
          thread.setDaemon(true);
          return thread;
        });
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  void execute(Runnable task, long timeoutNanos) throws InterruptedException, TimeoutException {
    Thread submittingThread = Thread.currentThread();
    AtomicBoolean dispatching = new AtomicBoolean(true);
    AtomicBoolean released = new AtomicBoolean();
    AtomicReference<RejectedExecutionException> inlineFailure = new AtomicReference<>();
    Runnable release = () -> {
      if (released.compareAndSet(false, true)) {
        capacity.release();
      }
    };
    if (!capacity.tryAcquire(timeoutNanos, TimeUnit.NANOSECONDS)) {
      throw new TimeoutException("Timed out waiting for vectored read worker capacity");
    }
    try {
      executor.execute(() -> {
        if (Thread.currentThread() == submittingThread && dispatching.get()) {
          RejectedExecutionException failure =
              new RejectedExecutionException("Vectored read executor must not execute tasks inline");
          inlineFailure.set(failure);
          throw failure;
        }
        try {
          task.run();
        } finally {
          // Cancellation does not release capacity: the backend and cleanup must actually exit.
          release.run();
        }
      });
      // Some executors capture task failures instead of propagating them from execute().
      if (inlineFailure.get() != null) {
        throw inlineFailure.get();
      }
    } catch (RuntimeException | Error failure) {
      release.run();
      throw failure;
    } finally {
      // A queued task can later run on the former submitting thread after its read has timed out.
      dispatching.set(false);
    }
  }
}
