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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Limits accepted operations, including submissions which continue after the caller times out. */
final class VectoredReadExecutor extends ThreadPoolExecutor {
  private final Semaphore capacity;

  VectoredReadExecutor(int maximumThreads) {
    super(maximumThreads, maximumThreads, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(maximumThreads), task -> {
      Thread thread = new Thread(task, "parquet-vectored-read");
      thread.setDaemon(true);
      return thread;
    });
    capacity = new Semaphore(maximumThreads, true);
    allowCoreThreadTimeOut(true);
  }

  void execute(Runnable task, long timeoutNanos) throws InterruptedException, TimeoutException {
    if (!capacity.tryAcquire(timeoutNanos, TimeUnit.NANOSECONDS)) {
      throw new TimeoutException("Timed out waiting for vectored read worker capacity");
    }
    try {
      super.execute(() -> {
        try {
          task.run();
        } finally {
          // Cancellation does not release capacity: the backend and cleanup must actually exit.
          capacity.release();
        }
      });
    } catch (RuntimeException | Error failure) {
      capacity.release();
      throw failure;
    }
  }
}
