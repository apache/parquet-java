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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.hadoop.util.wrapped.io.FutureIO;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns one vectored submission and its allocations until the reader can take ownership.
 * Its worker retains ownership until submission exits and the caller transfers or aborts the read.
 * Cleanup therefore cannot overtake a backend which continues after interruption.
 */
final class VectoredReadOperation {
  private static final Logger LOG = LoggerFactory.getLogger(VectoredReadOperation.class);

  private final SeekableInputStream stream;
  private final List<ParquetFileRange> ranges;
  private final VectoredReadBufferAllocator allocator;
  private final VectoredReadExecutor executor;
  private final long timeoutNanos;
  private final long readStart = System.nanoTime();
  private final CompletableFuture<Void> submission = new CompletableFuture<>();
  private final CountDownLatch finished = new CountDownLatch(1);
  private Thread submittingThread;
  private boolean submitted;
  private volatile boolean submissionSucceeded;
  private volatile Throwable abortFailure;
  private boolean releaseRegistered;

  private static final class SharedExecutor {
    private static final VectoredReadExecutor INSTANCE = create();

    private static VectoredReadExecutor create() {
      int threads = Integer.parseInt(System.getProperty("parquet.hadoop.vectored.io.threads", "64"));
      if (threads <= 0) {
        throw new IllegalArgumentException("parquet.hadoop.vectored.io.threads must be positive");
      }
      return new VectoredReadExecutor(threads);
    }
  }

  VectoredReadOperation(
      SeekableInputStream stream,
      List<ParquetFileRange> ranges,
      ByteBufferAllocator allocator,
      long timeout,
      TimeUnit unit) {
    this(stream, ranges, allocator, SharedExecutor.INSTANCE, timeout, unit);
  }

  VectoredReadOperation(
      SeekableInputStream stream,
      List<ParquetFileRange> ranges,
      ByteBufferAllocator allocator,
      VectoredReadExecutor executor,
      long timeout,
      TimeUnit unit) {
    this.stream = stream;
    this.ranges = ranges;
    this.allocator = new VectoredReadBufferAllocator(allocator);
    this.executor = executor;
    this.timeoutNanos = unit.toNanos(timeout);
  }

  void awaitSubmission() throws IOException, TimeoutException {
    try {
      executor.execute(this::run, remainingNanos());
      submitted = true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      InterruptedIOException failure =
          new InterruptedIOException("Interrupted waiting for vectored read worker capacity");
      failure.initCause(e);
      throw failure;
    }
    FutureIO.awaitFuture(submission, remainingNanos(), TimeUnit.NANOSECONDS);
  }

  boolean hasSubmission() {
    return submitted;
  }

  private void run() {
    try {
      boolean submit;
      synchronized (this) {
        submit = abortFailure == null;
        if (submit) {
          submittingThread = Thread.currentThread();
        }
      }
      if (submit) {
        try {
          stream.readVectored(ranges, allocator);
          submissionSucceeded = true;
          submission.complete(null);
        } catch (Throwable failure) {
          // Errors must also wake the caller so that it can abort this operation.
          submission.completeExceptionally(failure);
        } finally {
          synchronized (this) {
            submittingThread = null;
          }
        }
      }
      // Keep this worker and its capacity until the caller decides buffer ownership.
      // Abort interrupts only submission; clear any remaining interrupt before cleanup.
      while (true) {
        try {
          finished.await();
          break;
        } catch (InterruptedException ignored) {
          // Interruption alone does not establish that the caller has finished with this operation.
        }
      }
      Throwable failure = abortFailure;
      if (failure != null) {
        releaseWhenReadsFinish(failure);
        try {
          stream.close();
        } catch (IOException | RuntimeException closeFailure) {
          if (failure != closeFailure) {
            failure.addSuppressed(closeFailure);
          }
          LOG.warn("Failed to close a stream after a vectored read failure", closeFailure);
        }
      }
    } finally {
      synchronized (this) {
        submittingThread = null;
      }
    }
  }

  boolean submissionSucceeded() {
    return submissionSucceeded;
  }

  long remainingNanos() {
    return Math.max(timeoutNanos - (System.nanoTime() - readStart), 0L);
  }

  void transferTo(ByteBufferReleaser releaser) {
    if (!submissionSucceeded || abortFailure != null) {
      throw new IllegalStateException("Cannot transfer buffers from an unsuccessful vectored read");
    }
    for (ParquetFileRange range : ranges) {
      CompletableFuture<ByteBuffer> future = range.getDataReadFuture();
      if (future == null || !future.isDone() || future.isCompletedExceptionally()) {
        throw new IllegalStateException("Cannot transfer buffers before all vectored reads succeed");
      }
    }
    allocator.transferTo(releaser);
    finished.countDown();
  }

  /**
   * Stop the caller's wait without treating interruption as proof that backend IO stopped.
   * Once an accepted submission is aborted, the reader must not reuse its stream.
   */
  synchronized void abort(Throwable failure) {
    if (abortFailure != null) {
      return;
    }
    abortFailure = failure;
    allocator.stopAllocating();
    if (submittingThread != null) {
      // The worker clears this reference before returning to the shared executor, so
      // interruption can never reach a later operation which reuses the same thread.
      submittingThread.interrupt();
    }
    if (submissionSucceeded) {
      // Reclaim completed reads before the caller closes its allocator. If submission
      // is still running, only its worker may inspect the final published futures.
      releaseWhenReadsFinish(failure);
    }
    finished.countDown();
  }

  private void releaseWhenReadsFinish(Throwable failure) {
    synchronized (this) {
      if (releaseRegistered) {
        return;
      }
      releaseRegistered = true;
    }
    CompletableFuture<?>[] futures = new CompletableFuture<?>[ranges.size()];
    for (int i = 0; i < ranges.size(); i++) {
      CompletableFuture<ByteBuffer> future = ranges.get(i).getDataReadFuture();
      if (future == null) {
        // A backend may have started IO without publishing its result. Neither closing
        // its stream nor cancelling a future establishes that pooled memory is reusable.
        LOG.debug("Retaining allocations after a vectored submission with unpublished reads");
        return;
      }
      futures[i] = future;
    }
    // A failed submission may also publish a future for work never submitted. Such a
    // future need not complete: retain its ownership without blocking a cleanup worker.
    CompletableFuture.allOf(futures).whenComplete((ignored, readFailure) -> {
      for (CompletableFuture<?> future : futures) {
        if (future.isCancelled()) {
          LOG.debug("Retaining allocations after a cancelled vectored result with unknown IO lifetime");
          return;
        }
      }
      try {
        allocator.close();
      } catch (RuntimeException releaseFailure) {
        if (failure != releaseFailure) {
          failure.addSuppressed(releaseFailure);
        }
        LOG.warn("Failed to release buffers after a vectored read failure", releaseFailure);
      }
    });
  }
}
