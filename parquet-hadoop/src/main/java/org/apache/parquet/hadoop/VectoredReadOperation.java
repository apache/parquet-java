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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
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
 * The executor must have a single worker: queued failure cleanup must not overtake a
 * submission which is still running after its Future has been cancelled.
 */
final class VectoredReadOperation {
  private static final Logger LOG = LoggerFactory.getLogger(VectoredReadOperation.class);

  private final SeekableInputStream stream;
  private final List<ParquetFileRange> ranges;
  private final VectoredReadBufferAllocator allocator;
  private final ExecutorService executor;
  private final long timeoutNanos;
  private final long readStart = System.nanoTime();
  private Future<Void> submission;
  private volatile boolean submissionSucceeded;
  private boolean aborted;
  private boolean releaseRegistered;

  VectoredReadOperation(
      SeekableInputStream stream,
      List<ParquetFileRange> ranges,
      ByteBufferAllocator allocator,
      ExecutorService executor,
      long timeout,
      TimeUnit unit) {
    this.stream = stream;
    this.ranges = ranges;
    this.allocator = new VectoredReadBufferAllocator(allocator);
    this.executor = executor;
    this.timeoutNanos = unit.toNanos(timeout);
  }

  void awaitSubmission() throws IOException, TimeoutException {
    submission = executor.submit(() -> {
      stream.readVectored(ranges, allocator);
      submissionSucceeded = true;
      return null;
    });
    FutureIO.awaitFuture(submission, remainingNanos(), TimeUnit.NANOSECONDS);
  }

  boolean submissionSucceeded() {
    return submissionSucceeded;
  }

  long remainingNanos() {
    return Math.max(timeoutNanos - (System.nanoTime() - readStart), 0L);
  }

  void transferTo(ByteBufferReleaser releaser) {
    if (!submissionSucceeded || aborted) {
      throw new IllegalStateException("Cannot transfer buffers from an unsuccessful vectored read");
    }
    for (ParquetFileRange range : ranges) {
      CompletableFuture<ByteBuffer> future = range.getDataReadFuture();
      if (future == null || !future.isDone() || future.isCompletedExceptionally()) {
        throw new IllegalStateException("Cannot transfer buffers before all vectored reads succeed");
      }
    }
    allocator.transferTo(releaser);
  }

  /**
   * Stop the caller's wait without treating interruption as proof that backend IO stopped.
   * Once aborted, the reader must not reuse the stream or this executor.
   */
  void abort(Throwable failure) {
    if (aborted) {
      return;
    }
    aborted = true;
    allocator.stopAllocating();
    if (submission != null) {
      submission.cancel(true);
    }
    if (submissionSucceeded) {
      // All futures are published. If the failed read and its siblings already
      // finished, reclaim their buffers before the caller closes its allocator.
      releaseWhenReadsFinish(failure);
    }
    try {
      // Future.cancel(true) may return while the callable is still running. A task on
      // the same single worker cannot close the stream until that callable has exited.
      executor.execute(() -> {
        releaseWhenReadsFinish(failure);
        try {
          stream.close();
        } catch (IOException | RuntimeException closeFailure) {
          if (failure != closeFailure) {
            failure.addSuppressed(closeFailure);
          }
          LOG.warn("Failed to close a stream after a vectored read failure", closeFailure);
        }
      });
    } catch (RejectedExecutionException cleanupFailure) {
      // The reader owns this executor and must not shut it down before queuing cleanup.
      // Do not mask the read error or recycle buffers whose IO lifetime is now unknown.
      failure.addSuppressed(cleanupFailure);
      LOG.warn("Could not schedule cleanup after a vectored read failure", cleanupFailure);
    } finally {
      // shutdownNow would discard the queued cleanup or interrupt its close operation.
      executor.shutdown();
    }
  }

  private void releaseWhenReadsFinish(Throwable failure) {
    if (releaseRegistered) {
      return;
    }
    releaseRegistered = true;
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
