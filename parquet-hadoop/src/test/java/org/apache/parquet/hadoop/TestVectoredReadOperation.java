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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
public class TestVectoredReadOperation {
  private final List<ExecutorService> executors = new ArrayList<>();

  @AfterEach
  public void stopExecutors() throws InterruptedException {
    for (ExecutorService executor : executors) {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Submission worker did not stop");
    }
  }

  @Test
  public void testTimeoutIncludesBlockedSubmission() throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    CountDownLatch readsPublished = new CountDownLatch(1);
    CountDownLatch finishSubmission = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    TestStream stream = new TestStream((ranges, buffers) -> {
      ByteBuffer buffer = buffers.allocate(8);
      CompletableFuture<ByteBuffer> read = new CompletableFuture<>();
      ranges.get(0).setDataReadFuture(read);
      readsPublished.countDown();
      try {
        if (!finishSubmission.await(5, TimeUnit.SECONDS)) {
          throw new IOException("Test submission was not released");
        }
        read.complete(buffer);
      } catch (InterruptedException e) {
        interrupted.countDown();
        InterruptedIOException failure = new InterruptedIOException("Submission interrupted");
        failure.initCause(e);
        read.completeExceptionally(failure);
        throw failure;
      }
    });
    ExecutorService executor = newExecutor();
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(1), allocator, executor, 50, TimeUnit.MILLISECONDS);
    try {
      TimeoutException failure = awaitSubmissionTimeout(operation);
      await(readsPublished);
      assertFalse(operation.submissionSucceeded());
      assertEquals(0L, operation.remainingNanos());
      operation.abort(failure);

      await(interrupted);
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertEquals(1, stream.closes.get());
      assertFalse(stream.closedDuringSubmission);
      assertEquals(1, allocator.released.size());
    } finally {
      finishSubmission.countDown();
    }
  }

  @Test
  public void testCancelledSubmissionDoesNotMeanBackendHasStopped() throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    CountDownLatch readsPublished = new CountDownLatch(1);
    CountDownLatch finishSubmission = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    TestStream stream = new TestStream((ranges, buffers) -> {
      ByteBuffer buffer = buffers.allocate(8);
      CompletableFuture<ByteBuffer> read = new CompletableFuture<>();
      ranges.get(0).setDataReadFuture(read);
      readsPublished.countDown();
      awaitIgnoringInterrupts(finishSubmission, interrupted);
      buffer.put(0, (byte) 37);
      read.complete(buffer);
    });
    ExecutorService executor = newExecutor();
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(1), allocator, executor, 50, TimeUnit.MILLISECONDS);
    try {
      TimeoutException failure = awaitSubmissionTimeout(operation);
      await(readsPublished);
      assertTimeoutPreemptively(Duration.ofSeconds(2), () -> operation.abort(failure));
      await(interrupted);

      assertTrue(stream.submitting);
      assertEquals(0, stream.closes.get());
      assertTrue(allocator.released.isEmpty());

      finishSubmission.countDown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertFalse(stream.closedDuringSubmission);
      assertEquals(1, stream.closes.get());
      assertEquals(1, allocator.released.size());
      assertEquals((byte) 37, allocator.released.get(0).get(0));
    } finally {
      finishSubmission.countDown();
    }
  }

  @Test
  public void testAbortRejectsAllocationsRequestedByLateSubmission() throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    CountDownLatch readsPublished = new CountDownLatch(1);
    CountDownLatch finishSubmission = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    AtomicReference<RuntimeException> allocationFailure = new AtomicReference<>();
    TestStream stream = new TestStream((ranges, buffers) -> {
      CompletableFuture<ByteBuffer> read = new CompletableFuture<>();
      ranges.get(0).setDataReadFuture(read);
      readsPublished.countDown();
      awaitIgnoringInterrupts(finishSubmission, interrupted);
      try {
        read.complete(buffers.allocate(8));
      } catch (RuntimeException e) {
        allocationFailure.set(e);
        read.completeExceptionally(e);
      }
    });
    ExecutorService executor = newExecutor();
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(1), allocator, executor, 50, TimeUnit.MILLISECONDS);
    try {
      TimeoutException failure = awaitSubmissionTimeout(operation);
      await(readsPublished);
      operation.abort(failure);
      await(interrupted);
      assertEquals(0, stream.closes.get());

      finishSubmission.countDown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertNotNull(allocationFailure.get());
      assertTrue(allocator.allocated.isEmpty());
      assertTrue(allocator.released.isEmpty());
      assertEquals(1, stream.closes.get());
      assertFalse(stream.closedDuringSubmission);
    } finally {
      finishSubmission.countDown();
    }
  }

  @Test
  public void testAbortDoesNotBlockBehindAnAllocationInProgress() throws Exception {
    CountDownLatch allocationStarted = new CountDownLatch(1);
    CountDownLatch finishAllocation = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    RecordingAllocator allocator = new RecordingAllocator() {
      @Override
      public ByteBuffer allocate(int size) {
        allocationStarted.countDown();
        try {
          awaitIgnoringInterrupts(finishAllocation, interrupted);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        return super.allocate(size);
      }
    };
    TestStream stream = new TestStream((ranges, buffers) -> {
      CompletableFuture<ByteBuffer> read = new CompletableFuture<>();
      ranges.get(0).setDataReadFuture(read);
      try {
        read.complete(buffers.allocate(8));
      } catch (RuntimeException e) {
        read.completeExceptionally(e);
      }
    });
    ExecutorService executor = newExecutor();
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(1), allocator, executor, 50, TimeUnit.MILLISECONDS);
    try {
      TimeoutException failure = awaitSubmissionTimeout(operation);
      await(allocationStarted);
      assertTimeoutPreemptively(Duration.ofSeconds(2), () -> operation.abort(failure));
      await(interrupted);
      assertTrue(stream.submitting);
      assertEquals(0, stream.closes.get());

      finishAllocation.countDown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertEquals(1, allocator.allocated.size());
      assertEquals(1, allocator.released.size());
      assertSame(allocator.allocated.get(0), allocator.released.get(0));
      assertEquals(1, stream.closes.get());
      assertFalse(stream.closedDuringSubmission);
    } finally {
      finishAllocation.countDown();
    }
  }

  @Test
  public void testSubmissionUsesTheConsumptionDeadline() throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    CountDownLatch submissionStarted = new CountDownLatch(1);
    CountDownLatch finishSubmission = new CountDownLatch(1);
    TestStream stream = new TestStream((ranges, buffers) -> {
      submissionStarted.countDown();
      awaitIgnoringInterrupts(finishSubmission, new CountDownLatch(1));
      ranges.get(0).setDataReadFuture(CompletableFuture.completedFuture(buffers.allocate(8)));
    });
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    executors.add(scheduler);
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(1), allocator, newExecutor(), 5, TimeUnit.SECONDS);
    try (ByteBufferReleaser releaser = new ByteBufferReleaser(allocator)) {
      long remainingBeforeSubmission = operation.remainingNanos();
      scheduler.schedule(finishSubmission::countDown, 150, TimeUnit.MILLISECONDS);
      operation.awaitSubmission();
      await(submissionStarted);

      assertTrue(operation.submissionSucceeded());
      assertTrue(operation.remainingNanos() <= remainingBeforeSubmission - TimeUnit.MILLISECONDS.toNanos(100));
      operation.transferTo(releaser);
    } finally {
      finishSubmission.countDown();
    }
    assertEquals(1, allocator.released.size());
    assertEquals(0, stream.closes.get());
  }

  @Test
  public void testSuccessTransfersOriginalBuffersInsteadOfFutureViews() throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    AtomicReference<ByteBuffer> futureView = new AtomicReference<>();
    TestStream stream = new TestStream((ranges, buffers) -> {
      ByteBuffer original = buffers.allocate(8);
      ByteBuffer view = original.slice();
      futureView.set(view);
      ranges.get(0).setDataReadFuture(CompletableFuture.completedFuture(view));
    });
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(1), allocator, newExecutor(), 5, TimeUnit.SECONDS);
    try (ByteBufferReleaser releaser = new ByteBufferReleaser(allocator)) {
      operation.awaitSubmission();
      operation.transferTo(releaser);
      assertTrue(allocator.released.isEmpty());
      assertFalse(futureView.get() == allocator.allocated.get(0));
    }
    assertEquals(1, allocator.released.size());
    assertSame(allocator.allocated.get(0), allocator.released.get(0));
    assertEquals(0, stream.closes.get());
  }

  @Test
  public void testCancelledReadFutureDoesNotPermitBufferRelease() throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    TestStream stream = new TestStream((ranges, buffers) -> {
      buffers.allocate(8);
      CompletableFuture<ByteBuffer> read = new CompletableFuture<>();
      ranges.get(0).setDataReadFuture(read);
      read.cancel(false);
    });
    ExecutorService executor = newExecutor();
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(1), allocator, executor, 5, TimeUnit.SECONDS);
    operation.awaitSubmission();
    operation.abort(new IOException("Read was cancelled without stopping backend IO"));

    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    assertEquals(1, stream.closes.get());
    assertEquals(1, allocator.allocated.size());
    assertTrue(allocator.released.isEmpty());
  }

  @Test
  public void testMissingReadFutureDoesNotPermitBufferRelease() throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    TestStream stream = new TestStream((ranges, buffers) -> {
      ByteBuffer original = buffers.allocate(8);
      ranges.get(0).setDataReadFuture(CompletableFuture.completedFuture(original));
      throw new IOException("Rejected before publishing the second range future");
    });
    ExecutorService executor = newExecutor();
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(2), allocator, executor, 5, TimeUnit.SECONDS);
    IOException failure = assertThrows(IOException.class, operation::awaitSubmission);
    assertFalse(operation.submissionSucceeded());
    operation.abort(failure);

    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    assertEquals(1, stream.closes.get());
    assertEquals(1, allocator.allocated.size());
    assertTrue(allocator.released.isEmpty());
  }

  @Test
  public void testPendingReadReleasesBuffersWhenItActuallyFinishes() throws Exception {
    RecordingAllocator allocator = new RecordingAllocator();
    CompletableFuture<ByteBuffer> read = new CompletableFuture<>();
    TestStream stream = new TestStream((ranges, buffers) -> {
      buffers.allocate(8);
      ranges.get(0).setDataReadFuture(read);
    });
    ExecutorService executor = newExecutor();
    VectoredReadOperation operation =
        new VectoredReadOperation(stream, ranges(1), allocator, executor, 5, TimeUnit.SECONDS);
    operation.awaitSubmission();
    operation.abort(new IOException("Another read failed"));

    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    assertEquals(1, stream.closes.get());
    assertTrue(allocator.released.isEmpty());
    assertFalse(read.isCancelled());

    ByteBuffer original = allocator.allocated.get(0);
    original.put(0, (byte) 41);
    read.complete(original);
    await(allocator.bufferReleased);
    assertEquals(1, allocator.released.size());
    assertSame(original, allocator.released.get(0));
  }

  private ExecutorService newExecutor() {
    ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
      Thread thread = new Thread(runnable, "vectored-read-test");
      thread.setDaemon(true);
      return thread;
    });
    executors.add(executor);
    return executor;
  }

  private static List<ParquetFileRange> ranges(int count) {
    List<ParquetFileRange> ranges = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ranges.add(new ParquetFileRange(i * 8L, 8));
    }
    return ranges;
  }

  private static TimeoutException awaitSubmissionTimeout(VectoredReadOperation operation) {
    return assertTimeoutPreemptively(
        Duration.ofSeconds(2), () -> assertThrows(TimeoutException.class, operation::awaitSubmission));
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Backend did not reach the expected state");
  }

  private static void awaitIgnoringInterrupts(CountDownLatch latch, CountDownLatch interrupted) throws IOException {
    while (true) {
      try {
        if (!latch.await(5, TimeUnit.SECONDS)) {
          throw new IOException("Test backend was not released");
        }
        return;
      } catch (InterruptedException e) {
        interrupted.countDown();
      }
    }
  }

  @FunctionalInterface
  private interface Submission {
    void submit(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException;
  }

  private static class TestStream extends DelegatingSeekableInputStream {
    private final Submission submission;
    private final AtomicInteger closes = new AtomicInteger();
    private volatile boolean submitting;
    private volatile boolean closedDuringSubmission;

    private TestStream(Submission submission) {
      super(new ByteArrayInputStream(new byte[0]));
      this.submission = submission;
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
      submitting = true;
      try {
        submission.submit(ranges, allocator);
      } finally {
        submitting = false;
      }
    }

    @Override
    public void close() throws IOException {
      closedDuringSubmission |= submitting;
      closes.incrementAndGet();
      super.close();
    }

    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public void seek(long position) {}
  }

  private static class RecordingAllocator implements ByteBufferAllocator {
    private final List<ByteBuffer> allocated = new CopyOnWriteArrayList<>();
    private final List<ByteBuffer> released = new CopyOnWriteArrayList<>();
    private final CountDownLatch bufferReleased = new CountDownLatch(1);

    @Override
    public ByteBuffer allocate(int size) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      allocated.add(buffer);
      return buffer;
    }

    @Override
    public synchronized void release(ByteBuffer buffer) {
      if (allocated.stream().noneMatch(original -> original == buffer)) {
        throw new IllegalArgumentException("Released a view instead of the original buffer");
      }
      if (released.stream().anyMatch(original -> original == buffer)) {
        throw new IllegalStateException("Released the same buffer twice");
      }
      released.add(buffer);
      bufferReleased.countDown();
    }

    @Override
    public boolean isDirect() {
      return false;
    }
  }
}
