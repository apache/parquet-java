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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.util.AutoCloseables;
import org.junit.jupiter.api.Test;

class TestVectoredReadBufferAllocator {
  @Test
  void testTransfersOriginalBuffersAndAdditionalChecksumAllocations() {
    CountingAllocator delegate = new CountingAllocator();
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate);
        VectoredReadBufferAllocator owner = new VectoredReadBufferAllocator(tracking);
        ByteBufferReleaser rowGroup = new ByteBufferReleaser(tracking)) {
      ByteBuffer first = owner.allocate(32);
      ByteBuffer second = owner.allocate(32);
      owner.allocate(8); // A filesystem checksum allocation does not appear in the range results.
      assertEquals(first, second); // Content equality must not collapse distinct original allocations.
      ByteBuffer firstResult = first.slice();
      ByteBuffer secondResult = second.slice();
      assertNotSame(first, firstResult);
      assertNotSame(second, secondResult);

      owner.transferTo(rowGroup);
      owner.close();
      assertEquals(0, delegate.releases.get());
      firstResult.put(0, (byte) 37);
      assertEquals(37, first.get(0));
      assertThrows(IllegalStateException.class, () -> owner.allocate(1));
      assertThrows(IllegalStateException.class, () -> owner.transferTo(rowGroup));

      rowGroup.close();
      assertEquals(3, delegate.releases.get());
    }
    assertEquals(3, delegate.releases.get());
  }

  @Test
  void testStopAllocatingDoesNotReleaseBuffersStillUsedByIo() {
    CountingAllocator delegate = new CountingAllocator();
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate);
        VectoredReadBufferAllocator owner = new VectoredReadBufferAllocator(tracking);
        ByteBufferReleaser rowGroup = new ByteBufferReleaser(tracking)) {
      ByteBuffer buffer = owner.allocate(32);
      owner.stopAllocating();
      owner.stopAllocating();
      assertThrows(IllegalStateException.class, () -> owner.allocate(16));
      assertThrows(IllegalStateException.class, () -> owner.transferTo(rowGroup));
      assertEquals(1, delegate.allocations.get());
      assertEquals(0, delegate.releases.get());
      buffer.putInt(0, 1234); // An already accepted read may still finish after abort.
      assertEquals(1234, buffer.getInt(0));

      owner.close(); // The caller has now established that the read finished.
      owner.close();
      assertEquals(1, delegate.releases.get());
    }
  }

  @Test
  void testBackendReleasedOriginalIsNotReleasedAgain() {
    CountingAllocator delegate = new CountingAllocator();
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate);
        VectoredReadBufferAllocator owner = new VectoredReadBufferAllocator(tracking);
        ByteBufferReleaser rowGroup = new ByteBufferReleaser(tracking)) {
      ByteBuffer original = owner.allocate(32);
      owner.allocate(32);
      assertThrows(IllegalArgumentException.class, () -> owner.release(original.slice()));
      assertEquals(0, delegate.releases.get());
      owner.release(original);
      assertEquals(1, delegate.releases.get());
      assertThrows(IllegalArgumentException.class, () -> owner.release(original));

      owner.transferTo(rowGroup);
      owner.close();
      rowGroup.close();
      assertEquals(2, delegate.releases.get());
    }
  }

  @Test
  void testAbortDoesNotWaitForBlockedDelegateAllocation() throws Exception {
    CountDownLatch allocationStarted = new CountDownLatch(1);
    CountDownLatch allowAllocation = new CountDownLatch(1);
    AtomicInteger releases = new AtomicInteger();
    ByteBufferAllocator delegate = new HeapByteBufferAllocator() {
      @Override
      public ByteBuffer allocate(int size) {
        allocationStarted.countDown();
        try {
          if (!allowAllocation.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Test did not unblock allocation");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
        return super.allocate(size);
      }

      @Override
      public void release(ByteBuffer buffer) {
        releases.incrementAndGet();
      }
    };
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate);
        VectoredReadBufferAllocator owner = new VectoredReadBufferAllocator(tracking)) {
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<ByteBuffer> allocation = executor.submit(() -> owner.allocate(32));
        assertTrue(allocationStarted.await(10, TimeUnit.SECONDS));
        executor.submit(owner::stopAllocating).get(5, TimeUnit.SECONDS);
        assertFalse(allocation.isDone());
        assertEquals(0, releases.get());

        allowAllocation.countDown();
        ByteBuffer buffer = allocation.get(10, TimeUnit.SECONDS);
        buffer.putInt(0, 1234);
        assertEquals(1234, buffer.getInt(0));
        assertEquals(0, releases.get());
        assertThrows(IllegalStateException.class, () -> owner.allocate(32));
        owner.close();
        assertEquals(1, releases.get());
      } finally {
        allowAllocation.countDown();
        owner.stopAllocating();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void testConcurrentAllocationsAndAbortRetainEveryAcceptedOriginal() throws Exception {
    CountingAllocator delegate = new CountingAllocator();
    try (TrackingByteBufferAllocator tracking = TrackingByteBufferAllocator.wrap(delegate);
        VectoredReadBufferAllocator owner = new VectoredReadBufferAllocator(tracking)) {
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch firstAllocation = new CountDownLatch(1);
      ExecutorService executor = Executors.newFixedThreadPool(5);
      try {
        List<Future<?>> workers = new ArrayList<>();
        for (int worker = 0; worker < 4; worker++) {
          workers.add(executor.submit(() -> {
            assertTrue(start.await(10, TimeUnit.SECONDS));
            for (int allocation = 0; allocation < 100; allocation++) {
              try {
                owner.allocate(32);
                firstAllocation.countDown();
              } catch (IllegalStateException stopped) {
                break;
              }
            }
            return null;
          }));
        }
        Future<?> abort = executor.submit(() -> {
          assertTrue(firstAllocation.await(10, TimeUnit.SECONDS));
          owner.stopAllocating();
          return null;
        });
        start.countDown();
        for (Future<?> worker : workers) {
          worker.get(10, TimeUnit.SECONDS);
        }
        abort.get(10, TimeUnit.SECONDS);
        assertTrue(delegate.allocations.get() > 0);
        assertEquals(0, delegate.releases.get());
        assertThrows(IllegalStateException.class, () -> owner.allocate(32));
        owner.close();
        assertEquals(delegate.allocations.get(), delegate.releases.get());
      } finally {
        start.countDown();
        owner.stopAllocating();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void testCloseAttemptsEveryReleaseEvenWhenOneFails() {
    AtomicInteger releases = new AtomicInteger();
    RuntimeException failure = new IllegalStateException("injected release failure");
    ByteBufferAllocator delegate = new HeapByteBufferAllocator() {
      @Override
      public void release(ByteBuffer buffer) {
        if (releases.incrementAndGet() == 1) {
          throw failure;
        }
      }
    };
    VectoredReadBufferAllocator owner = new VectoredReadBufferAllocator(delegate);
    owner.allocate(8);
    owner.allocate(8);
    owner.allocate(8);
    AutoCloseables.ParquetCloseResourceException error =
        assertThrows(AutoCloseables.ParquetCloseResourceException.class, owner::close);
    assertSame(failure, error.getCause());
    assertEquals(3, releases.get());
    owner.close();
    assertEquals(3, releases.get());
  }

  private static final class CountingAllocator extends HeapByteBufferAllocator {
    private final AtomicInteger allocations = new AtomicInteger();
    private final AtomicInteger releases = new AtomicInteger();

    @Override
    public ByteBuffer allocate(int size) {
      allocations.incrementAndGet();
      return super.allocate(size);
    }

    @Override
    public void release(ByteBuffer buffer) {
      releases.incrementAndGet();
    }
  }
}
