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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.util.AutoCloseables;

/**
 * Owns the original allocations for one vectored read. Filesystems may return slices of these buffers and allocate
 * additional buffers for checksums, so the range results do not identify all the allocations that need releasing.
 *
 * <p>The caller must wait for submission and all reads to finish before transferring or closing this owner. Stopping
 * allocations, cancelling a result future, or closing the stream does not establish that the filesystem has stopped
 * using a previously allocated buffer.
 */
final class VectoredReadBufferAllocator implements ByteBufferAllocator, AutoCloseable {
  private final ByteBufferAllocator allocator;
  private final Map<ByteBuffer, Boolean> buffers = new IdentityHashMap<>();
  private volatile boolean acceptingAllocations = true;

  VectoredReadBufferAllocator(ByteBufferAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
  }

  @Override
  public synchronized ByteBuffer allocate(int size) {
    if (!acceptingAllocations) {
      throw new IllegalStateException("Vectored read is no longer accepting buffer allocations");
    }
    ByteBuffer buffer = Objects.requireNonNull(allocator.allocate(size), "allocated buffer");
    // stopAllocating() may run while the delegate is allocating. The already accepted allocation still belongs
    // to this owner and must remain available to the filesystem until the caller establishes completion.
    buffers.put(buffer, Boolean.TRUE);
    return buffer;
  }

  @Override
  public synchronized void release(ByteBuffer buffer) {
    if (buffers.remove(buffer) == null) {
      throw new IllegalArgumentException("Buffer is not owned by this vectored read");
    }
    allocator.release(buffer);
  }

  @Override
  public boolean isDirect() {
    return allocator.isDirect();
  }

  /** Rejects new allocations without waiting for a potentially blocked delegate allocation or releasing buffers. */
  void stopAllocating() {
    acceptingAllocations = false;
  }

  /**
   * Transfers original buffers to a releaser associated with the delegate allocator. This is only valid after
   * successful submission and completion of every range. The owner must not have been aborted or transferred.
   */
  synchronized void transferTo(ByteBufferReleaser releaser) {
    Objects.requireNonNull(releaser, "releaser");
    if (!acceptingAllocations) {
      throw new IllegalStateException("Vectored read buffer ownership has already been stopped or transferred");
    }
    acceptingAllocations = false;
    Iterator<ByteBuffer> iterator = buffers.keySet().iterator();
    while (iterator.hasNext()) {
      releaser.releaseLater(iterator.next());
      iterator.remove();
    }
  }

  /**
   * Releases the still-owned originals exactly once. The caller must establish that no backend work can use them;
   * this method itself does not wait for asynchronous IO. Buffers already transferred to a row group are unaffected.
   */
  @Override
  public synchronized void close() {
    acceptingAllocations = false;
    List<AutoCloseable> releases = new ArrayList<>(buffers.size());
    for (ByteBuffer buffer : buffers.keySet()) {
      releases.add(() -> allocator.release(buffer));
    }
    buffers.clear();
    AutoCloseables.uncheckedClose(releases);
  }
}
