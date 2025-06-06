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
package org.apache.parquet.bytes;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper {@link ByteBufferAllocator} implementation that tracks whether all allocated buffers are released. It
 * throws the related exception at {@link #close()} if any buffer remains un-released. It also clears the buffers at
 * release so if they continued being used it'll generate errors.
 * <p>To be used for testing purposes.
 */
public final class TrackingByteBufferAllocator implements ByteBufferAllocator, AutoCloseable {

  /**
   * The stacktraces of the allocation are not stored by default because it significantly decreases the unit test
   * execution performance
   *
   * @see ByteBufferAllocationStacktraceException
   */
  private static final boolean DEBUG = true;

  private static final Logger LOG = LoggerFactory.getLogger(TrackingByteBufferAllocator.class);

  public static TrackingByteBufferAllocator wrap(ByteBufferAllocator allocator) {
    return new TrackingByteBufferAllocator(allocator);
  }

  private static class Key {

    private final int hashCode;
    private final ByteBuffer buffer;

    Key(ByteBuffer buffer) {
      hashCode = System.identityHashCode(buffer);
      this.buffer = buffer;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return this.buffer == key.buffer;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      return buffer.toString();
    }
  }

  public static class LeakDetectorHeapByteBufferAllocatorException extends RuntimeException {

    private LeakDetectorHeapByteBufferAllocatorException(String msg) {
      super(msg);
    }

    private LeakDetectorHeapByteBufferAllocatorException(String msg, Throwable cause) {
      super(msg, cause);
    }

    private LeakDetectorHeapByteBufferAllocatorException(
        String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
    }
  }

  public static class ByteBufferAllocationStacktraceException extends LeakDetectorHeapByteBufferAllocatorException {

    private static final ByteBufferAllocationStacktraceException WITHOUT_STACKTRACE =
        new ByteBufferAllocationStacktraceException(false);

    private static ByteBufferAllocationStacktraceException create() {
      return DEBUG ? new ByteBufferAllocationStacktraceException() : WITHOUT_STACKTRACE;
    }

    private ByteBufferAllocationStacktraceException() {
      super("Allocation stacktrace of the first ByteBuffer:");
    }

    private ByteBufferAllocationStacktraceException(boolean unused) {
      super(
          "Set org.apache.parquet.bytes.TrackingByteBufferAllocator.DEBUG = true for more info",
          null,
          false,
          false);
    }
  }

  public static class ReleasingUnallocatedByteBufferException extends LeakDetectorHeapByteBufferAllocatorException {

    private ReleasingUnallocatedByteBufferException() {
      super("Releasing a ByteBuffer instance that is not allocated by this allocator or already been released");
    }
  }

  public static class LeakedByteBufferException extends LeakDetectorHeapByteBufferAllocatorException {

    private LeakedByteBufferException(int count, ByteBufferAllocationStacktraceException e) {
      super(count + " ByteBuffer object(s) is/are remained unreleased after closing this allocator.", e);
    }
  }

  private final Map<Key, ByteBufferAllocationStacktraceException> allocated = new HashMap<>();
  private final ByteBufferAllocator allocator;

  private TrackingByteBufferAllocator(ByteBufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public ByteBuffer allocate(int size) {
    ByteBuffer buffer = allocator.allocate(size);
    final ByteBufferAllocationStacktraceException ex = ByteBufferAllocationStacktraceException.create();
    final Key key = new Key(buffer);
    allocated.put(key, ex);
    LOG.debug("Creating ByteBuffer:{} size {} {}", key.hashCode(), size, buffer);
    if (DEBUG) {
      LOG.debug("Stack", ex);
    }
    return buffer;
  }

  @Override
  public void release(ByteBuffer b) throws ReleasingUnallocatedByteBufferException {
    Objects.requireNonNull(b);
    final Key key = new Key(b);
    LOG.debug("Releasing ByteBuffer: {}: {}", key.hashCode(), b);
    if (allocated.remove(key) == null) {
      throw new ReleasingUnallocatedByteBufferException();
    }
    allocator.release(b);
    // Clearing the buffer so subsequent access would probably generate errors
    b.clear();
  }

  @Override
  public boolean isDirect() {
    return allocator.isDirect();
  }

  @Override
  public void close() throws LeakedByteBufferException {
    if (!allocated.isEmpty()) {
      allocated.keySet().forEach(key ->
          LOG.warn("Unreleased ByteBuffer {}; {}", key.hashCode(), key));
      LeakedByteBufferException ex = new LeakedByteBufferException(
          allocated.size(), allocated.values().iterator().next());
      allocated.clear(); // Drop the references to the ByteBuffers, so they can be gc'd
      throw ex;
    }
  }
}
