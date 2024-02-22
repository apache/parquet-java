/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.bytes;

import java.nio.ByteBuffer;

/**
 * A special {@link ByteBufferAllocator} implementation that keeps one {@link ByteBuffer} object and reuse it at the
 * next {@link #allocate(int)} call. The {@link #close()} shall be called when this allocator is not needed anymore to
 * really release the one buffer.
 */
public class ReusingByteBufferAllocator implements ByteBufferAllocator, AutoCloseable {

  private final ByteBufferAllocator allocator;
  private final ByteBufferReleaser releaser = new ByteBufferReleaser(this);
  private ByteBuffer buffer;
  private ByteBuffer bufferOut;

  /**
   * Constructs a new {@link ReusingByteBufferAllocator} object with the specified "parent" allocator to be used for
   * allocating/releasing the one buffer.
   *
   * @param allocator the allocator to be used for allocating/releasing the one buffer
   */
  public ReusingByteBufferAllocator(ByteBufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * A convenience method to get a {@link ByteBufferReleaser} instance already created for this allocator.
   *
   * @return a releaser for this allocator
   */
  public ByteBufferReleaser getReleaser() {
    return releaser;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException if the one buffer was not released yet
   */
  @Override
  public ByteBuffer allocate(int size) {
    if (bufferOut != null) {
      throw new IllegalStateException("The single buffer is not yet released");
    }
    if (buffer == null) {
      bufferOut = buffer = allocator.allocate(size);
    } else if (buffer.capacity() < size) {
      allocator.release(buffer);
      bufferOut = buffer = allocator.allocate(size);
    } else {
      buffer.clear();
      buffer.limit(size);
      bufferOut = buffer.slice();
    }
    return bufferOut;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException    if the one has already been released or never allocated
   * @throws IllegalArgumentException if the specified buffer is not the one allocated by this allocator
   */
  @Override
  public void release(ByteBuffer b) {
    if (bufferOut == null) {
      throw new IllegalStateException("The single buffer has already been released or never allocated");
    }
    if (b != bufferOut) {
      throw new IllegalArgumentException("The buffer to be released is not the one allocated by this allocator");
    }
    bufferOut = null;
  }

  @Override
  public boolean isDirect() {
    return allocator.isDirect();
  }

  @Override
  public void close() {
    if (buffer != null) {
      allocator.release(buffer);
      buffer = null;
      bufferOut = null;
    }
  }
}
