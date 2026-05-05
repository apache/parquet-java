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
import java.util.ArrayList;
import java.util.List;

/**
 * Convenient class for releasing {@link java.nio.ByteBuffer} objects with the corresponding allocator;
 */
public class ByteBufferReleaser implements AutoCloseable {

  final ByteBufferAllocator allocator;
  private final List<ByteBuffer> toRelease = new ArrayList<>();

  /**
   * Constructs a new {@link ByteBufferReleaser} instance with the specified {@link ByteBufferAllocator} to be used for
   * releasing the buffers in {@link #close()}.
   *
   * @param allocator the allocator to be used for releasing the buffers
   * @see #releaseLater(ByteBuffer)
   * @see #close()
   */
  public ByteBufferReleaser(ByteBufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Adds a {@link ByteBuffer} object to the list of buffers to be released at {@link #close()}. The specified buffer
   * shall be one that was allocated by the {@link ByteBufferAllocator} of this object.
   *
   * @param buffer the buffer to be released
   */
  public void releaseLater(ByteBuffer buffer) {
    toRelease.add(buffer);
  }

  @Override
  public void close() {
    for (ByteBuffer buf : toRelease) {
      allocator.release(buf);
    }
    toRelease.clear();
  }
}
