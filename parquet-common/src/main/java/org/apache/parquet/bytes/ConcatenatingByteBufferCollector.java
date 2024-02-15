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

import static java.lang.String.format;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Alternative to {@link ConcatenatingByteArrayCollector} but using {@link java.nio.ByteBuffer}s allocated by its
 * {@link ByteBufferAllocator}.
 */
public class ConcatenatingByteBufferCollector extends BytesInput implements AutoCloseable {

  private final ByteBufferAllocator allocator;
  private final List<ByteBuffer> slabs = new ArrayList<>();
  private long size = 0;

  /**
   * Constructs a new {@link ConcatenatingByteBufferCollector} instance with the specified allocator.
   *
   * @param allocator to be used for allocating the required {@link ByteBuffer} instances
   */
  public ConcatenatingByteBufferCollector(ByteBufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Collects the content of the specified input. It allocates a new {@link ByteBuffer} instance that can contain all
   * the content.
   *
   * @param bytesInput the input which content is to be collected
   */
  public void collect(BytesInput bytesInput) {
    int inputSize = Math.toIntExact(bytesInput.size());
    ByteBuffer slab = allocator.allocate(inputSize);
    bytesInput.writeInto(slab);
    slab.flip();
    slabs.add(slab);
    size += inputSize;
  }

  @Override
  public void close() {
    for (ByteBuffer slab : slabs) {
      allocator.release(slab);
    }
    slabs.clear();
  }

  @Override
  public void writeAllTo(OutputStream out) throws IOException {
    WritableByteChannel channel = Channels.newChannel(out);
    for (ByteBuffer buffer : slabs) {
      channel.write(buffer.duplicate());
    }
  }

  @Override
  public void writeInto(ByteBuffer buffer) {
    for (ByteBuffer slab : slabs) {
      buffer.put(slab.duplicate());
    }
  }

  @Override
  ByteBuffer getInternalByteBuffer() {
    return slabs.size() == 1 ? slabs.get(0).duplicate() : null;
  }

  @Override
  public long size() {
    return size;
  }

  /**
   * @param prefix a prefix to be used for every new line in the string
   * @return a text representation of the memory usage of this structure
   */
  public String memUsageString(String prefix) {
    return format("%s %s %d slabs, %,d bytes", prefix, getClass().getSimpleName(), slabs.size(), size);
  }
}
