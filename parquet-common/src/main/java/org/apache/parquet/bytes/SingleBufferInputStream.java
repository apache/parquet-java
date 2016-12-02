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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * This ByteBufferInputStream does not consume the ByteBuffer being passed in, 
 * but will create a slice of the current buffer.
 */
class SingleBufferInputStream extends ByteBufferInputStream {

  private final ByteBuffer buffer;
  private final long startPosition;
  private int mark = -1;

  SingleBufferInputStream(ByteBuffer buffer) {
    // duplicate the buffer because its state will be modified
    this.buffer = buffer.duplicate();
    this.startPosition = buffer.position();
  }

  @Override
  public long position() {
    // position is relative to the start of the stream, not the buffer
    return buffer.position() - startPosition;
  }

  @Override
  public int read() throws IOException {
    if (!buffer.hasRemaining()) {
    	throw new EOFException();
    }
    return buffer.get() & 0xFF; // as unsigned
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }

    int remaining = buffer.remaining();
    if (remaining <= 0) {
      return -1;
    }

    int bytesToRead = Math.min(buffer.remaining(), length);
    buffer.get(bytes, offset, bytesToRead);

    return bytesToRead;
  }
  
  @Override
  public long skip(long n) {
    if (n == 0) {
      return 0;
    }

    if (buffer.remaining() <= 0) {
      return -1;
    }

    // buffer.remaining is an int, so this will always fit in an int
    int bytesToSkip = (int) Math.min(buffer.remaining(), n);
    buffer.position(buffer.position() + bytesToSkip);

    return bytesToSkip;
  }

  @Override
  public List<ByteBuffer> sliceBuffers(long length) throws EOFException {
    if (length > buffer.remaining()) {
      throw new EOFException();
    }

    // length is less than remaining, so it must fit in an int
    ByteBuffer copy = buffer.duplicate();
    copy.limit(copy.position() + (int) length);
    buffer.position(copy.position() + (int) length);

    return Collections.singletonList(copy);
  }

  @Override
  public synchronized void mark(int readlimit) {
    this.mark = buffer.position();
  }

  @Override
  public synchronized void reset() throws IOException {
    if (mark >= 0) {
      buffer.position(mark);
      this.mark = -1;
    } else {
      throw new IOException("No mark defined");
    }
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public int available() {
    return buffer.remaining();
  }
}
