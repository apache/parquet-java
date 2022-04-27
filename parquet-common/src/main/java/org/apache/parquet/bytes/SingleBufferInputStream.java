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

import java.nio.BufferUnderflowException;
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
    this.buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
  }

  SingleBufferInputStream(ByteBuffer buffer, int start, int length) {
    // duplicate the buffer because its state will be modified
    this.buffer = buffer.duplicate();
    this.startPosition = start;
    this.buffer.position(start);
    this.buffer.limit(start + length);
    this.buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
  }

  SingleBufferInputStream(byte[] inBuf) {
    this.buffer = ByteBuffer.wrap(inBuf);
    this.buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    this.startPosition = 0;
  }

  SingleBufferInputStream(byte[] inBuf, int start, int length) {
    this.buffer = ByteBuffer.wrap(inBuf, start, length);
    this.buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    // This seems to be consistent with HeapByteBuffer.wrap(), which leaves
    // the internal "offset" at zero and sets the starting position at start.
    this.startPosition = 0;
  }

  SingleBufferInputStream(List<ByteBuffer> inBufs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long position() {
    // position is relative to the start of the stream, not the buffer
    return buffer.position() - startPosition;
  }

  /*
  For all read methods, if we read off the end of the ByteBuffer, BufferUnderflowException is thrown, which
  we catch and turn into an EOFException. This is measured to be faster than explicitly checking if the ByteBuffer
  has any remaining bytes.
   */
  @Override
  public int read() throws IOException {
    try {
      return buffer.get() & 0xFF;
    } catch (BufferUnderflowException e) {
      throw new EOFException(e.getMessage());
    }
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
  public void readFully(byte[] bytes, int offset, int length) throws IOException {
    try {
      buffer.get(bytes, offset, length);
    } catch (BufferUnderflowException|IndexOutOfBoundsException e) {
      throw new EOFException(e.getMessage());
    }
  }

  @Override
  public long skip(long n) {
    if (n < 0) {
      throw new IllegalArgumentException();
    }
    
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
  public void skipFully(long n) throws IOException {
    if (n < 0 || n > Integer.MAX_VALUE) {
      throw new IllegalArgumentException();
    }
    
    try {
      buffer.position(buffer.position() + (int)n);
    } catch (IllegalArgumentException e) {
      // Be sure to leave buffer in EOF state
      buffer.position(buffer.limit());
      throw new EOFException(e.getMessage());
    }
  }

  @Override
  public int read(ByteBuffer out) {
    int bytesToCopy;
    ByteBuffer copyBuffer;
    if (buffer.remaining() <= out.remaining()) {
      // copy all of the buffer
      bytesToCopy = buffer.remaining();
      copyBuffer = buffer;
    } else {
      // copy a slice of the current buffer
      bytesToCopy = out.remaining();
      copyBuffer = buffer.duplicate();
      copyBuffer.limit(buffer.position() + bytesToCopy);
      buffer.position(buffer.position() + bytesToCopy);
    }

    out.put(copyBuffer);
    out.flip();

    return bytesToCopy;
  }

  @Override
  public ByteBuffer slice(int length) throws EOFException {
    if (buffer.remaining() < length) {
      throw new EOFException();
    }

    // length is less than remaining, so it must fit in an int
    ByteBuffer copy = buffer.duplicate();
    copy.limit(copy.position() + length);
    buffer.position(buffer.position() + length);

    return copy;
  }

  @Override
  public List<ByteBuffer> sliceBuffers(long length) throws EOFException {
    if (length == 0) {
      return Collections.emptyList();
    }

    if (length > buffer.remaining()) {
      throw new EOFException();
    }

    // length is less than remaining, so it must fit in an int
    return Collections.singletonList(slice((int) length));
  }

  @Override
  public List<ByteBuffer> remainingBuffers() {
    if (buffer.remaining() <= 0) {
      return Collections.emptyList();
    }

    ByteBuffer remaining = buffer.duplicate();
    buffer.position(buffer.limit());

    return Collections.singletonList(remaining);
  }

  @Override
  public ByteBufferInputStream remainingStream() {
    // Constructor makes duplicate, so we don't have to explicitly make a duplicate here
    ByteBufferInputStream remaining = new SingleBufferInputStream(buffer);
    buffer.position(buffer.limit());
    return remaining;
  }

  @Override
  public ByteBufferInputStream sliceStream(long length) throws EOFException {
    if (length > buffer.remaining()) throw new EOFException();

    ByteBufferInputStream remaining = new SingleBufferInputStream(buffer, buffer.position(), (int)length);
    buffer.position(buffer.position() + (int)length);
    return remaining;
  }

  @Override
  public void mark(int readlimit) {
    this.mark = buffer.position();
  }

  @Override
  public void reset() throws IOException {
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

  @Override
  public byte readByte() throws IOException {
    try {
      return buffer.get();
    } catch (BufferUnderflowException e) {
      throw new EOFException(e.getMessage());
    }
  }

  @Override
  public int readUnsignedByte() throws IOException {
    try {
      return buffer.get() & 0xFF;
    } catch (BufferUnderflowException e) {
      throw new EOFException(e.getMessage());
    }
  }

  @Override
  public short readShort() throws IOException {
    try {
      return buffer.getShort();
    } catch (BufferUnderflowException e) {
      throw new EOFException(e.getMessage());
    }
  }

  @Override
  public int readUnsignedShort() throws IOException {
    try {
      return buffer.getShort() & 0xFFFF;
    } catch (BufferUnderflowException e) {
      throw new EOFException(e.getMessage());
    }
  }

  /*
  Use ByteBuffer.getInt(), which takes advantage of platform intrinsics
  */
  @Override
  public int readInt() throws IOException {
    try {
      return buffer.getInt();
    } catch (BufferUnderflowException e) {
      throw new EOFException(e.getMessage());
    }
  }

  /*
  Use ByteBuffer.getLonmg(), which takes advantage of platform intrinsics
  */
  @Override
  public long readLong() throws IOException {
    try {
      return buffer.getLong();
    } catch (BufferUnderflowException e) {
      throw new EOFException(e.getMessage());
    }
  }
}
