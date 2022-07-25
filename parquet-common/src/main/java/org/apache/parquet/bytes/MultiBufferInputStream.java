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

import org.apache.parquet.ShouldNeverHappenException;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.nio.BufferUnderflowException;

class MultiBufferInputStream extends ByteBufferInputStream {
  private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

  private final List<ByteBuffer> buffers;
  private final long length;

  private Iterator<ByteBuffer> iterator;
  private ByteBuffer current = EMPTY;
  private long position = 0;

  private long mark = -1;
  private long markLimit = 0;
  private List<ByteBuffer> markBuffers = new ArrayList<>();

  MultiBufferInputStream(List<ByteBuffer> buffers) {
    this.buffers = buffers;

    long totalLen = 0;
    for (ByteBuffer buffer : buffers) {
      totalLen += buffer.remaining();
    }
    this.length = totalLen;

    this.iterator = buffers.iterator();

    nextBuffer();
  }

  /**
   * Returns the position in the stream.
   */
  public long position() {
    return position;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }

    if (current == null) {
      return -1;
    }

    long bytesSkipped = 0;
    while (bytesSkipped < n) {
      if (current.remaining() > 0) {
        long bytesToSkip = Math.min(n - bytesSkipped, current.remaining());
        current.position(current.position() + (int) bytesToSkip);
        bytesSkipped += bytesToSkip;
        this.position += bytesToSkip;
      } else if (!nextBuffer()) {
        // there are no more buffers
        return bytesSkipped > 0 ? bytesSkipped : -1;
      }
    }

    return bytesSkipped;
  }

  @Override
  public void skipFully(long n) throws IOException {
    if (current == null || n > length) {
      throw new EOFException();
    }
    
    skip(n);
  }

  @Override
  public int read(ByteBuffer out) {
    int len = out.remaining();
    if (len <= 0) {
      return 0;
    }

    if (current == null) {
      return -1;
    }

    int bytesCopied = 0;
    while (bytesCopied < len) {
      if (current.remaining() > 0) {
        int bytesToCopy;
        ByteBuffer copyBuffer;
        if (current.remaining() <= out.remaining()) {
          // copy all of the current buffer
          bytesToCopy = current.remaining();
          copyBuffer = current;
        } else {
          // copy a slice of the current buffer
          bytesToCopy = out.remaining();
          copyBuffer = current.duplicate();
          copyBuffer.limit(copyBuffer.position() + bytesToCopy);
          current.position(copyBuffer.position() + bytesToCopy);
        }

        out.put(copyBuffer);
        bytesCopied += bytesToCopy;
        this.position += bytesToCopy;

      } else if (!nextBuffer()) {
        // there are no more buffers
        return bytesCopied > 0 ? bytesCopied : -1;
      }
    }

    return bytesCopied;
  }

  @Override
  public ByteBuffer slice(int length) throws EOFException {
    if (length <= 0) {
      return EMPTY;
    }

    if (current == null) {
      throw new EOFException();
    }

    ByteBuffer slice;
    if (length > current.remaining()) {
      // a copy is needed to return a single buffer
      // TODO: use an allocator
      slice = ByteBuffer.allocate(length);
      int bytesCopied = read(slice);
      slice.flip();
      if (bytesCopied < length) {
        throw new EOFException();
      }
    } else {
      slice = current.duplicate();
      slice.limit(slice.position() + length);
      current.position(slice.position() + length);
      this.position += length;
    }

    return slice;
  }

  public List<ByteBuffer> sliceBuffers(long len) throws EOFException {
    if (len <= 0) {
      return Collections.emptyList();
    }

    if (current == null) {
      throw new EOFException();
    }

    List<ByteBuffer> buffers = new ArrayList<>();
    long bytesAccumulated = 0;
    while (bytesAccumulated < len) {
      if (current.remaining() > 0) {
        // get a slice of the current buffer to return
        // always fits in an int because remaining returns an int that is >= 0
        int bufLen = (int) Math.min(len - bytesAccumulated, current.remaining());
        ByteBuffer slice = current.duplicate();
        slice.limit(slice.position() + bufLen);
        buffers.add(slice);
        bytesAccumulated += bufLen;

        // update state; the bytes are considered read
        current.position(current.position() + bufLen);
        this.position += bufLen;
      } else if (!nextBuffer()) {
        // there are no more buffers
        throw new EOFException();
      }
    }

    return buffers;
  }

  public ByteBufferInputStream sliceStream(long length) throws EOFException {
    return ByteBufferInputStream.wrap(sliceBuffers(length));
  }

  @Override
  public List<ByteBuffer> remainingBuffers() {
    if (position >= length) {
      return Collections.emptyList();
    }

    try {
      return sliceBuffers(length - position);
    } catch (EOFException e) {
      throw new RuntimeException(
          "[Parquet bug] Stream is bad: incorrect bytes remaining " +
              (length - position));
    }
  }

  public ByteBufferInputStream remainingStream() {
    return ByteBufferInputStream.wrap(remainingBuffers());
  }

  @Override
  public int read(byte[] bytes, int off, int len) {
    if (len <= 0) {
      if (len < 0) {
        throw new IndexOutOfBoundsException("Read length must be greater than 0: " + len);
      }
      return 0;
    }

    if (current == null) {
      return -1;
    }

    int bytesRead = 0;
    while (bytesRead < len) {
      if (current.remaining() > 0) {
        int bytesToRead = Math.min(len - bytesRead, current.remaining());
        current.get(bytes, off + bytesRead, bytesToRead);
        bytesRead += bytesToRead;
        this.position += bytesToRead;
      } else if (!nextBuffer()) {
        // there are no more buffers
        return bytesRead > 0 ? bytesRead : -1;
      }
    }

    return bytesRead;
  }

  @Override
  public void readFully(byte[] bytes, int off, int len) throws IOException {
    if (len <= 0) {
      if (len < 0) {
        throw new IndexOutOfBoundsException("Read length must be greater than 0: " + len);
      }
      
      return;
    }

    if (current == null || len > length) {
      throw new EOFException();
    }

    int bytesRead = 0;
    while (bytesRead < len) {
      if (current.remaining() > 0) {
        int bytesToRead = Math.min(len - bytesRead, current.remaining());
        current.get(bytes, off + bytesRead, bytesToRead);
        bytesRead += bytesToRead;
        this.position += bytesToRead;
      } else if (!nextBuffer()) {
        // there are no more buffers
        throw new ShouldNeverHappenException();
      }
    }
  }

  @Override
  public int read() throws IOException {
    if (current == null) {
      throw new EOFException();
    }

    this.position += 1;
    while (true) {
      try {
        return current.get() & 0xFF;
      } catch (BufferUnderflowException e) {
        // It has been measured to be faster to rely on ByteBuffer throwing BufferUnderflowException to determine
        // when we're run out of bytes in the current buffer.
        if (!nextBuffer()) {
          // there are no more buffers
          throw new EOFException();
        }
      }
    }
  }

  @Override
  public int available() {
    long remaining = length - position;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) remaining;
    }
  }

  @Override
  public void mark(int readlimit) {
    if (mark >= 0) {
      discardMark();
    }
    this.mark = position;
    this.markLimit = mark + readlimit + 1;
    if (current != null) {
      markBuffers.add(current.duplicate());
    }
  }

  @Override
  public void reset() throws IOException {
    if (mark >= 0 && position < markLimit) {
      this.position = mark;
      // replace the current iterator with one that adds back the buffers that
      // have been used since mark was called.
      this.iterator = concat(markBuffers.iterator(), iterator);
      discardMark();
      nextBuffer(); // go back to the marked buffers
    } else {
      throw new IOException("No mark defined or has read past the previous mark limit");
    }
  }

  private void discardMark() {
    this.mark = -1;
    this.markLimit = 0;
    markBuffers = new ArrayList<>();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  private boolean nextBuffer() {
    if (!iterator.hasNext()) {
      this.current = null;
      return false;
    }

    this.current = iterator.next().duplicate();
    // Have to put the buffer in little endian mode, because it defaults to big endian
    this.current.order(java.nio.ByteOrder.LITTLE_ENDIAN);

    if (mark >= 0) {
      if (position < markLimit) {
        // the mark is defined and valid. save the new buffer
        markBuffers.add(current.duplicate());
      } else {
        // the mark has not been used and is no longer valid
        discardMark();
      }
    }

    return true;
  }

  private static <E> Iterator<E> concat(Iterator<E> first, Iterator<E> second) {
    return new ConcatIterator<>(first, second);
  }

  private static class ConcatIterator<E> implements Iterator<E> {
    private final Iterator<E> first;
    private final Iterator<E> second;
    boolean useFirst = true;

    public ConcatIterator(Iterator<E> first, Iterator<E> second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public boolean hasNext() {
      if (useFirst) {
        if (first.hasNext()) {
          return true;
        } else {
          useFirst = false;
          return second.hasNext();
        }
      }
      return second.hasNext();
    }

    @Override
    public E next() {
      if (useFirst && !first.hasNext()) {
        useFirst = false;
      }

      if (!useFirst && !second.hasNext()) {
        throw new NoSuchElementException();
      }

      if (useFirst) {
        return first.next();
      }

      return second.next();
    }

    @Override
    public void remove() {
      if (useFirst) {
        first.remove();
      }
      second.remove();
    }
  }

  @Override
  public byte readByte() throws IOException {
    return (byte)readUnsignedByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    if (current == null) {
      throw new EOFException();
    }

    this.position += 1;
    while (true) {
      try {
        return current.get() & 0xFF;
      } catch (BufferUnderflowException e) {
        if (!nextBuffer()) {
          // there are no more buffers
          throw new EOFException();
        }
      }
    }
  }

  /**
   * When reading a short will cross a buffer boundary, read one byte at a time.
   * @return a short value
   * @throws IOException
   */
  private int getShortSlow() throws IOException {
    int c0 = readUnsignedByte();
    int c1 = readUnsignedByte();
    return ((c0 << 0) + (c1 << 8));
  }

  public short readShort() throws IOException {
    if (current == null) {
      throw new EOFException();
    }
    
    if (current.remaining() >= Short.BYTES) {
      // If the whole short can be read from the current buffer, use intrinsics
      this.position += Short.BYTES;
      return current.getShort();
    } else {
      // Otherwise get the short one byte at a time
      return (short)getShortSlow();
    }
  }

  public int readUnsignedShort() throws IOException {
    return readShort() & 0xffff;
  }

  /**
   * When reading an int will cross a buffer boundary, read one byte at a time.
   * @return an int value
   * @throws IOException
   */
  private int getIntSlow() throws IOException {
    int c0 = readUnsignedByte();
    int c1 = readUnsignedByte();
    int c2 = readUnsignedByte();
    int c3 = readUnsignedByte();
    return ((c0 << 0) + (c1 << 8)) + ((c2 << 16) + (c3 << 24));
  }

  @Override
  public int readInt() throws IOException {
    if (current == null) {
      throw new EOFException();
    }
    
    if (current.remaining() >= Integer.BYTES) {
      // If the whole int can be read from the current buffer, use intrinsics
      this.position += Integer.BYTES;
      return current.getInt();
    } else {
      // Otherwise get the int one byte at a time
      return getIntSlow();
    }
  }

  /**
   * When reading a long will cross a buffer boundary, read one byte at a time.
   * @return a long value
   * @throws IOException
   */
  private long getLongSlow() throws IOException {
    long ch0 = (long)readUnsignedByte() << 0;
    long ch1 = (long)readUnsignedByte() << 8;
    long ch2 = (long)readUnsignedByte() << 16;
    long ch3 = (long)readUnsignedByte() << 24;
    long ch4 = (long)readUnsignedByte() << 32;
    long ch5 = (long)readUnsignedByte() << 40;
    long ch6 = (long)readUnsignedByte() << 48;
    long ch7 = (long)readUnsignedByte() << 56;
    return ((ch0 + ch1) + (ch2 + ch3)) + ((ch4 + ch5) + (ch6 + ch7));
  }

  @Override
  public long readLong() throws IOException {
    if (current == null) {
      throw new EOFException();
    }
    
    if (current.remaining() >= Long.BYTES) {
      // If the whole short can be read from the current buffer, use intrinsics
      this.position += Long.BYTES;
      return current.getLong();
    } else {
      // Otherwise get the long one byte at a time
      return getLongSlow();
    }
  }
}
