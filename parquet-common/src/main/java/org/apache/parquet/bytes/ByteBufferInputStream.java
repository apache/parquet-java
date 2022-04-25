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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.parquet.ShouldNeverHappenException;

/*
Changes implemented:
All of the functionality of LittleEndianDataInputStream has been merged into ByteBufferInputStream and its child
classes. This has resulted in measurable performance improvements for the following reasons:
- Elimination of at least one layer of abstraction / method call overhead
- Enabling support for intrinsics for readInt, readLong, etc.
- Eliminate the need for the JIT to make inferences that may or may not inline methods from BytesUtils and
  the InputStream.read() that is called by BytesUtils.
 */

public class ByteBufferInputStream extends InputStream {

  // Used to maintain the deprecated behavior of instantiating ByteBufferInputStream directly
  private final ByteBufferInputStream delegate;

  public static ByteBufferInputStream wrap(ByteBuffer... buffers) {
    if (buffers.length == 1) {
      return new SingleBufferInputStream(buffers[0]);
    } else {
      return new MultiBufferInputStream(Arrays.asList(buffers));
    }
  }

  public static ByteBufferInputStream wrap(List<ByteBuffer> buffers) {
    if (buffers.size() == 1) {
      return new SingleBufferInputStream(buffers.get(0));
    } else {
      return new MultiBufferInputStream(buffers);
    }
  }

  public static ByteBufferInputStream wrap(ByteBuffer buffer, int offset, int count) {
    return new SingleBufferInputStream(buffer, offset, count);
  }

  public static ByteBufferInputStream wrap(byte[] buf) {
    return new SingleBufferInputStream(buf);
  }

  public static ByteBufferInputStream wrap(byte[] buf, int start, int length) {
    return new SingleBufferInputStream(buf, start, length);
  }


  ByteBufferInputStream() {
    delegate = null;
  }

  /**
   * @param buffer
   *          the buffer to be wrapped in this input stream
   * @deprecated Will be removed in 2.0.0; Use {@link #wrap(ByteBuffer...)} instead
   */
  @Deprecated
  public ByteBufferInputStream(ByteBuffer buffer) {
    delegate = wrap(buffer);
  }

  /**
   * @param buffer
   *          the buffer to be wrapped in this input stream
   * @param offset
   *          the offset of the data in the buffer
   * @param count
   *          the number of bytes to be read from the buffer
   * @deprecated Will be removed in 2.0.0; Use {@link #wrap(ByteBuffer...)} instead
   */
  @Deprecated
  public ByteBufferInputStream(ByteBuffer buffer, int offset, int count) {
    // This is necessary to pass "TestDeprecatedBufferInputStream"...
    ByteBuffer temp = buffer.duplicate();
    temp.position(offset);
    ByteBuffer byteBuf = temp.slice();
    byteBuf.limit(count);
    delegate = wrap(byteBuf);
    // ... but it would probably be faster to do this:
//    delegate = wrap(buffer, offset, count);
  }

  public ByteBufferInputStream(byte[] inBuf) {
    delegate = wrap(inBuf);
  }

  public ByteBufferInputStream(byte[] inBuf, int start, int length) {
    delegate = wrap(inBuf, start, length);
  }

  public ByteBufferInputStream(List<ByteBuffer> inBufs) {
    delegate = wrap(inBufs);
  }

  /**
   * @return the slice of the byte buffer inside this stream
   * @deprecated Will be removed in 2.0.0; Use {@link #slice(int)} instead
   */
  @Deprecated
  public ByteBuffer toByteBuffer() {
    try {
      return slice(available());
    } catch (EOFException e) {
      throw new ShouldNeverHappenException(e);
    }
  }

  public long position() {
    return delegate.position();
  }

  public void position(int pos) {
    throw new UnsupportedOperationException();
  }

  public void skipFully(long n) throws IOException {
    delegate.skipFully(n);
  }

  public int read(ByteBuffer out) {
    return delegate.read(out);
  }

  public ByteBuffer slice(int length) throws EOFException {
    return delegate.slice(length);
  }

  public List<ByteBuffer> sliceBuffers(long length) throws EOFException {
    return delegate.sliceBuffers(length);
  }

  public ByteBufferInputStream sliceStream(long length) throws EOFException {
    return delegate.sliceStream(length);
    //return ByteBufferInputStream.wrap(sliceBuffers(length));
  }

  public List<ByteBuffer> remainingBuffers() {
    return delegate.remainingBuffers();
  }

  public ByteBufferInputStream remainingStream() {
    return delegate.remainingStream();
  }

  public ByteBufferInputStream duplicate() {
    return delegate.duplicate();
  }

  public int read() throws IOException {
    return delegate.read();
  }

  public int read(byte[] b, int off, int len) throws IOException {
    return delegate.read(b, off, len);
  }

  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  public void readFully(byte b[]) throws IOException {
    delegate.readFully(b, 0, b.length);
  }

  public void readFully(byte b[], int off, int len) throws IOException {
    delegate.readFully(b, off, len);
  }

  public long skip(long n) {
    return delegate.skip(n);
  }

  public int skipBytes(int n) {
    return (int)skip(n);
  }

  public int available() {
    return delegate.available();
  }

  public int remaining() {
    return available();
  }

  public void mark(int readlimit) {
    delegate.mark(readlimit);
  }

  public void reset() throws IOException {
    delegate.reset();
  }

  public boolean markSupported() {
    return delegate.markSupported();
  }

  public void close() throws IOException {
  }

  public boolean readBoolean() throws IOException {
    return readByte() != 0;
  }

  public byte readByte() throws IOException {
    return delegate.readByte();
  }

  public int readUnsignedByte() throws IOException {
    return delegate.readUnsignedByte();
  }

  public short readShort() throws IOException {
    return delegate.readShort();
  }

  public int readUnsignedShort() throws IOException {
    return delegate.readUnsignedShort();
  }

  public int readInt() throws IOException {
    return delegate.readInt();
  }

  public long readLong() throws IOException {
    return delegate.readLong();
  }

  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  public int readIntLittleEndianOnThreeBytes() throws IOException {
    int ch1 = readUnsignedByte();
    int ch2 = readUnsignedByte();
    int ch3 = readUnsignedByte();
    return ((ch3 << 16) + (ch2 << 8) + (ch1 << 0));
  }

  public int readIntLittleEndianPaddedOnBitWidth(int bitWidth)
    throws IOException {

    int bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return readUnsignedByte();
      case 2:
        return readUnsignedShort();
      case 3:
        return readIntLittleEndianOnThreeBytes();
      case 4:
        return readInt();
      default:
        throw new IOException(
          String.format("Encountered bitWidth (%d) that requires more than 4 bytes", bitWidth));
    }
  }

  public int readUnsignedVarInt() throws IOException {
    int value = 0;
    int i = 0;
    int b;
    while (((b = readUnsignedByte()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
    }
    return value | (b << i);
  }

}
