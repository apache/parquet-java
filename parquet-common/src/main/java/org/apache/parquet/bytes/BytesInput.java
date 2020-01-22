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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A source of bytes capable of writing itself to an output. A BytesInput should
 * be consumed right away. It is not a container. For example if it is referring
 * to a stream, subsequent BytesInput reads from the stream will be incorrect if
 * the previous has not been consumed.
 */
abstract public class BytesInput {
  private static final Logger LOG = LoggerFactory.getLogger(BytesInput.class);
  private static final EmptyBytesInput EMPTY_BYTES_INPUT = new EmptyBytesInput();

  /**
   * logically concatenate the provided inputs
   * 
   * @param inputs the inputs to concatenate
   * @return a concatenated input
   */
  public static BytesInput concat(BytesInput... inputs) {
    return new SequenceBytesIn(Arrays.asList(inputs));
  }

  /**
   * logically concatenate the provided inputs
   * 
   * @param inputs the inputs to concatenate
   * @return a concatenated input
   */
  public static BytesInput concat(List<BytesInput> inputs) {
    return new SequenceBytesIn(inputs);
  }

  /**
   * @param in an input stream
   * @param bytes number of bytes to read
   * @return a BytesInput that will read that number of bytes from the stream
   */
  public static BytesInput from(InputStream in, int bytes) {
    return new StreamBytesInput(in, bytes);
  }

  /**
   * @param buffer
   * @param length number of bytes to read
   * @return a BytesInput that will read the given bytes from the ByteBuffer
   * @deprecated Will be removed in 2.0.0
   */
  @Deprecated
  public static BytesInput from(ByteBuffer buffer, int offset, int length) {
    ByteBuffer tmp = buffer.duplicate();
    tmp.position(offset);
    ByteBuffer slice = tmp.slice();
    slice.limit(length);
    return new ByteBufferBytesInput(slice);
  }

  /**
   * @param buffers an array of byte buffers
   * @return a BytesInput that will read the given bytes from the ByteBuffers
   */
  public static BytesInput from(ByteBuffer... buffers) {
    if (buffers.length == 1) {
      return new ByteBufferBytesInput(buffers[0]);
    }
    return new BufferListBytesInput(Arrays.asList(buffers));
  }

  /**
   * @param buffers a list of byte buffers
   * @return a BytesInput that will read the given bytes from the ByteBuffers
   */
  public static BytesInput from(List<ByteBuffer> buffers) {
    if (buffers.size() == 1) {
      return new ByteBufferBytesInput(buffers.get(0));
    }
    return new BufferListBytesInput(buffers);
  }

  /**
   *
   * @param in a byte array
   * @return a Bytes input that will write the given bytes
   */
  public static BytesInput from(byte[] in) {
    LOG.debug("BytesInput from array of {} bytes", in.length);
    return new ByteArrayBytesInput(in, 0, in.length);
  }

  public static BytesInput from(byte[] in, int offset, int length) {
    LOG.debug("BytesInput from array of {} bytes", length);
    return new ByteArrayBytesInput(in, offset, length);
  }

  /**
   * @param intValue the int to write
   * @return a BytesInput that will write 4 bytes in little endian
   */
  public static BytesInput fromInt(int intValue) {
    return new IntBytesInput(intValue);
  }

  /**
   * @param intValue the int to write
   * @return a BytesInput that will write var int
   */
  public static BytesInput fromUnsignedVarInt(int intValue) {
    return new UnsignedVarIntBytesInput(intValue);
  }

  /**
   *
   * @param intValue the int to write
   * @return a ByteInput that contains the int value as a variable-length zig-zag
   * encoded int
   */
  public static BytesInput fromZigZagVarInt(int intValue) {
    int zigZag = (intValue << 1) ^ (intValue >> 31);
    return new UnsignedVarIntBytesInput(zigZag);
  }

  /**
   * @param longValue the long to write
   * @return a BytesInput that will write var long
   */
  public static BytesInput fromUnsignedVarLong(long longValue) {
    return new UnsignedVarLongBytesInput(longValue);
  }

  /**
   *
   * @param longValue the long to write
   * @return a ByteInput that contains the long value as a variable-length zig-zag
   * encoded long
   */
  public static BytesInput fromZigZagVarLong(long longValue) {
    long zigZag = (longValue << 1) ^ (longValue >> 63);
    return new UnsignedVarLongBytesInput(zigZag);
  }

  /**
   * @param arrayOut a capacity byte array output stream to wrap into a BytesInput
   * @return a BytesInput that will write the content of the buffer
   */
  public static BytesInput from(CapacityByteArrayOutputStream arrayOut) {
    return new CapacityBAOSBytesInput(arrayOut);
  }

  /**
   * @param baos - stream to wrap into a BytesInput
   * @return a BytesInput that will write the content of the buffer
   */
  public static BytesInput from(ByteArrayOutputStream baos) {
    return new BAOSBytesInput(baos);
  }

  /**
   * @return an empty bytes input
   */
  public static BytesInput empty() {
    return EMPTY_BYTES_INPUT;
  }

  /**
   * copies the input into a new byte array
   * 
   * @param bytesInput a BytesInput
   * @return a copy of the BytesInput
   * @throws IOException if there is an exception when reading bytes from the
   * BytesInput
   */
  public static BytesInput copy(BytesInput bytesInput) throws IOException {
    return from(bytesInput.toByteArray());
  }

  /**
   * writes the bytes into a stream
   * 
   * @param out an output stream
   * @throws IOException if there is an exception writing
   */
  abstract public void writeAllTo(OutputStream out) throws IOException;

  /**
   *
   * @return a new byte array materializing the contents of this input
   * @throws IOException if there is an exception reading
   */
  public byte[] toByteArray() throws IOException {
    BAOS baos = new BAOS((int) size());
    this.writeAllTo(baos);
    LOG.debug("converted {} to byteArray of {} bytes", size(), baos.size());
    return baos.getBuf();
  }

  /**
   *
   * @return a new ByteBuffer materializing the contents of this input
   * @throws IOException if there is an exception reading
   */
  public ByteBuffer toByteBuffer() throws IOException {
    return ByteBuffer.wrap(toByteArray());
  }

  /**
   *
   * @return a new InputStream materializing the contents of this input
   * @throws IOException if there is an exception reading
   */
  public ByteBufferInputStream toInputStream() throws IOException {
    return ByteBufferInputStream.wrap(toByteBuffer());
  }

  /**
   *
   * @return the size in bytes that would be written
   */
  abstract public long size();

  private static final class BAOS extends ByteArrayOutputStream {
    private BAOS(int size) {
      super(size);
    }

    public byte[] getBuf() {
      return this.buf;
    }
  }

  private static class StreamBytesInput extends BytesInput {
    private static final Logger LOG = LoggerFactory.getLogger(BytesInput.StreamBytesInput.class);
    private final InputStream in;
    private final int byteCount;

    private StreamBytesInput(InputStream in, int byteCount) {
      super();
      this.in = in;
      this.byteCount = byteCount;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      LOG.debug("write All {} bytes", byteCount);
      // TODO: more efficient
      out.write(this.toByteArray());
    }

    public byte[] toByteArray() throws IOException {
      LOG.debug("read all {} bytes", byteCount);
      byte[] buf = new byte[byteCount];
      new DataInputStream(in).readFully(buf);
      return buf;
    }

    @Override
    public long size() {
      return byteCount;
    }

  }

  private static class SequenceBytesIn extends BytesInput {
    private static final Logger LOG = LoggerFactory.getLogger(BytesInput.SequenceBytesIn.class);

    private final List<BytesInput> inputs;
    private final long size;

    private SequenceBytesIn(List<BytesInput> inputs) {
      this.inputs = inputs;
      long total = 0;
      for (BytesInput input : inputs) {
        total += input.size();
      }
      this.size = total;
    }

    @SuppressWarnings("unused")
    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      for (BytesInput input : inputs) {

        LOG.debug("write {} bytes to out", input.size());
        if (input instanceof SequenceBytesIn)
          LOG.debug("{");
        input.writeAllTo(out);
        if (input instanceof SequenceBytesIn)
          LOG.debug("}");
      }
    }

    @Override
    public long size() {
      return size;
    }

  }

  private static class IntBytesInput extends BytesInput {

    private final int intValue;

    public IntBytesInput(int intValue) {
      this.intValue = intValue;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      BytesUtils.writeIntLittleEndian(out, intValue);
    }

    public ByteBuffer toByteBuffer() throws IOException {
      return ByteBuffer.allocate(4).putInt(0, intValue);
    }

    @Override
    public long size() {
      return 4;
    }

  }

  private static class UnsignedVarIntBytesInput extends BytesInput {

    private final int intValue;

    public UnsignedVarIntBytesInput(int intValue) {
      this.intValue = intValue;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      BytesUtils.writeUnsignedVarInt(intValue, out);
    }

    public ByteBuffer toByteBuffer() throws IOException {
      ByteBuffer ret = ByteBuffer.allocate((int) size());
      BytesUtils.writeUnsignedVarInt(intValue, ret);
      return ret;
    }

    @Override
    public long size() {
      int s = (38 - Integer.numberOfLeadingZeros(intValue)) / 7;
      return s == 0 ? 1 : s;
    }
  }

  private static class UnsignedVarLongBytesInput extends BytesInput {

    private final long longValue;

    public UnsignedVarLongBytesInput(long longValue) {
      this.longValue = longValue;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      BytesUtils.writeUnsignedVarLong(longValue, out);
    }

    @Override
    public long size() {
      int s = (70 - Long.numberOfLeadingZeros(longValue)) / 7;
      return s == 0 ? 1 : s;
    }
  }

  private static class EmptyBytesInput extends BytesInput {

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
    }

    @Override
    public long size() {
      return 0;
    }

    public ByteBuffer toByteBuffer() throws IOException {
      return ByteBuffer.allocate(0);
    }

  }

  private static class CapacityBAOSBytesInput extends BytesInput {

    private final CapacityByteArrayOutputStream arrayOut;

    private CapacityBAOSBytesInput(CapacityByteArrayOutputStream arrayOut) {
      this.arrayOut = arrayOut;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      arrayOut.writeTo(out);
    }

    @Override
    public long size() {
      return arrayOut.size();
    }

  }

  private static class BAOSBytesInput extends BytesInput {

    private final ByteArrayOutputStream arrayOut;

    private BAOSBytesInput(ByteArrayOutputStream arrayOut) {
      this.arrayOut = arrayOut;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      arrayOut.writeTo(out);
    }

    @Override
    public long size() {
      return arrayOut.size();
    }

  }

  private static class ByteArrayBytesInput extends BytesInput {

    private final byte[] in;
    private final int offset;
    private final int length;

    private ByteArrayBytesInput(byte[] in, int offset, int length) {
      this.in = in;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      out.write(in, offset, length);
    }

    public ByteBuffer toByteBuffer() throws IOException {
      return java.nio.ByteBuffer.wrap(in, offset, length);
    }

    @Override
    public long size() {
      return length;
    }

  }

  private static class BufferListBytesInput extends BytesInput {
    private final List<ByteBuffer> buffers;
    private final long length;

    public BufferListBytesInput(List<ByteBuffer> buffers) {
      this.buffers = buffers;
      long totalLen = 0;
      for (ByteBuffer buffer : buffers) {
        totalLen += buffer.remaining();
      }
      this.length = totalLen;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      WritableByteChannel channel = Channels.newChannel(out);
      for (ByteBuffer buffer : buffers) {
        channel.write(buffer.duplicate());
      }
    }

    @Override
    public ByteBufferInputStream toInputStream() {
      return ByteBufferInputStream.wrap(buffers);
    }

    @Override
    public long size() {
      return length;
    }
  }

  private static class ByteBufferBytesInput extends BytesInput {
    private final ByteBuffer buffer;

    private ByteBufferBytesInput(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      Channels.newChannel(out).write(buffer.duplicate());
    }

    @Override
    public ByteBufferInputStream toInputStream() {
      return ByteBufferInputStream.wrap(buffer);
    }

    @Override
    public long size() {
      return buffer.remaining();
    }
  }
}
