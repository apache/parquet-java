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
package parquet.bytes;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import parquet.Log;


/**
 * A source of bytes capable of writing itself to an output.
 * A BytesInput should be consumed right away.
 * It is not a container.
 * For example if it is referring to a stream,
 * subsequent BytesInput reads from the stream will be incorrect
 * if the previous has not been consumed.
 *
 * @author Julien Le Dem
 *
 */
abstract public class BytesInput {
  private static final Log LOG = Log.getLog(BytesInput.class);
  private static final boolean DEBUG = false;//Log.DEBUG;
  private static final EmptyBytesInput EMPTY_BYTES_INPUT = new EmptyBytesInput();

  /**
   * logically concatenate the provided inputs
   * @param inputs the inputs to concatenate
   * @return a concatenated input
   */
  public static BytesInput concat(BytesInput... inputs) {
    return new SequenceBytesIn(Arrays.asList(inputs));
  }

  /**
   * logically concatenate the provided inputs
   * @param inputs the inputs to concatenate
   * @return a concatenated input
   */
  public static BytesInput concat(List<BytesInput> inputs) {
    return new SequenceBytesIn(inputs);
  }

  /**
   * @param in
   * @param bytes number of bytes to read
   * @return a BytesInput that will read that number of bytes from the stream
   */
  public static BytesInput from(InputStream in, int bytes) {
    return new StreamBytesInput(in, bytes);
  }

  /**
   *
   * @param in
   * @return a Bytes input that will write the given bytes
   */
  public static BytesInput from(byte[] in) {
    if (DEBUG) LOG.debug("BytesInput from array of " + in.length + " bytes");
    return new ByteArrayBytesInput(in, 0 , in.length);
  }

  public static BytesInput from(byte[] in, int offset, int length) {
    if (DEBUG) LOG.debug("BytesInput from array of " + length + " bytes");
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
   */
  public static BytesInput fromZigZagVarInt(int intValue) {
    int zigZag = (intValue << 1) ^ (intValue >> 31);
    return new UnsignedVarIntBytesInput(zigZag);
  }

  /**
   * @param arrayOut
   * @return a BytesInput that will write the content of the buffer
   */
  public static BytesInput from(CapacityByteArrayOutputStream arrayOut) {
    return new CapacityBAOSBytesInput(arrayOut);
  }

  /**
   * @param arrayOut
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
   * @param bytesInput
   * @return
   * @throws IOException
   */
  public static BytesInput copy(BytesInput bytesInput) throws IOException {
    return from(bytesInput.toByteArray());
  }

  /**
   * writes the bytes into a stream
   * @param out
   * @throws IOException
   */
  abstract public void writeAllTo(OutputStream out) throws IOException;

  /**
   *
   * @return a new byte array materializing the contents of this input
   * @throws IOException
   */
  public byte[] toByteArray() throws IOException {
    BAOS baos = new BAOS((int)size());
    this.writeAllTo(baos);
    if (DEBUG) LOG.debug("converted " + size() + " to byteArray of " + baos.size() + " bytes");
    return baos.getBuf();
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
    private static final Log LOG = Log.getLog(BytesInput.StreamBytesInput.class);
    private final InputStream in;
    private final int byteCount;

    private StreamBytesInput(InputStream in, int byteCount) {
      super();
      this.in = in;
      this.byteCount = byteCount;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      if (DEBUG) LOG.debug("write All "+ byteCount + " bytes");
      // TODO: more efficient
      out.write(this.toByteArray());
    }

    public byte[] toByteArray() throws IOException {
      if (DEBUG) LOG.debug("read all "+ byteCount + " bytes");
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
    private static final Log LOG = Log.getLog(BytesInput.SequenceBytesIn.class);

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
        if (DEBUG) LOG.debug("write " + input.size() + " bytes to out");
        if (DEBUG && input instanceof SequenceBytesIn) LOG.debug("{");
        input.writeAllTo(out);
        if (DEBUG && input instanceof SequenceBytesIn) LOG.debug("}");
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

    @Override
    public long size() {
      int s = 5 - ((Integer.numberOfLeadingZeros(intValue) + 3) / 7);
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

    @Override
    public long size() {
      return length;
    }

  }

}
