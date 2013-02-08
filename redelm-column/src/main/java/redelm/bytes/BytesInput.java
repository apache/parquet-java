/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redelm.bytes;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import redelm.Log;

/**
 *
 * A source of bytes capable of writing itself to an output
 *
 * @author Julien Le Dem
 *
 */
abstract public class BytesInput {
  private static final Log LOG = Log.getLog(BytesInput.class);
  private static final boolean DEBUG = false;//Log.DEBUG;
  private static final EmptyBytesInput EMPTY_BYTES_INPUT = new EmptyBytesInput();

  public static BytesInput fromSequence(BytesInput... inputs) {
    return new SequenceBytesIn(inputs);
  }

  public static BytesInput from(InputStream in, int bytes) {
    return new StreamBytesInput(in, bytes);
  }

  public static BytesInput from(byte[] in) {
    if (DEBUG) LOG.debug("BytesInput from array of " + in.length + " bytes");
    return new ByteArrayBytesInput(in);
  }

  public static BytesInput fromInt(int intValue) {
    return new IntBytesInput(intValue);
  }

  public static BytesInput from(ByteArrayOutputStream arrayOut) {
    return new BAOSBytesInput(arrayOut);
  }

  public static BytesInput empty() {
    return EMPTY_BYTES_INPUT;
  }

  public static BytesInput copy(BytesInput bytesInput) {
    return from(bytesInput.toByteArray());
  }

  abstract public void writeAllTo(OutputStream out) throws IOException;

  public byte[] toByteArray() {
    BAOS baos = new BAOS((int)size());
    try {
      this.writeAllTo(baos);
    } catch (IOException e) {
      throw new RuntimeException("Never happens", e);
    }
    if (DEBUG) LOG.debug("converted " + size() + " to byteArray of " + baos.size() + " bytes");
    return baos.getBuf();
  }

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
      byte[] buf = new byte[byteCount];
      new DataInputStream(in).readFully(buf);
      out.write(buf);
    }

    @Override
    public long size() {
      return byteCount;
    }

  }

  private static class SequenceBytesIn extends BytesInput {
    private static final Log LOG = Log.getLog(BytesInput.SequenceBytesIn.class);

    public final BytesInput[] inputs;
    private final long size;

    private SequenceBytesIn(BytesInput[] inputs) {
      this.inputs = inputs;
      long total = 0;
      for (BytesInput input : inputs) {
        total += input.size();
      }
      this.size = total;
    }

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
      BytesUtils.writeIntBigEndian(out, intValue);
    }

    @Override
    public long size() {
      return 4;
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

    private ByteArrayBytesInput(byte[] in) {
      this.in = in;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      out.write(in);
    }

    @Override
    public long size() {
      return in.length;
    }

  }
}
