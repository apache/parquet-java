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
package redelm.column.primitive;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import redelm.Log;
import redelm.bytes.BytesInput;
import redelm.bytes.BytesUtils;
import redelm.bytes.LittleEndianDataOutputStream;

/**
 * A combination of DataOutputStream and ByteArrayOutputStream
 *
 * @author Julien Le Dem
 *
 */
public class PlainColumnWriter extends PrimitiveColumnWriter {
  private static final Log LOG = Log.getLog(PlainColumnWriter.class);

  public static final Charset CHARSET = Charset.forName("UTF-8");

  private ByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;

  public PlainColumnWriter(int initialSize) {
    arrayOut = new ByteArrayOutputStream(initialSize);
    out = new LittleEndianDataOutputStream(arrayOut);
  }

  @Override
  public final void writeBoolean(boolean v) {
    try {
      out.writeBoolean(v);
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public final void writeBytes(byte[] v) {
    try {
      BytesUtils.writeUnsignedVarInt(v.length, out);
      out.write(v);
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public final void writeInteger(int v) {
    try {
      out.writeInt(v);
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public final void writeLong(long v) {
    try {
      out.writeLong(v);
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public final void writeFloat(float v) {
    try {
      out.writeFloat(v);
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public final void writeDouble(double v) {
    try {
      out.writeDouble(v);
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public void writeByte(int value) {
    try {
      out.write(value);
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public long getMemSize() {
    return arrayOut.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      out.flush();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
    if (Log.DEBUG) LOG.debug("writing a buffer of size " + arrayOut.size());
    return BytesInput.from(arrayOut);
  }

  @Override
  public void reset() {
    arrayOut.reset();
  }

}
