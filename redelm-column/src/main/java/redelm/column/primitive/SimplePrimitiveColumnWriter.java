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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import redelm.column.RedelmByteArrayOutputStream;

/**
 * A combination of DataOutputStream and ByteArrayOutputStream
 *
 * @author Julien Le Dem
 *
 */
public class SimplePrimitiveColumnWriter extends PrimitiveColumnWriter {

  public static final Charset CHARSET = Charset.forName("UTF-8");

  private RedelmByteArrayOutputStream arrayOut;
  private DataOutputStream out;

  public SimplePrimitiveColumnWriter(int initialSize) {
    arrayOut = new RedelmByteArrayOutputStream(initialSize);
    out = new DataOutputStream(arrayOut);
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
      out.writeInt(v.length);
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
  public final void writeString(String str) {
    try {
      // writeUTF() has a max size of 64k :((
      byte[] bytes = str.getBytes(CHARSET);
      writeUnsignedVarInt(bytes.length);
      out.write(bytes);
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
  public int getMemSize() {
    return arrayOut.size();
  }

  @Override
  public void writeData(DataOutput output) throws IOException {
    arrayOut.writeTo(output);
  }

  @Override
  public void reset() {
    arrayOut.reset();
  }

  private void writeUnsignedVarInt(int value) throws IOException {
    while ((value & 0xFFFFFF80) != 0L) {
      out.writeByte((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.writeByte(value & 0x7F);
  }
}
