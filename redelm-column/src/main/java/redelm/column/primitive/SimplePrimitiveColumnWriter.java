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

import java.io.DataOutputStream;
import java.io.IOException;

import redelm.column.BytesOutput;
import redelm.column.RedelmByteArrayOutputStream;

/**
 * A combination of DataOutputStream and ByteArrayOutputStream
 *
 * @author Julien Le Dem
 *
 */
public class SimplePrimitiveColumnWriter extends PrimitiveColumnWriter {

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
  public final void writeInt(int v) {
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
      out.writeUTF(str);
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
  public void writeData(BytesOutput output) throws IOException {
    arrayOut.writeTo(output);
  }

  @Override
  public void reset() {
    arrayOut.reset();
  }

}
