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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import static redelm.column.primitive.SimplePrimitiveColumnWriter.CHARSET;

public class SimplePrimitiveColumnReader extends PrimitiveColumnReader {

  private DataInputStream in;

  public SimplePrimitiveColumnReader(byte[] data, int offset, int length) {
    this.in = new DataInputStream(new ByteArrayInputStream(data, offset, length));
  }

  @Override
  public float readFloat() {
    try {
      return in.readFloat();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public byte[] readBytes() {
    try {
      byte[] value = new byte[in.readInt()];
      in.readFully(value);
      return value;
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public boolean readBoolean() {
    try {
      return in.readBoolean();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public String readString() {
    try {
      int size = readUnsignedVarInt();
      if (size == 0) {
        return "";
      } else {
        byte[] bytes = new byte[size];
        int i = 0;
        do {
          int n = in.read(bytes, i, bytes.length - i);
          if (n == -1) {
            throw new RuntimeException("Reached end of stream");
          }
          i = i + n;
        } while (i < bytes.length);
        return new String(bytes, CHARSET);
      }
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public double readDouble() {
    try {
      return in.readDouble();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public int readInt() {
    try {
      return in.readInt();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public long readLong() {
    try {
      return in.readLong();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public int readByte() {
    try {
      return in.read();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  private int readUnsignedVarInt() throws IOException {
    int value = 0;
    int i = 0;
    int b;
    while (((b = in.readByte()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
    }
    return value | (b << i);
  }

}
