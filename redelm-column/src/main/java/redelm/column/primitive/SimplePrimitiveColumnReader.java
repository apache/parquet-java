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
      return in.readUTF();
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
  public int readByte() {
    try {
      return in.read();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

}
