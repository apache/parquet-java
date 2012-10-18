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

import java.io.IOException;
import java.io.OutputStream;

import redelm.column.BytesOutput;

public abstract class PrimitiveColumnWriter {

  public abstract int getMemSize();

  public abstract void writeData(BytesOutput out) throws IOException;

  public abstract void reset();

  public void writeByte(int value) {
    throw new UnsupportedOperationException();
  }

  public void writeBoolean(boolean v) {
    throw new UnsupportedOperationException();
  }

  public void writeBytes(byte[] v) {
    throw new UnsupportedOperationException();
  }

  public void writeInt(int v) {
    throw new UnsupportedOperationException();
  }

  public void writeLong(long v) {
    throw new UnsupportedOperationException();
  }

  public void writeDouble(double v) {
    throw new UnsupportedOperationException();
  }

  public void writeString(String str) {
    throw new UnsupportedOperationException();
  }

  public void writeFloat(float v) {
    throw new UnsupportedOperationException();
  }

}
