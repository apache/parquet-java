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

public abstract class PrimitiveColumnReader {

  /**
   * to initialize the column reader
   * The underlying implementation knows how much data to read
   * @param in the array to read from
   * @param offset where to start reading from
   * @return the next offset to read from
   * @throws IOException
   */
  public abstract int initFromPage(byte[] in, int offset) throws IOException;

  public boolean readBoolean() {
    throw new UnsupportedOperationException();
  }

  public int readByte() {
    throw new UnsupportedOperationException();
  }

  public float readFloat() {
    throw new UnsupportedOperationException();
  }

  public byte[] readBytes() {
    throw new UnsupportedOperationException();
  }

  public double readDouble() {
    throw new UnsupportedOperationException();
  }

  public int readInteger() {
    throw new UnsupportedOperationException();
  }

  public long readLong() {
    throw new UnsupportedOperationException();
  }
}