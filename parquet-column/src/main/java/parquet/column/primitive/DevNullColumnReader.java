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
package parquet.column.primitive;

import java.io.IOException;
import parquet.io.Binary;

/**
 * ColumnReader which does not read any actual data, but rather simply produces
 * an endless stream of constant values.
 */
public class DevNullColumnReader extends PrimitiveColumnReader {
  private boolean defaultBoolean = false;
  private int defaultInt = 0;
  private long defaultLong = 0L;
  private byte defaultByte = 0;
  private float defaultFloat = 0f;
  private double defaultDouble = 0.0;
  private Binary defaultBytes = Binary.EMPTY;

  // TODO(julien): the setDefault* don't seem to be used anywhere. Can we kill them
  // for now, so that this is truly DevNull instead of producing a constant stream of
  // potentially some other value?
  public void setDefaultBoolean(boolean defaultBoolean) {
    this.defaultBoolean = defaultBoolean;
  }

  public void setDefaultInteger(int defaultInt) {
    this.defaultInt = defaultInt;
  }

  public void setDefaultLong(long defaultLong) {
    this.defaultLong = defaultLong;
  }

  public void setDefaultFloat(float defaultFloat) {
    this.defaultFloat = defaultFloat;
  }

  public void setDefaultDouble(double defaultDouble) {
    this.defaultDouble = defaultDouble;
  }

  public void setDefaultByte(byte defaultByte) {
    this.defaultByte = defaultByte;
  }

  public void setDefaultBytes(Binary defaultBytes) {
    this.defaultBytes = defaultBytes;
  }

  public boolean readBoolean() {
    return defaultBoolean;
  }

  public int readByte() {
    return defaultByte;
  }

  public float readFloat() {
    return defaultFloat;
  }

  public Binary readBytes() {
    return defaultBytes;
  }

  public double readDouble() {
    return defaultDouble;
  }

  public int readInteger() {
    return defaultInt;
  }

  public long readLong() {
    return defaultLong;
  }

  @Override
  public int initFromPage(long valueCount, byte[] in, int offset) throws IOException {
    return offset;
  }

}
