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
import java.io.IOException;

/**
 * This is a special writer that doesn't write anything. The idea being that
 * some columns will always be the same value, and this will capture that. An
 * example is a schema with no repeated fields.
 */
public class DevNullColumnWriter extends PrimitiveColumnWriter {
  @Override
  public int getMemSize() {
    return 0;
  }

  @Override
  public void writeData(DataOutput out) throws IOException {
  }

  @Override
  public void reset() {
  }

  @Override
  public void writeInteger(int v) {
  }

  @Override
  public void writeByte(int value) {
  }

  @Override
  public void writeBoolean(boolean v) {
  }

  @Override
  public void writeBytes(byte[] v) {
  }

  @Override
  public void writeLong(long v) {
  }

  @Override
  public void writeDouble(double v) {
  }

  @Override
  public void writeString(String str) {
  }

  @Override
  public void writeFloat(float v) {
  }
}