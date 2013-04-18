/**
 * Copyright 2013 Criteo.
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
package parquet.hive.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import parquet.io.api.Binary;
/**
 *
 * A Parquet InputFormat for Hive (with the deprecated package mapred)
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class BinaryWritable implements Writable {

  private byte[] bytes;

  public BinaryWritable(final Binary binary) {
    bytes = binary.getBytes();
  }

  public BinaryWritable(final String string) {
    bytes = string.getBytes();
  }

  @Override
  public void readFields(final DataInput input) throws IOException {
    final int size = input.readInt();

    // Define a new byte of array of the exact size of the payload
    final byte[] bytes = new byte[size];
    input.readFully(bytes);
  }

  @Override
  public void write(final DataOutput output) throws IOException {
    if (bytes != null) {
      output.writeInt(bytes.length);
      output.write(bytes);
    }
  }

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(final byte[] bytes) {
    this.bytes = bytes;
  }
}
