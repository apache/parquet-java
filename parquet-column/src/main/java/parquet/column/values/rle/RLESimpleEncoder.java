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
package parquet.column.values.rle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.BitPacking;

/**
 * Simple RLE Encoder that does only bitpacking
 *
 * @author Julien Le Dem
 *
 */
public class RLESimpleEncoder {

  private final int bitWidth;
  private final BitPacking.BitPackingWriter out;
  private final ByteArrayOutputStream buffer;

  public RLESimpleEncoder(int bitWidth) {
    this.bitWidth = bitWidth;
    buffer = new ByteArrayOutputStream(64 * 1024);
    out = BitPacking.getBitPackingWriter(bitWidth, buffer);
  }

  public void writeInt(int value) throws IOException {
    out.write(value);
  }

  public BytesInput toBytes() throws IOException {
    out.finish();
    final BytesInput bytes = BytesInput.from(buffer);
    ByteArrayOutputStream size = new ByteArrayOutputStream(4);
    int header = (int)(bytes.size()/bitWidth) << 1 | 1; // TODO: fix int vs long
    BytesUtils.writeUnsignedVarInt(header, size);
    return BytesInput.fromSequence(BytesInput.from(size), bytes);
  }

}
