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

import static parquet.bytes.BytesInput.concat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.ByteBasedBitPackingEncoder;

/**
 * Simple RLE Encoder that does only bitpacking
 *
 * @author Julien Le Dem
 *
 */
public class RLESimpleEncoder {

  private ByteBasedBitPackingEncoder bitPackingEncoder;
  private int totalValues = 0;

  public RLESimpleEncoder(int bitWidth) {
    this.bitPackingEncoder = new ByteBasedBitPackingEncoder(bitWidth);
  }

  public void writeInt(int value) throws IOException {
    bitPackingEncoder.writeInt(value);
    ++ totalValues;
  }

  public BytesInput toBytes() throws IOException {
    ByteArrayOutputStream size = new ByteArrayOutputStream(4);
    int header = BytesUtils.paddedByteCountFromBits(totalValues) << 1 | 1;
    BytesUtils.writeUnsignedVarInt(header, size);
    return concat(BytesInput.from(size), bitPackingEncoder.toBytes());
  }
}
