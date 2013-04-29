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

import static parquet.Log.DEBUG;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import parquet.Log;
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
  private static final Log LOG = Log.getLog(RLESimpleEncoder.class);

  private ByteBasedBitPackingEncoder bitPackingEncoder;
  private int totalValues = 0;

  private final int bitWidth;

  public RLESimpleEncoder(int bitWidth) {
    this.bitWidth = bitWidth;
    if (DEBUG) LOG.debug("encoding bitWidth " + bitWidth);
    this.bitPackingEncoder = new ByteBasedBitPackingEncoder(bitWidth);
  }

  public void writeInt(int value) throws IOException {
    bitPackingEncoder.writeInt(value);
    ++ totalValues;
  }

  public BytesInput toBytes() throws IOException {
    if (totalValues % 8 != 0) {
      // padding to the next multiple of 8
      for (int i = 0; i < 8 - (totalValues % 8); ++ i) {
        bitPackingEncoder.writeInt(0);
      }
    }
    ByteArrayOutputStream size = new ByteArrayOutputStream(4);
    final int padded8ValuesBlocks = (totalValues + 7) / 8;
    if (DEBUG) LOG.debug("writing " + totalValues + " values padded to " + (padded8ValuesBlocks * 8));
    int header = padded8ValuesBlocks << 1 | 1;
    BytesUtils.writeUnsignedVarInt(header, size);
    BytesInput bitPacked = bitPackingEncoder.toBytes();
    return BytesInput.fromSequence(
        BytesInput.from(size),
        bitPacked
        );
  }
}
