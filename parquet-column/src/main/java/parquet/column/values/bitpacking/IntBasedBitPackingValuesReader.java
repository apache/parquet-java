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
package parquet.column.values.bitpacking;

import static parquet.bytes.BytesUtils.getWidthFromMaxInt;

import java.io.IOException;
import java.nio.ByteBuffer;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;

public class IntBasedBitPackingValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(IntBasedBitPackingValuesReader.class);

  private final int bitsPerValue;
  private final IntPacker packer;
  private final int[] decoded = new int[32];
  private int decodedSize;
  private int[] encoded;
  private int encodedPosition = 0;

  /**
   * @param bound the maximum value stored by this column
   */
  public IntBasedBitPackingValuesReader(int bound) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
    this.packer = LemireBitPackingBE.getPacker(bitsPerValue);
    this.encoded = new int[bitsPerValue];
    decode();
  }

  private void decode() {
    decodedSize = 32;
    packer.unpack32Values(encoded, 0, decoded, encodedPosition);
    encodedPosition += bitsPerValue;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.values.ValuesReader#readInteger()
   */
  @Override
  public int readInteger() {
    if (decodedSize == 0) {
      decode();
    }
    -- decodedSize;
    return decoded[decodedSize];
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset)
      throws IOException {
    // TODO: int vs long
    int effectiveBitLength = (int)valueCount * bitsPerValue;
    int length = (effectiveBitLength + 7) / 8; // ceil
    int intLength = (int)(valueCount + 63) / 64 * bitsPerValue; // We write bitsPerValue ints for every 64 values (rounded up to the next 64th value)
    this.encoded = new int[intLength];
    // TODO: :( generate a byte boundary version instead
    for (int i = 0, j = offset; i < page.length / 4; ++i, j += 4) {
      int ch4 = page[j] & 0xff;
      int ch3 = page[j + 1] & 0xff;
      int ch2 = page[j + 2] & 0xff;
      int ch1 = page[j + 3] & 0xff;
      this.encoded[i] = ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
    }
    if (page.length % 4 > 0) {
      int ch4 = 0;
      int ch3 = 0;
      int ch2 = 0;
      int ch1 = 0;
      int j = offset + page.length / 4 * 4;
      switch (page.length % 4) {
      case 3:
        ch2 = page[j + 2] & 0xff;
      case 2:
        ch3 = page[j + 1] & 0xff;
      case 1:
        ch4 = page[j + 0] & 0xff;
      }
      this.encoded[this.encoded.length - 1] = ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
    }

    if (Log.DEBUG) LOG.debug("reading " + length + " bytes for " + valueCount + " values of size " + bitsPerValue + " bits." );
    return offset + length;
  }

}
