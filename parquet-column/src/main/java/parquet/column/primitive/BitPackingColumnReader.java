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

import static parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static parquet.column.primitive.BitPacking.createBitPackingReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Log;
import parquet.column.primitive.BitPacking.BitPackingReader;
import parquet.io.ParquetDecodingException;

/**
 * a column reader that packs the ints in the number of bits required based on the maximum size.
 *
 * @author Julien Le Dem
 *
 */
public class BitPackingColumnReader extends PrimitiveColumnReader {
  private static final Log LOG = Log.getLog(BitPackingColumnReader.class);

  private ByteArrayInputStream in;
  private BitPackingReader bitPackingReader;
  private final int bitsPerValue;

  /**
   *
   * @param bound the maximum value stored by this column
   */
  public BitPackingColumnReader(int bound) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.primitive.PrimitiveColumnReader#readInteger()
   */
  @Override
  public int readInteger() {
    try {
      return bitPackingReader.read();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.primitive.PrimitiveColumnReader#initFromPage(long, byte[], int)
   */
  @Override
  public int initFromPage(long valueCount, byte[] in, int offset) throws IOException {
    // TODO: int vs long
    int effectiveBitLength = (int)valueCount * bitsPerValue;
    // TODO: maybe ((effectiveBitLength - 1) / 8 + 1) here? has fewer conditionals and divides
    int length = effectiveBitLength / 8 + (effectiveBitLength % 8 == 0 ? 0 : 1); // ceil
    if (Log.DEBUG) LOG.debug("reading " + length + " bytes for " + valueCount + " values of size " + bitsPerValue + " bits." );
    this.in = new ByteArrayInputStream(in, offset, length);
    this.bitPackingReader = createBitPackingReader(bitsPerValue, this.in, valueCount);
    return offset + length;
  }

}
