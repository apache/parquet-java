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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import parquet.Log;
import parquet.Preconditions;
import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.BytePacker;
import parquet.column.values.bitpacking.Packer;
import parquet.io.ParquetDecodingException;

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 *
 * @author Julien Le Dem
 */
public class RunLengthBitPackingHybridDecoder {
  private static final Log LOG = Log.getLog(RunLengthBitPackingHybridDecoder.class);

  private static enum MODE { RLE, PACKED }

  private final int bitWidth;
  private final BytePacker packer;
  private final InputStream in;

  private MODE mode;
  private int valuesRemaining;
  private int currentCount;
  private int currentValue;
  private int[] currentBuffer;

  public RunLengthBitPackingHybridDecoder(int numValues, int bitWidth, InputStream in) {
    if (DEBUG) LOG.debug("decoding bitWidth " + bitWidth);

    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    this.in = in;
    this.valuesRemaining = numValues;
  }

  public int readInt() throws IOException {
	Preconditions.checkArgument(valuesRemaining > 0, "Reading past RLE/BitPacking stream.");
    if (currentCount == 0) {
      readNext();
    }
    -- currentCount;
    --valuesRemaining;
    int result;
    switch (mode) {
    case RLE:
      result = currentValue;
      break;
    case PACKED:
      result = currentBuffer[currentBuffer.length - 1 - currentCount];
      break;
    default:
      throw new ParquetDecodingException("not a valid mode " + mode);
    }
    return result;
  }

  private void readNext() throws IOException {
    final int header = BytesUtils.readUnsignedVarInt(in);
    mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
    switch (mode) {
    case RLE:
      currentCount = header >>> 1;
      if (DEBUG) LOG.debug("reading " + currentCount + " values RLE");
      currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
      break;
    case PACKED:
      int numGroups = header >>> 1;
      currentCount = numGroups * 8;
      if (DEBUG) LOG.debug("reading " + currentCount + " values BIT PACKED");
      currentBuffer = new int[currentCount]; // TODO: reuse a buffer
      byte[] bytes = new byte[numGroups * bitWidth];
      // At the end of the file RLE data though, there might not be that many bytes left. 
      // For example, if the number of values is 3, with bitwidth 2, this is encoded as 1 
      // group (even though it's really 3/8 of a group). We only need 1 byte to encode the 
      // group values (2 * 3 = 6 bits) but using the numGroups data, we'd think we needed 
      // 2 bytes (1 * 2 = 2 Bytes).
      int valuesLeft = Math.min(currentCount, valuesRemaining);
      new DataInputStream(in).readFully(bytes, 0, (int)Math.ceil(valuesLeft * bitWidth / 8.0));
      for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex += bitWidth) {
        packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
      }
      break;
    default:
      throw new ParquetDecodingException("not a valid mode " + mode);
    }
  }
}
