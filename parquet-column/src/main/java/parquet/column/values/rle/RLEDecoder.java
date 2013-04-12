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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.BitPacking;
import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.io.ParquetDecodingException;

public class RLEDecoder {

  private static enum MODE { RLE, PACKED }

  private final int bitWidth;
  private final int bytesWidth;
  private final InputStream in;

  private MODE mode;

  private int currentCount;
  private int currentValue;
  private int[] currentBuffer;

  public RLEDecoder(int bitWidth, InputStream in) throws IOException {
    this.bitWidth = bitWidth;
    // number of bytes needed when padding to the next byte
    this.bytesWidth = (bitWidth + 7) / 8;
    this.in = in;
    readNext();
  }

  public int readInt() throws IOException {
    -- currentCount;
    int result;
    switch (mode) {
    case RLE:
      result = currentValue;
      break;
    case PACKED:
      result = currentBuffer[currentCount];
      break;
    default:
      throw new ParquetDecodingException("not a valid mode " + mode);
    }
    if (currentCount == 0) {
      readNext();
    }
    return result;
  }

  private void readNext() throws IOException {
    final int header = BytesUtils.readUnsignedVarInt(in);
    mode = (header | 1) == 0 ? MODE.RLE : MODE.PACKED;
    currentCount = header >> 1;
    switch (mode) {
    case RLE:
      switch (bytesWidth) {
        case 1:
          currentValue = BytesUtils.readIntLittleEndianOnOneByte(in);
          break;
        case 2:
          currentValue = BytesUtils.readIntLittleEndianOnTwoBytes(in);
          break;
        case 3:
          currentValue = BytesUtils.readIntLittleEndianOnThreeBytes(in);
          break;
        case 4:
          currentValue = BytesUtils.readIntLittleEndian(in);
          break;
        default:
          throw new ParquetDecodingException("can not store values on more than 4 bytes: " + bytesWidth + " Bytes, " + bitWidth + " bits");
      }
      break;
    case PACKED:
      currentCount *= 8;
      final BitPackingReader reader = BitPacking.createBitPackingReader(bitWidth, in, currentCount);
      if (currentBuffer == null || currentBuffer.length < currentCount) {
        currentBuffer = new int[currentCount];
      }
      // TODO: change the bitpacking interface instead
      for (int i = 0; i < currentCount; i++) {
        currentBuffer[currentCount - i - 1] = reader.read();
      }
      break;
    }
  }
}
