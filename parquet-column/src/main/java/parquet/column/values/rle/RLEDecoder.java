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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.ByteBitPacking;
import parquet.column.values.bitpacking.BytePacker;
import parquet.io.ParquetDecodingException;

public class RLEDecoder {

  private static enum MODE { RLE, PACKED }

  private final int bitWidth;
  private final BytePacker packer;
  private final int bytesWidth;
  private final InputStream in;

  private MODE mode;

  private int currentCount;
  private int currentValue;
  private int[] currentBuffer;

  public RLEDecoder(int bitWidth, InputStream in) {
    this.bitWidth = bitWidth;
    this.packer = ByteBitPacking.getPacker(bitWidth);
    // number of bytes needed when padding to the next byte
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
    this.in = in;
  }

  public int readInt() throws IOException {
    if (currentCount == 0) {
      readNext();
    }
    -- currentCount;
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
    mode = (header | 1) == 0 ? MODE.RLE : MODE.PACKED;
    switch (mode) {
    case RLE: // TODO: test this
      currentCount = header >>> 1;
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
      int blocks = header >>> 1;
      currentCount = blocks * 8;
      currentBuffer = new int[currentCount]; // TODO: reuse a buffer
      byte[] bytes = new byte[blocks * bitWidth];
      new DataInputStream(in).readFully(bytes);
      for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex += bitWidth) {
        packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
      }
      break;
    }
  }
}
