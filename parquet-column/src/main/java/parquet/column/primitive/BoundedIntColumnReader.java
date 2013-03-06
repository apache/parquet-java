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

import static parquet.Log.DEBUG;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.io.ParquetDecodingException;

/**
 * @see BoundedIntColumnWriter
 */
class BoundedIntColumnReader extends PrimitiveColumnReader {
  private static final Log LOG = Log.getLog(BoundedIntColumnReader.class);

  private int currentValueCt = 0;
  private int currentValue = 0;
  private final int bitsPerValue;
  private BitReader bitReader = new BitReader();

  public BoundedIntColumnReader(int bound) {
    if (bound == 0) {
      throw new ParquetDecodingException("Value bound cannot be 0. Use DevNullColumnReader instead.");
    }
    bitsPerValue = BytesUtils.getWidthFromMaxInt(bound);
  }

  @Override
  public int readInteger() {
    try {
      if (currentValueCt > 0) {
        currentValueCt--;
        return currentValue;
      }
      if (bitReader.readBit()) {
        currentValue = bitReader.readNBitInteger(bitsPerValue);
        currentValueCt = bitReader.readUnsignedVarint() - 1;
      } else {
        currentValue = bitReader.readNBitInteger(bitsPerValue);
      }
      return currentValue;
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read int", e);
    }
  }

  // This forces it to deserialize into memory. If it wanted
  // to, it could just read the bytes (though that number of
  // bytes would have to be serialized). This is the flip-side
  // to BoundedIntColumnWriter.writeData(BytesOutput)
  @Override
  public int initFromPage(long valueCount, byte[] in, int offset) throws IOException {
    if (DEBUG) LOG.debug("reading size at "+ offset + ": " + in[offset] + " " + in[offset + 1] + " " + in[offset + 2] + " " + in[offset + 3] + " ");
    int totalBytes = BytesUtils.readIntLittleEndian(in, offset);
    if (DEBUG) LOG.debug("will read "+ totalBytes + " bytes");
    currentValueCt = 0;
    currentValue = 0;
    bitReader.prepare(in, offset + 4, totalBytes);
    if (DEBUG) LOG.debug("will read next from " + (offset + totalBytes + 4));
    return offset + totalBytes + 4;
  }
}