/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.column.values.bitpacking;

import java.io.IOException;
import java.util.Arrays;
import java.nio.ByteBuffer;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;

public class ByteBitPackingValuesReader extends ValuesReader {
  private static final int VALUES_AT_A_TIME = 8; // because we're using unpack8Values()

  private static final Log LOG = Log.getLog(ByteBitPackingValuesReader.class);

  private final int bitWidth;
  private final BytePacker packer;
  private final int[] decoded = new int[VALUES_AT_A_TIME];
  private int decodedPosition = VALUES_AT_A_TIME - 1;
  private ByteBuffer encoded;
  private int encodedPos;
  private int nextOffset;

  public ByteBitPackingValuesReader(int bound, Packer packer) {
    this.bitWidth = BytesUtils.getWidthFromMaxInt(bound);
    this.packer = packer.newBytePacker(bitWidth);
  }
  
  @Override
  public int readInteger() {
    ++ decodedPosition;
    if (decodedPosition == decoded.length) {
      encoded.position(encodedPos);
      if (encodedPos + bitWidth > encoded.limit()) {
        // unpack8Values needs at least bitWidth bytes to read from,
        // We have to fill in 0 byte at the end of encoded bytes.
        byte[] tempEncode = new byte[bitWidth];
        encoded.get(tempEncode, 0, encoded.limit() - encodedPos);
        packer.unpack8Values(ByteBuffer.wrap(tempEncode), 0, decoded, 0);
      } else {
        packer.unpack8Values(encoded, encodedPos, decoded, 0);
      }      
      encodedPos += bitWidth;
      decodedPosition = 0;
    }
    return decoded[decodedPosition];
  }

  @Override
  public void initFromPage(int valueCount, ByteBuffer page, int offset)
      throws IOException {
    int effectiveBitLength = valueCount * bitWidth;
    int length = BytesUtils.paddedByteCountFromBits(effectiveBitLength); // ceil
    if (Log.DEBUG) LOG.debug("reading " + length + " bytes for " + valueCount + " values of size " + bitWidth + " bits." );
    this.encoded = page;
    this.encodedPos = offset;
    this.decodedPosition = VALUES_AT_A_TIME - 1;
    this.nextOffset = offset + length;
  }
  
  @Override
  public void initFromPage(int valueCount, byte[] page, int offset) throws IOException{
    this.initFromPage(valueCount, ByteBuffer.wrap(page), offset);
  }
  
  @Override
  public int getNextOffset() {
    return nextOffset;
  }

  @Override
  public void skip() {
    readInteger();
  }

}
