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
package org.apache.parquet.column.values.bitpacking;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBitPackingValuesReader extends ValuesReader {
  private static final int VALUES_AT_A_TIME = 8; // because we're using unpack8Values()

  private static final Logger LOG = LoggerFactory.getLogger(ByteBitPackingValuesReader.class);

  private final int bitWidth;
  private final BytePacker packer;
  private final int[] decoded = new int[VALUES_AT_A_TIME];
  private int decodedPosition = VALUES_AT_A_TIME - 1;
  private ByteBufferInputStream in;

  public ByteBitPackingValuesReader(int bound, Packer packer) {
    this.bitWidth = BytesUtils.getWidthFromMaxInt(bound);
    this.packer = packer.newBytePacker(bitWidth);
  }

  @Override
  public int readInteger() {
    ++ decodedPosition;
    if (decodedPosition == decoded.length) {
      try {
        if (in.available() < bitWidth) {
          // unpack8Values needs at least bitWidth bytes to read from,
          // We have to fill in 0 byte at the end of encoded bytes.
          byte[] tempEncode = new byte[bitWidth];
          in.read(tempEncode, 0, in.available());
          packer.unpack8Values(tempEncode, 0, decoded, 0);
        } else {
          ByteBuffer encoded = in.slice(bitWidth);
          packer.unpack8Values(encoded, encoded.position(), decoded, 0);
        }
      } catch (IOException e) {
        throw new ParquetDecodingException("Failed to read packed values", e);
      }
      decodedPosition = 0;
    }
    return decoded[decodedPosition];
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream)
      throws IOException {
    int effectiveBitLength = valueCount * bitWidth;
    int length = BytesUtils.paddedByteCountFromBits(effectiveBitLength); // ceil
    LOG.debug("reading {} bytes for {} values of size {} bits.",
        length, valueCount, bitWidth);
    // work-around for null values. this will not happen for repetition or
    // definition levels (never null), but will happen when valueCount has not
    // been adjusted for null values in the data.
    length = Math.min(length, stream.available());
    this.in = stream.sliceStream(length);
    this.decodedPosition = VALUES_AT_A_TIME - 1;
  }

  @Override
  public void skip() {
    readInteger();
  }

}
