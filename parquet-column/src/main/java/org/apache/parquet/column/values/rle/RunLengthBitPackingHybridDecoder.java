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
package org.apache.parquet.column.values.rle;


import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 */
public class RunLengthBitPackingHybridDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(RunLengthBitPackingHybridDecoder.class);

  private final int bitWidth;
  private final BytePacker packer;
  private final ByteBufferInputStream in;

  /*
  Note: In an older version, this class used to use an enum to keep track of the mode. Switching to a boolean
  resulted in a measurable performance improvement.
   */
  boolean packed_mode;

  private int currentCount;
  private int currentValue;
  private int[] currentBuffer;

  public RunLengthBitPackingHybridDecoder(int bitWidth, InputStream in) {
    LOG.debug("decoding bitWidth {}", bitWidth);

    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    // Every place in ParquetMR that calls this constructor does so with a ByteBufferInputStream. If some other
    // user calls this with some other type, we can rely on the ClassCastException to be thrown.
    this.in = (ByteBufferInputStream)in;
  }

  public int readInt() throws IOException {
    if (currentCount == 0) {
      readNext();
    }
    --currentCount;
    int result;
    if (packed_mode) {
      return currentBuffer[currentBuffer.length - 1 - currentCount];
    } else {
      return currentValue;
    }
  }

  /*
  Note: An older version used to create a DataInputStream just to be able to call readFully. This object creation
  in the critical path was bad for performance. Since we're using the new ByteBufferInputStream, we can call
  its built-in readFully, along with other optimized methods like readUnsignedVarInt and
  readIntLittleEndianPaddedOnBitWidth, which replace the use of BytesUtils, which we can't count on being
  inlined.
   */

  private void readNext() throws IOException {
    Preconditions.checkArgument(in.available() > 0, "Reading past RLE/BitPacking stream.");
    final int header = in.readUnsignedVarInt();
    packed_mode = (header & 1) != 0;
    if (!packed_mode) {
      currentCount = header >>> 1;
      LOG.debug("reading {} values RLE", currentCount);
      currentValue = in.readIntLittleEndianPaddedOnBitWidth(bitWidth);
    } else {
      int numGroups = header >>> 1;
      currentCount = numGroups * 8;
      LOG.debug("reading {} values BIT PACKED", currentCount);
      currentBuffer = new int[currentCount]; // TODO: reuse a buffer
      byte[] bytes = new byte[numGroups * bitWidth];
      // At the end of the file RLE data though, there might not be that many bytes left.
      int bytesToRead = (int)Math.ceil(currentCount * bitWidth / 8.0);
      bytesToRead = Math.min(bytesToRead, in.available());
      in.readFully(bytes, 0, bytesToRead);
      for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex += bitWidth) {
        // It's faster to use an array with unpack8Values than to use a ByteBuffer.
        packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
      }
    }
  }
}
