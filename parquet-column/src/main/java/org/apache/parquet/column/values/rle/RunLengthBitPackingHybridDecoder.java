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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.Preconditions;
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

  private static enum MODE {
    RLE,
    PACKED
  }

  private final int bitWidth;
  private final BytePacker packer;
  private final ByteBuffer buffer;

  private MODE mode;
  private int currentCount;
  private int currentValue;
  private int[] currentBuffer;
  private int currentBufferLength;

  // Reusable buffers to avoid per-run allocation in PACKED mode
  private int[] packedValuesBuffer = new int[0];
  private byte[] packedBytesBuffer = new byte[0];

  public RunLengthBitPackingHybridDecoder(int bitWidth, ByteBuffer buffer) {
    LOG.debug("decoding bitWidth {}", bitWidth);

    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Reads the next int value from the RLE/Bit-Packing hybrid stream.
   *
   * @return the next decoded integer value
   * @throws ParquetDecodingException if a decoding error occurs
   */
  public int readInt() {
    if (currentCount == 0) {
      readNext();
    }
    --currentCount;
    int result;
    switch (mode) {
      case RLE:
        result = currentValue;
        break;
      case PACKED:
        result = currentBuffer[currentBufferLength - 1 - currentCount];
        break;
      default:
        throw new ParquetDecodingException("not a valid mode " + mode);
    }
    return result;
  }

  private void readNext() {
    Preconditions.checkArgument(buffer.hasRemaining(), "Reading past RLE/BitPacking stream.");
    final int header = BytesUtils.readUnsignedVarInt(buffer);
    mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
    switch (mode) {
      case RLE:
        currentCount = header >>> 1;
        LOG.debug("reading {} values RLE", currentCount);
        currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(buffer, bitWidth);
        break;
      case PACKED:
        int numGroups = header >>> 1;
        currentCount = numGroups * 8;
        currentBufferLength = currentCount;
        LOG.debug("reading {} values BIT PACKED", currentCount);
        if (packedValuesBuffer.length < currentCount) {
          packedValuesBuffer = new int[currentCount];
        }
        currentBuffer = packedValuesBuffer;
        int bytesRequired = numGroups * bitWidth;
        if (packedBytesBuffer.length < bytesRequired) {
          packedBytesBuffer = new byte[bytesRequired];
        }
        // At the end of the file RLE data though, there might not be that many bytes left.
        int bytesToRead = (int) Math.ceil(currentCount * bitWidth / 8.0);
        bytesToRead = Math.min(bytesToRead, buffer.remaining());
        buffer.get(packedBytesBuffer, 0, bytesToRead);
        for (int valueIndex = 0, byteIndex = 0;
            valueIndex < currentCount;
            valueIndex += 8, byteIndex += bitWidth) {
          packer.unpack8Values(packedBytesBuffer, byteIndex, currentBuffer, valueIndex);
        }
        break;
      default:
        throw new ParquetDecodingException("not a valid mode " + mode);
    }
  }
}
