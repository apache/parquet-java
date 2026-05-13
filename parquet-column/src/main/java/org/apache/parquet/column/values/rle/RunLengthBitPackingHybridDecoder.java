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
  private final InputStream in;

  private MODE mode;
  private int currentCount;
  private int currentValue;
  private int[] currentBuffer;
  // Saved packed bytes for bitWidth=1 boolean optimization (lookup table decode)
  private byte[] packedBytesBuffer;

  public RunLengthBitPackingHybridDecoder(int bitWidth, InputStream in) {
    LOG.debug("decoding bitWidth {}", bitWidth);

    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    this.in = in;
  }

  public int readInt() throws IOException {
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
        result = currentBuffer[currentBuffer.length - 1 - currentCount];
        break;
      default:
        throw new ParquetDecodingException("not a valid mode " + mode);
    }
    return result;
  }

  /**
   * Reads {@code count} int values into {@code dest} starting at {@code offset}.
   * This avoids per-value virtual dispatch overhead by batching across RLE runs
   * and packed groups.
   *
   * @param dest   destination array
   * @param offset start index in dest
   * @param count  number of values to read
   */
  public void readInts(int[] dest, int offset, int count) throws IOException {
    int remaining = count;
    int pos = offset;
    while (remaining > 0) {
      if (currentCount == 0) {
        readNext();
      }
      int batchSize = Math.min(remaining, currentCount);
      switch (mode) {
        case RLE:
          java.util.Arrays.fill(dest, pos, pos + batchSize, currentValue);
          break;
        case PACKED:
          int startIdx = currentBuffer.length - currentCount;
          System.arraycopy(currentBuffer, startIdx, dest, pos, batchSize);
          break;
        default:
          throw new ParquetDecodingException("not a valid mode " + mode);
      }
      currentCount -= batchSize;
      remaining -= batchSize;
      pos += batchSize;
    }
  }

  /**
   * Lookup table for bitWidth=1: maps each byte to its 8 boolean values.
   * Used by {@link #readBooleans} PACKED path to bypass the int[] intermediate.
   */
  private static final boolean[][] BYTE_TO_BOOLS = new boolean[256][8];

  static {
    for (int b = 0; b < 256; b++) {
      for (int bit = 0; bit < 8; bit++) {
        BYTE_TO_BOOLS[b][bit] = ((b >>> bit) & 1) != 0;
      }
    }
  }

  /**
   * Reads {@code count} boolean values into {@code dest} starting at {@code offset}.
   * For RLE runs, uses {@code Arrays.fill} with a single boolean value.
   * For packed groups, uses a lookup table to decode 8 booleans per byte directly
   * from the raw packed bytes, bypassing the int[] intermediate buffer.
   *
   * @param dest   destination array
   * @param offset start index in dest
   * @param count  number of values to read
   */
  public void readBooleans(boolean[] dest, int offset, int count) throws IOException {
    int remaining = count;
    int pos = offset;
    while (remaining > 0) {
      if (currentCount == 0) {
        readNext();
      }
      int batchSize = Math.min(remaining, currentCount);
      switch (mode) {
        case RLE:
          java.util.Arrays.fill(dest, pos, pos + batchSize, currentValue != 0);
          break;
        case PACKED:
          // For bitWidth=1, read directly from packedBytesBuffer via lookup table
          int bitOff = currentBuffer.length - currentCount;
          int written = 0;

          // Handle partial byte alignment
          int bitPos = bitOff & 7;
          if (bitPos != 0) {
            int byteIdx = bitOff >>> 3;
            byte b = packedBytesBuffer[byteIdx];
            while (bitPos < 8 && written < batchSize) {
              dest[pos + written] = ((b >>> bitPos) & 1) != 0;
              bitPos++;
              written++;
            }
          }

          // Process full bytes via lookup table
          int byteIdx = (bitOff + written) >>> 3;
          while (written + 8 <= batchSize) {
            System.arraycopy(BYTE_TO_BOOLS[packedBytesBuffer[byteIdx] & 0xFF], 0, dest, pos + written, 8);
            byteIdx++;
            written += 8;
          }

          // Handle remaining bits
          if (written < batchSize) {
            byte b = packedBytesBuffer[byteIdx];
            int bp = 0;
            while (written < batchSize) {
              dest[pos + written] = ((b >>> bp) & 1) != 0;
              bp++;
              written++;
            }
          }
          break;
        default:
          throw new ParquetDecodingException("not a valid mode " + mode);
      }
      currentCount -= batchSize;
      remaining -= batchSize;
      pos += batchSize;
    }
  }

  private void readNext() throws IOException {
    Preconditions.checkArgument(in.available() > 0, "Reading past RLE/BitPacking stream.");
    final int header = BytesUtils.readUnsignedVarInt(in);
    mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
    switch (mode) {
      case RLE:
        currentCount = header >>> 1;
        LOG.debug("reading {} values RLE", currentCount);
        currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
        break;
      case PACKED:
        int numGroups = header >>> 1;
        currentCount = numGroups * 8;
        LOG.debug("reading {} values BIT PACKED", currentCount);
        currentBuffer = new int[currentCount]; // TODO: reuse a buffer
        byte[] bytes = new byte[numGroups * bitWidth];
        // At the end of the file RLE data though, there might not be that many bytes left.
        int bytesToRead = (int) Math.ceil(currentCount * bitWidth / 8.0);
        bytesToRead = Math.min(bytesToRead, in.available());
        new DataInputStream(in).readFully(bytes, 0, bytesToRead);
        packedBytesBuffer = bytes;
        for (int valueIndex = 0, byteIndex = 0;
            valueIndex < currentCount;
            valueIndex += 8, byteIndex += bitWidth) {
          packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
        }
        break;
      default:
        throw new ParquetDecodingException("not a valid mode " + mode);
    }
  }
}
