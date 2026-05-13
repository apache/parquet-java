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
package org.apache.parquet.column.values.plain;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes PLAIN-encoded booleans: one bit per value, packed 8 per byte, little-endian
 * bit order (bit 0 of each byte is the first value).
 *
 * <p>Direct bit extraction from the page ByteBuffer avoids the overhead of the generic
 * bit-packing machinery ({@code ByteBitPackingValuesReader}) and intermediate
 * {@code int[8]} buffers.
 *
 * <p>The batch path uses a static 256-entry lookup table that maps each byte value to
 * its 8 pre-decoded booleans. This enables {@code System.arraycopy} of 8 booleans per
 * byte (a single 64-bit memory operation in HotSpot) instead of 8 individual
 * comparison+store operations.
 */
public class BooleanPlainValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(BooleanPlainValuesReader.class);

  /**
   * Lookup table: BYTE_TO_BOOLS[b] contains the 8 boolean values for byte value b,
   * in little-endian bit order (bit 0 = index 0).
   */
  private static final boolean[][] BYTE_TO_BOOLS = new boolean[256][8];

  static {
    for (int b = 0; b < 256; b++) {
      for (int bit = 0; bit < 8; bit++) {
        BYTE_TO_BOOLS[b][bit] = ((b >>> bit) & 1) != 0;
      }
    }
  }

  private byte[] pageData;
  private int pageOffset;
  private int bitIndex;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());
    int effectiveBitLength = valueCount; // bitWidth = 1
    int length = BytesUtils.paddedByteCountFromBits(effectiveBitLength);
    length = Math.min(length, stream.available());
    ByteBuffer buf = stream.slice(length);

    // Bulk access: use backing array directly if available, otherwise copy once.
    if (buf.hasArray()) {
      pageData = buf.array();
      pageOffset = buf.arrayOffset() + buf.position();
    } else {
      pageData = new byte[length];
      buf.get(pageData);
      pageOffset = 0;
    }
    bitIndex = 0;
    updateNextOffset(length);
  }

  @Override
  public boolean readBoolean() {
    int byteIdx = pageOffset + (bitIndex >>> 3);
    int bitPos = bitIndex & 7;
    bitIndex++;
    return ((pageData[byteIdx] >>> bitPos) & 1) != 0;
  }

  @Override
  public void readBooleans(boolean[] dest, int offset, int count) {
    int i = 0;

    // Handle partial byte at current position
    int bitPos = bitIndex & 7;
    if (bitPos != 0) {
      int byteIdx = pageOffset + (bitIndex >>> 3);
      byte b = pageData[byteIdx];
      while (bitPos < 8 && i < count) {
        dest[offset + i] = ((b >>> bitPos) & 1) != 0;
        bitPos++;
        i++;
      }
    }

    // Process full bytes: 8 booleans per byte via lookup table + arraycopy
    int byteIdx = pageOffset + ((bitIndex + i) >>> 3);
    while (i + 8 <= count) {
      System.arraycopy(BYTE_TO_BOOLS[pageData[byteIdx] & 0xFF], 0, dest, offset + i, 8);
      byteIdx++;
      i += 8;
    }

    // Handle remaining bits in the last partial byte
    if (i < count) {
      byte b = pageData[byteIdx];
      int bp = 0;
      while (i < count) {
        dest[offset + i] = ((b >>> bp) & 1) != 0;
        bp++;
        i++;
      }
    }

    bitIndex += count;
  }

  @Override
  public void skip() {
    bitIndex++;
  }

  @Override
  public void skip(int n) {
    bitIndex += n;
  }
}
