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
package org.apache.parquet.arrow.writer;

import java.io.IOException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;

/**
 * Encodes Parquet repetition and definition levels for flat schemas.
 *
 * <p>For flat schemas, repetition levels are always 0 and definition levels are either
 * 0 (null) or max (present). These produce single-value RLE runs encoded in O(1).
 */
final class LevelEncoder {

  private LevelEncoder() {}

  /**
   * Encodes a constant level value repeated {@code count} times in O(1).
   *
   * <p>Produces a single RLE run. The RLE/Bit-Packing Hybrid format for a single run is:
   * <ul>
   *   <li>VarInt header: (count &lt;&lt; 1) | 0  (bit 0 = 0 indicates RLE)</li>
   *   <li>Value: the repeated value in ceil(bitWidth/8) bytes, little-endian</li>
   * </ul>
   *
   * <p>V1 data pages require a 4-byte little-endian length prefix before the level data.
   *
   * @param value the constant level value to repeat
   * @param count the number of values
   * @param maxLevel the maximum possible level (determines bit width)
   * @return encoded level bytes with V1 length prefix, or empty if maxLevel is 0
   */
  static BytesInput encodeConstant(int value, int count, int maxLevel) {
    if (maxLevel == 0) {
      // Bit width 0: no level data written (reader knows it's always 0)
      return BytesInput.empty();
    }

    int bitWidth = getBitWidth(maxLevel);
    int valueByteCount = (bitWidth + 7) / 8;

    // RLE header varint: (count << 1) encodes the run length with mode bit = 0 (RLE)
    BytesInput headerBytes = BytesInput.fromUnsignedVarInt(count << 1);

    // RLE value: little-endian encoding of the repeated value
    byte[] valueEncoded = new byte[valueByteCount];
    for (int i = 0; i < valueByteCount; i++) {
      valueEncoded[i] = (byte) ((value >>> (i * 8)) & 0xFF);
    }
    BytesInput valueBytes = BytesInput.from(valueEncoded);

    // The RLE payload (header + value)
    BytesInput rlePayload = BytesInput.concat(headerBytes, valueBytes);

    // V1 format: [4-byte LE length of level data][level data]
    return BytesInput.concat(BytesInput.fromInt((int) rlePayload.size()), rlePayload);
  }

  /**
   * Encodes definition levels from an Arrow validity bitmap.
   *
   * <p>Uses the standard RLE encoder which naturally produces efficient runs for
   * data with clustered nulls/non-nulls.
   *
   * <p>V1 data pages require a 4-byte little-endian length prefix before the level data.
   *
   * @param validityBuf the Arrow validity buffer (bit i = 1 means value present)
   * @param offset the starting row index in the validity buffer
   * @param length the number of rows to encode
   * @param maxDefinitionLevel the DL value for non-null rows
   * @return encoded level bytes with V1 length prefix
   * @throws IOException if encoding fails
   */
  static BytesInput encodeFromValidityBitmap(
      org.apache.arrow.memory.ArrowBuf validityBuf, int offset, int length,
      int maxDefinitionLevel) throws IOException {
    int bitWidth = getBitWidth(maxDefinitionLevel);
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(
        bitWidth, length, length,
        org.apache.parquet.bytes.HeapByteBufferAllocator.getInstance());

    for (int i = 0; i < length; i++) {
      int byteIndex = (offset + i) >> 3;
      int bitIndex = (offset + i) & 7;
      boolean isSet = ((validityBuf.getByte(byteIndex) >> bitIndex) & 1) == 1;
      encoder.writeInt(isSet ? maxDefinitionLevel : 0);
    }

    BytesInput encoded = encoder.toBytes();
    return BytesInput.concat(BytesInput.fromInt((int) encoded.size()), encoded);
  }

  /** Returns the minimum number of bits needed to represent values up to maxLevel. */
  private static int getBitWidth(int maxLevel) {
    return 32 - Integer.numberOfLeadingZeros(maxLevel);
  }
}
