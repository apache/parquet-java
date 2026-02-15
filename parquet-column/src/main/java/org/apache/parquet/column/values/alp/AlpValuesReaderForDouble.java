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
package org.apache.parquet.column.values.alp;

import static org.apache.parquet.column.values.alp.AlpConstants.*;

import java.nio.ByteBuffer;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * ALP values reader for DOUBLE type with lazy per-vector decoding.
 *
 * <p>Reads ALP-encoded double values from the interleaved page layout.
 * Each vector is decoded on first access using BytePackerForLong-based unpacking.
 */
public class AlpValuesReaderForDouble extends AlpValuesReader {

  private double[] decodedValues;

  public AlpValuesReaderForDouble() {
    super();
  }

  @Override
  protected void allocateDecodedBuffer(int capacity) {
    this.decodedValues = new double[capacity];
  }

  @Override
  public double readDouble() {
    if (currentIndex >= totalCount) {
      throw new ParquetDecodingException("ALP double data was already exhausted.");
    }
    ensureVectorDecoded();
    int indexInVector = currentIndex % vectorSize;
    currentIndex++;
    return decodedValues[indexInVector];
  }

  @Override
  protected void decodeVector(int vectorIdx) {
    int vectorLen = getVectorLength(vectorIdx);
    int pos = getVectorDataPosition(vectorIdx);

    // Read AlpInfo (4 bytes): exponent(1) + factor(1) + numExceptions(2)
    int exponent = vectorsData.get(pos) & 0xFF;
    int factor = vectorsData.get(pos + 1) & 0xFF;
    int numExceptions = getShortLE(vectorsData, pos + 2) & 0xFFFF;
    pos += ALP_INFO_SIZE;

    // Read ForInfo (9 bytes for double): frameOfReference(8) + bitWidth(1)
    long frameOfReference = getLongLE(vectorsData, pos);
    int bitWidth = vectorsData.get(pos + 8) & 0xFF;
    pos += DOUBLE_FOR_INFO_SIZE;

    // Unpack bit-packed values using BytePackerForLong
    long[] deltas = new long[vectorLen];
    if (bitWidth > 0) {
      pos = unpackLongsWithBytePacker(vectorsData, pos, deltas, vectorLen, bitWidth);
    }

    // Reverse Frame of Reference and decode
    for (int i = 0; i < vectorLen; i++) {
      long encoded = deltas[i] + frameOfReference;
      decodedValues[i] = AlpEncoderDecoder.decodeDouble(encoded, exponent, factor);
    }

    // Apply exceptions (positions and values are stored in separate blocks)
    if (numExceptions > 0) {
      int[] excPositions = new int[numExceptions];
      for (int e = 0; e < numExceptions; e++) {
        excPositions[e] = getShortLE(vectorsData, pos) & 0xFFFF;
        pos += Short.BYTES;
      }
      for (int e = 0; e < numExceptions; e++) {
        decodedValues[excPositions[e]] = getDoubleLE(vectorsData, pos);
        pos += Double.BYTES;
      }
    }
  }

  /**
   * Unpack longs from a ByteBuffer using BytePackerForLong, working in groups of 8.
   *
   * @return the position after the packed data
   */
  private int unpackLongsWithBytePacker(ByteBuffer buf, int pos, long[] output, int count, int bitWidth) {
    BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
    int numFullGroups = count / 8;
    int remaining = count % 8;

    for (int g = 0; g < numFullGroups; g++) {
      packer.unpack8Values(buf, pos, output, g * 8);
      pos += bitWidth;
    }

    // Handle partial last group: read only spec-required bytes, zero-pad, unpack
    // Spec: total packed size = ceil(count * bitWidth / 8)
    if (remaining > 0) {
      int totalPackedBytes = (count * bitWidth + 7) / 8;
      int alreadyRead = numFullGroups * bitWidth;
      int partialBytes = totalPackedBytes - alreadyRead;

      byte[] padded = new byte[bitWidth];
      for (int i = 0; i < partialBytes; i++) {
        padded[i] = buf.get(pos + i);
      }

      long[] temp = new long[8];
      packer.unpack8Values(padded, 0, temp, 0);
      System.arraycopy(temp, 0, output, numFullGroups * 8, remaining);
      pos += partialBytes;
    }

    return pos;
  }

  /** Read a little-endian unsigned short from a ByteBuffer at a specific position. */
  private static int getShortLE(ByteBuffer buf, int pos) {
    return (buf.get(pos) & 0xFF) | ((buf.get(pos + 1) & 0xFF) << 8);
  }

  /** Read a little-endian long from a ByteBuffer at a specific position. */
  private static long getLongLE(ByteBuffer buf, int pos) {
    return (buf.get(pos) & 0xFFL)
        | ((buf.get(pos + 1) & 0xFFL) << 8)
        | ((buf.get(pos + 2) & 0xFFL) << 16)
        | ((buf.get(pos + 3) & 0xFFL) << 24)
        | ((buf.get(pos + 4) & 0xFFL) << 32)
        | ((buf.get(pos + 5) & 0xFFL) << 40)
        | ((buf.get(pos + 6) & 0xFFL) << 48)
        | ((buf.get(pos + 7) & 0xFFL) << 56);
  }

  /** Read a little-endian double from a ByteBuffer at a specific position. */
  private static double getDoubleLE(ByteBuffer buf, int pos) {
    return Double.longBitsToDouble(getLongLE(buf, pos));
  }
}
