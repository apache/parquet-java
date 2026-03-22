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
  private long[] deltasBuffer;
  private int[] excPositionsBuffer;
  private final long[] unpackPadBuf = new long[8];
  private byte[] unpackByteBuf;

  public AlpValuesReaderForDouble() {
    super();
  }

  @Override
  protected void allocateDecodedBuffer(int capacity) {
    this.decodedValues = new double[capacity];
    this.deltasBuffer = new long[capacity];
    this.excPositionsBuffer = new int[capacity];
    this.unpackByteBuf = new byte[Long.SIZE]; // max bit width for long = 64 bytes
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

    int exponent = vectorsData.get(pos) & 0xFF;
    int factor = vectorsData.get(pos + 1) & 0xFF;
    int numExceptions = getShortLE(vectorsData, pos + 2) & 0xFFFF;
    pos += ALP_INFO_SIZE;

    long frameOfReference = getLongLE(vectorsData, pos);
    int bitWidth = vectorsData.get(pos + 8) & 0xFF;
    pos += DOUBLE_FOR_INFO_SIZE;

    if (bitWidth > 0) {
      pos = unpackLongsWithBytePacker(vectorsData, pos, deltasBuffer, vectorLen, bitWidth);
    } else {
      java.util.Arrays.fill(deltasBuffer, 0, vectorLen, 0L);
    }

    for (int i = 0; i < vectorLen; i++) {
      long encoded = deltasBuffer[i] + frameOfReference;
      decodedValues[i] = AlpEncoderDecoder.decodeDouble(encoded, exponent, factor);
    }

    if (numExceptions > 0) {
      for (int e = 0; e < numExceptions; e++) {
        excPositionsBuffer[e] = getShortLE(vectorsData, pos) & 0xFFFF;
        pos += Short.BYTES;
      }
      for (int e = 0; e < numExceptions; e++) {
        decodedValues[excPositionsBuffer[e]] = getDoubleLE(vectorsData, pos);
        pos += Double.BYTES;
      }
    }
  }

  private int unpackLongsWithBytePacker(ByteBuffer buf, int pos, long[] output, int count, int bitWidth) {
    BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
    int numFullGroups = count / 8;
    int remaining = count % 8;

    for (int g = 0; g < numFullGroups; g++) {
      packer.unpack8Values(buf, pos, output, g * 8);
      pos += bitWidth;
    }

    // Last group might have fewer than 8 values; zero-pad and unpack,
    // but only advance pos by the actual bytes in the page.
    if (remaining > 0) {
      int totalPackedBytes = (count * bitWidth + 7) / 8;
      int alreadyRead = numFullGroups * bitWidth;
      int partialBytes = totalPackedBytes - alreadyRead;

      for (int i = 0; i < partialBytes; i++) {
        unpackByteBuf[i] = buf.get(pos + i);
      }
      for (int i = partialBytes; i < bitWidth; i++) {
        unpackByteBuf[i] = 0;
      }

      packer.unpack8Values(unpackByteBuf, 0, unpackPadBuf, 0);
      System.arraycopy(unpackPadBuf, 0, output, numFullGroups * 8, remaining);
      pos += partialBytes;
    }

    return pos;
  }

  private static int getShortLE(ByteBuffer buf, int pos) {
    return (buf.get(pos) & 0xFF) | ((buf.get(pos + 1) & 0xFF) << 8);
  }

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

  private static double getDoubleLE(ByteBuffer buf, int pos) {
    return Double.longBitsToDouble(getLongLE(buf, pos));
  }
}
