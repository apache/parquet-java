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
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * ALP values reader for FLOAT type with lazy per-vector decoding.
 *
 * <p>Reads ALP-encoded float values from the interleaved page layout.
 * Each vector is decoded on first access using BytePacker-based unpacking.
 */
public class AlpValuesReaderForFloat extends AlpValuesReader {

  private float[] decodedValues;

  public AlpValuesReaderForFloat() {
    super();
  }

  @Override
  protected void allocateDecodedBuffer(int capacity) {
    this.decodedValues = new float[capacity];
  }

  @Override
  public float readFloat() {
    if (currentIndex >= totalCount) {
      throw new ParquetDecodingException("ALP float data was already exhausted.");
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

    int frameOfReference = getIntLE(vectorsData, pos);
    int bitWidth = vectorsData.get(pos + 4) & 0xFF;
    pos += FLOAT_FOR_INFO_SIZE;

    int[] deltas = new int[vectorLen];
    if (bitWidth > 0) {
      pos = unpackIntsWithBytePacker(vectorsData, pos, deltas, vectorLen, bitWidth);
    }

    // Reverse the frame-of-reference subtraction, then decimal-decode
    for (int i = 0; i < vectorLen; i++) {
      int encoded = deltas[i] + frameOfReference;
      decodedValues[i] = AlpEncoderDecoder.decodeFloat(encoded, exponent, factor);
    }

    // Overwrite exception slots with their original float values
    if (numExceptions > 0) {
      int[] excPositions = new int[numExceptions];
      for (int e = 0; e < numExceptions; e++) {
        excPositions[e] = getShortLE(vectorsData, pos) & 0xFFFF;
        pos += Short.BYTES;
      }
      for (int e = 0; e < numExceptions; e++) {
        decodedValues[excPositions[e]] = getFloatLE(vectorsData, pos);
        pos += Float.BYTES;
      }
    }
  }

  /** Unpack bit-packed ints in groups of 8, returns position after packed data. */
  private int unpackIntsWithBytePacker(ByteBuffer buf, int pos, int[] output, int count, int bitWidth) {
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int numFullGroups = count / 8;
    int remaining = count % 8;

    for (int g = 0; g < numFullGroups; g++) {
      packer.unpack8Values(buf, pos, output, g * 8);
      pos += bitWidth;
    }

    if (remaining > 0) {
      int totalPackedBytes = (count * bitWidth + 7) / 8;
      int alreadyRead = numFullGroups * bitWidth;
      int partialBytes = totalPackedBytes - alreadyRead;

      byte[] padded = new byte[bitWidth];
      for (int i = 0; i < partialBytes; i++) {
        padded[i] = buf.get(pos + i);
      }

      int[] temp = new int[8];
      packer.unpack8Values(padded, 0, temp, 0);
      System.arraycopy(temp, 0, output, numFullGroups * 8, remaining);
      pos += partialBytes;
    }

    return pos;
  }

  private static int getShortLE(ByteBuffer buf, int pos) {
    return (buf.get(pos) & 0xFF) | ((buf.get(pos + 1) & 0xFF) << 8);
  }

  // Explicit LE reads instead of relying on ByteBuffer order, since
  // we use absolute get() which ignores the buffer's byte order.
  private static int getIntLE(ByteBuffer buf, int pos) {
    return (buf.get(pos) & 0xFF)
        | ((buf.get(pos + 1) & 0xFF) << 8)
        | ((buf.get(pos + 2) & 0xFF) << 16)
        | ((buf.get(pos + 3) & 0xFF) << 24);
  }

  private static float getFloatLE(ByteBuffer buf, int pos) {
    return Float.intBitsToFloat(getIntLE(buf, pos));
  }
}
