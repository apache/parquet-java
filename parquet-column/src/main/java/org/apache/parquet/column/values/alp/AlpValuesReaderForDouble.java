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

import static org.apache.parquet.column.values.alp.AlpConstants.DOUBLE_FOR_INFO_SIZE;
import static org.apache.parquet.column.values.alp.AlpConstants.DOUBLE_MAX_EXPONENT;
import static org.apache.parquet.column.values.alp.AlpConstants.PACK_GROUP_SIZE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.bytes.BytesUtils;
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
  private final long[] unpackPadBuf = new long[PACK_GROUP_SIZE];
  private byte[] unpackByteBuf;

  public AlpValuesReaderForDouble() {
    super();
  }

  @Override
  protected void allocateDecodedBuffer(int capacity) {
    this.decodedValues = new double[capacity];
    this.deltasBuffer = new long[capacity];
    this.unpackByteBuf = new byte[Long.SIZE]; // max bit width for long = 64 bytes
  }

  @Override
  protected int maxExponent() {
    return DOUBLE_MAX_EXPONENT;
  }

  @Override
  protected String typeName() {
    return "double";
  }

  @Override
  protected int forInfoSize() {
    return DOUBLE_FOR_INFO_SIZE;
  }

  @Override
  protected int exceptionValueSize() {
    return Double.BYTES;
  }

  @Override
  public double readDouble() {
    if (pageValueIndex >= totalValueCount) {
      throw new ParquetDecodingException("ALP double data was already exhausted.");
    }
    ensureVectorDecoded();
    int vectorValueIndex = pageValueIndex % valuesPerVector;
    pageValueIndex++;
    return decodedValues[vectorValueIndex];
  }

  @Override
  protected int readForInfo(int pos, int vectorIndex, ForInfo out) {
    out.base = vectorsData.getLong(pos);
    out.bitWidth = vectorsData.get(pos + Long.BYTES) & 0xFF;
    if (out.bitWidth > Long.SIZE) {
      throw new ParquetDecodingException(
          "Invalid ALP double bitWidth " + out.bitWidth + " > 64 in vector " + vectorIndex);
    }
    return pos + DOUBLE_FOR_INFO_SIZE;
  }

  @Override
  protected int decodePackedValues(int pos, int vectorSize, int exponent, int factor, ForInfo forInfo) {
    if (forInfo.bitWidth > 0) {
      pos = unpackLongsWithBytePacker(vectorsData, pos, deltasBuffer, vectorSize, forInfo.bitWidth);
    } else {
      Arrays.fill(deltasBuffer, 0, vectorSize, 0L);
    }

    for (int i = 0; i < vectorSize; i++) {
      long encoded = deltasBuffer[i] + forInfo.base;
      decodedValues[i] = AlpCodec.decodeDouble(encoded, exponent, factor);
    }
    return pos;
  }

  @Override
  protected void applyExceptions(int pos, int numExceptions, int vectorSize) {
    pos = readExceptionPositions(vectorsData, pos, numExceptions, vectorSize, excPositionsBuffer);
    applyExceptionValues(vectorsData, pos, numExceptions, excPositionsBuffer, decodedValues);
  }

  /**
   * Writes {@code numExceptions} raw double values over the decoded values at the given positions.
   * Static and fully parameterised so it can be exercised directly.
   */
  static void applyExceptionValues(
      ByteBuffer data, int pos, int numExceptions, int[] positions, double[] decodedValues) {
    for (int e = 0; e < numExceptions; e++) {
      decodedValues[positions[e]] = data.getDouble(pos);
      pos += Double.BYTES;
    }
  }

  private int unpackLongsWithBytePacker(ByteBuffer buf, int pos, long[] output, int count, int bitWidth) {
    BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
    int numFullGroups = count / PACK_GROUP_SIZE;
    int remaining = count % PACK_GROUP_SIZE;

    for (int g = 0; g < numFullGroups; g++) {
      packer.unpack8Values(buf, pos, output, g * PACK_GROUP_SIZE);
      pos += bitWidth;
    }

    // Last group might have fewer than 8 values; zero-pad and unpack,
    // but only advance pos by the actual bytes in the page.
    if (remaining > 0) {
      int totalPackedBytes = BytesUtils.paddedByteCountFromBits(count * bitWidth);
      int alreadyRead = numFullGroups * bitWidth;
      int partialBytes = totalPackedBytes - alreadyRead;

      for (int i = 0; i < partialBytes; i++) {
        unpackByteBuf[i] = buf.get(pos + i);
      }
      for (int i = partialBytes; i < bitWidth; i++) {
        unpackByteBuf[i] = 0;
      }

      packer.unpack8Values(unpackByteBuf, 0, unpackPadBuf, 0);
      System.arraycopy(unpackPadBuf, 0, output, numFullGroups * PACK_GROUP_SIZE, remaining);
      pos += partialBytes;
    }

    return pos;
  }
}
