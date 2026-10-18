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

import static org.apache.parquet.column.values.alp.AlpConstants.FLOAT_FOR_INFO_SIZE;
import static org.apache.parquet.column.values.alp.AlpConstants.FLOAT_MAX_EXPONENT;
import static org.apache.parquet.column.values.alp.AlpConstants.PACK_GROUP_SIZE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.bytes.BytesUtils;
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
  private int[] deltasBuffer;
  private final int[] unpackPadBuf = new int[PACK_GROUP_SIZE];
  private final byte[] unpackByteBuf = new byte[Integer.SIZE]; // 8 values x 32 bits = 32 bytes

  public AlpValuesReaderForFloat() {
    super();
  }

  @Override
  protected void ensureDecodedCapacity(int capacity) {
    if (decodedValues == null || decodedValues.length < capacity) {
      this.decodedValues = new float[capacity];
      this.deltasBuffer = new int[capacity];
    }
  }

  @Override
  protected int maxExponent() {
    return FLOAT_MAX_EXPONENT;
  }

  @Override
  protected String typeName() {
    return "float";
  }

  @Override
  protected int forInfoSize() {
    return FLOAT_FOR_INFO_SIZE;
  }

  @Override
  protected int exceptionValueSize() {
    return Float.BYTES;
  }

  @Override
  public float readFloat() {
    if (pageValueIndex >= totalValueCount) {
      throw new ParquetDecodingException("ALP float data was already exhausted.");
    }
    ensureVectorDecoded();
    int vectorValueIndex = pageValueIndex % valuesPerVector;
    pageValueIndex++;
    return decodedValues[vectorValueIndex];
  }

  @Override
  protected int readForInfo(int pos, int vectorIndex, ForInfo out) {
    out.base = vectorsData.getInt(pos);
    out.bitWidth = BytesUtils.readUnsignedByte(vectorsData, pos + Integer.BYTES);
    if (out.bitWidth > Integer.SIZE) {
      throw new ParquetDecodingException(
          "Invalid ALP float bitWidth " + out.bitWidth + " > 32 in vector " + vectorIndex);
    }
    return pos + FLOAT_FOR_INFO_SIZE;
  }

  @Override
  protected int decodePackedValues(int pos, int vectorSize, int exponent, int factor, ForInfo forInfo) {
    // Narrow the base before adding: the sum must wrap in 32 bits exactly as the writer's did.
    int frameOfReference = (int) forInfo.base;
    if (forInfo.bitWidth > 0) {
      pos = unpackIntsWithBytePacker(vectorsData, pos, deltasBuffer, vectorSize, forInfo.bitWidth);
    } else {
      Arrays.fill(deltasBuffer, 0, vectorSize, 0);
    }

    for (int i = 0; i < vectorSize; i++) {
      int encoded = deltasBuffer[i] + frameOfReference;
      decodedValues[i] = AlpCodec.decodeFloat(encoded, exponent, factor);
    }
    return pos;
  }

  @Override
  protected void applyExceptions(int pos, int numExceptions, int vectorSize) {
    pos = readExceptionPositions(vectorsData, pos, numExceptions, vectorSize, excPositionsBuffer);
    applyExceptionValues(vectorsData, pos, numExceptions, excPositionsBuffer, decodedValues);
  }

  /**
   * Writes {@code numExceptions} raw float values over the decoded values at the given positions.
   * Static and fully parameterised so it can be exercised directly.
   */
  static void applyExceptionValues(
      ByteBuffer data, int pos, int numExceptions, int[] positions, float[] decodedValues) {
    for (int e = 0; e < numExceptions; e++) {
      decodedValues[positions[e]] = data.getFloat(pos);
      pos += Float.BYTES;
    }
  }

  private int unpackIntsWithBytePacker(ByteBuffer buf, int pos, int[] output, int count, int bitWidth) {
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int numFullGroups = count / PACK_GROUP_SIZE;
    int remaining = count % PACK_GROUP_SIZE;

    for (int g = 0; g < numFullGroups; g++) {
      packer.unpack8Values(buf, pos, output, g * PACK_GROUP_SIZE);
      pos += bitWidth;
    }

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
