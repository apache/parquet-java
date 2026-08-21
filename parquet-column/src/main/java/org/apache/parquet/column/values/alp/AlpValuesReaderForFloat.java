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
  private byte[] unpackByteBuf;

  public AlpValuesReaderForFloat() {
    super();
  }

  @Override
  protected void allocateDecodedBuffer(int capacity) {
    this.decodedValues = new float[capacity];
    this.deltasBuffer = new int[capacity];
    this.unpackByteBuf = new byte[Integer.SIZE]; // max bit width for int = 32 bytes
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
  public float readFloat() {
    if (pageValueIndex >= totalCount) {
      throw new ParquetDecodingException("ALP float data was already exhausted.");
    }
    ensureVectorDecoded();
    int vectorSlot = pageValueIndex % vectorSize;
    pageValueIndex++;
    return decodedValues[vectorSlot];
  }

  @Override
  protected int decodeBody(int pos, int vectorNumber, int vectorLen, int exponent, int factor) {
    int frameOfReference = vectorsData.getInt(pos);
    int bitWidth = vectorsData.get(pos + Integer.BYTES) & 0xFF;
    if (bitWidth > Integer.SIZE) {
      throw new ParquetDecodingException(
          "Invalid ALP float bitWidth " + bitWidth + " > 32 in vector " + vectorNumber);
    }
    pos += FLOAT_FOR_INFO_SIZE;

    if (bitWidth > 0) {
      pos = unpackIntsWithBytePacker(vectorsData, pos, deltasBuffer, vectorLen, bitWidth);
    } else {
      Arrays.fill(deltasBuffer, 0, vectorLen, 0);
    }

    for (int i = 0; i < vectorLen; i++) {
      int encoded = deltasBuffer[i] + frameOfReference;
      decodedValues[i] = AlpEncoderDecoder.decodeFloat(encoded, exponent, factor);
    }
    return pos;
  }

  // Overwrite exception slots with their original float values
  @Override
  protected void applyExceptionValues(int pos, int numExceptions) {
    for (int e = 0; e < numExceptions; e++) {
      decodedValues[excPositionsBuffer[e]] = vectorsData.getFloat(pos);
      pos += Float.BYTES;
    }
  }

  /** Unpack bit-packed ints in groups of 8, returns position after packed data. */
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
