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
package org.apache.parquet.column.values.pfor;

import static org.apache.parquet.column.values.pfor.PforConstants.*;

import java.nio.ByteBuffer;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * PFOR values reader for INT32 type with lazy per-vector decoding.
 *
 * <p>Reads PFOR-encoded int values from the interleaved page layout.
 * Each vector is decoded on first access using BytePacker-based unpacking.
 *
 * <p>Per-vector format:
 * <pre>
 * PforVectorInfo (7B): frame_of_reference(4) + bit_width(1) + num_exceptions(2)
 * PackedValues: ceil(N * bit_width / 8) bytes
 * ExceptionPositions: num_exceptions * 2 bytes
 * ExceptionValues: num_exceptions * 4 bytes
 * </pre>
 */
public class PforValuesReaderForInt extends PforValuesReader {

  private int[] decodedValues;

  public PforValuesReaderForInt() {
    super();
  }

  @Override
  protected void allocateDecodedBuffer(int capacity) {
    this.decodedValues = new int[capacity];
  }

  @Override
  public int readInteger() {
    if (currentIndex >= totalCount) {
      throw new ParquetDecodingException("PFOR int data was already exhausted.");
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

    // Read PforVectorInfo (7 bytes)
    int frameOfReference = getIntLE(vectorsData, pos);
    int bitWidth = vectorsData.get(pos + 4) & 0xFF;
    int numExceptions = getShortLE(vectorsData, pos + 5) & 0xFFFF;
    pos += INT32_VECTOR_INFO_SIZE;

    // Unpack bit-packed deltas
    int[] deltas = new int[vectorLen];
    if (bitWidth > 0) {
      pos = unpackIntsWithBytePacker(vectorsData, pos, deltas, vectorLen, bitWidth);
    }

    // Add frame of reference to reconstruct values
    for (int i = 0; i < vectorLen; i++) {
      decodedValues[i] = deltas[i] + frameOfReference;
    }

    // Overwrite exception slots with their original values
    if (numExceptions > 0) {
      int[] excPositions = new int[numExceptions];
      for (int e = 0; e < numExceptions; e++) {
        excPositions[e] = getShortLE(vectorsData, pos) & 0xFFFF;
        pos += Short.BYTES;
      }
      for (int e = 0; e < numExceptions; e++) {
        decodedValues[excPositions[e]] = getIntLE(vectorsData, pos);
        pos += Integer.BYTES;
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

}
