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
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * PFOR values reader for INT64 type with lazy per-vector decoding.
 *
 * <p>Reads PFOR-encoded long values from the interleaved page layout.
 * Each vector is decoded on first access using BytePackerForLong-based unpacking.
 *
 * <p>Per-vector format:
 * <pre>
 * PforVectorInfo (11B): frame_of_reference(8) + bit_width(1) + num_exceptions(2)
 * PackedValues: ceil(N * bit_width / 8) bytes
 * ExceptionPositions: num_exceptions * 2 bytes
 * ExceptionValues: num_exceptions * 8 bytes
 * </pre>
 */
public class PforValuesReaderForLong extends PforValuesReader {

  private long[] decodedValues;

  public PforValuesReaderForLong() {
    super();
  }

  @Override
  protected void allocateDecodedBuffer(int capacity) {
    this.decodedValues = new long[capacity];
  }

  @Override
  public long readLong() {
    if (currentIndex >= totalCount) {
      throw new ParquetDecodingException("PFOR long data was already exhausted.");
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

    // Read PforVectorInfo (11 bytes)
    long frameOfReference = getLongLE(vectorsData, pos);
    int bitWidth = vectorsData.get(pos + 8) & 0xFF;
    int numExceptions = getShortLE(vectorsData, pos + 9) & 0xFFFF;
    pos += INT64_VECTOR_INFO_SIZE;

    // Unpack bit-packed deltas
    long[] deltas = new long[vectorLen];
    if (bitWidth > 0) {
      pos = unpackLongsWithBytePacker(vectorsData, pos, deltas, vectorLen, bitWidth);
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
        decodedValues[excPositions[e]] = getLongLE(vectorsData, pos);
        pos += Long.BYTES;
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

}
