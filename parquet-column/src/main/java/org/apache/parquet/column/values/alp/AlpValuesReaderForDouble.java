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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ALP values reader for DOUBLE type.
 *
 * <p>Reads ALP-encoded double values from a page and decodes them back to double values.
 *
 * <p>Page Layout:
 * <pre>
 * ┌─────────┬────────────────┬────────────────┬─────────────┐
 * │ Header  │ AlpInfo Array  │ ForInfo Array  │ Data Array  │
 * │ 8 bytes │ 4B × N vectors │ 9B × N vectors │ Variable    │
 * └─────────┴────────────────┴────────────────┴─────────────┘
 * </pre>
 */
public class AlpValuesReaderForDouble extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(AlpValuesReaderForDouble.class);

  // Decoded double values (eagerly decoded)
  private double[] decodedValues;
  private int currentIndex;
  private int totalCount;

  public AlpValuesReaderForDouble() {
    this.currentIndex = 0;
    this.totalCount = 0;
  }

  @Override
  public void initFromPage(int valuesCount, ByteBufferInputStream stream)
      throws ParquetDecodingException, IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());

    // Read and validate header
    ByteBuffer headerBuf = stream.slice(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    int version = headerBuf.get() & 0xFF;
    int compressionMode = headerBuf.get() & 0xFF;
    int integerEncoding = headerBuf.get() & 0xFF;
    int logVectorSize = headerBuf.get() & 0xFF;
    int numElements = headerBuf.getInt();

    if (version != ALP_VERSION) {
      throw new ParquetDecodingException("Unsupported ALP version: " + version + ", expected " + ALP_VERSION);
    }
    if (compressionMode != ALP_COMPRESSION_MODE) {
      throw new ParquetDecodingException("Unsupported ALP compression mode: " + compressionMode);
    }
    if (integerEncoding != ALP_INTEGER_ENCODING_FOR) {
      throw new ParquetDecodingException("Unsupported ALP integer encoding: " + integerEncoding);
    }

    int vectorSize = 1 << logVectorSize;
    int numVectors = (numElements + vectorSize - 1) / vectorSize;

    this.totalCount = numElements;
    this.decodedValues = new double[numElements];
    this.currentIndex = 0;

    // Read AlpInfo array
    ByteBuffer alpInfoBuf = stream.slice(ALP_INFO_SIZE * numVectors).order(ByteOrder.LITTLE_ENDIAN);
    int[] exponents = new int[numVectors];
    int[] factors = new int[numVectors];
    int[] numExceptions = new int[numVectors];

    for (int v = 0; v < numVectors; v++) {
      exponents[v] = alpInfoBuf.get() & 0xFF;
      factors[v] = alpInfoBuf.get() & 0xFF;
      numExceptions[v] = alpInfoBuf.getShort() & 0xFFFF;
    }

    // Read ForInfo array
    ByteBuffer forInfoBuf = stream.slice(DOUBLE_FOR_INFO_SIZE * numVectors).order(ByteOrder.LITTLE_ENDIAN);
    long[] frameOfReference = new long[numVectors];
    int[] bitWidths = new int[numVectors];

    for (int v = 0; v < numVectors; v++) {
      frameOfReference[v] = forInfoBuf.getLong();
      bitWidths[v] = forInfoBuf.get() & 0xFF;
    }

    // Decode each vector
    for (int v = 0; v < numVectors; v++) {
      int vectorStart = v * vectorSize;
      int vectorEnd = Math.min(vectorStart + vectorSize, numElements);
      int vectorLen = vectorEnd - vectorStart;

      // Calculate packed data size
      int packedBytes = (vectorLen * bitWidths[v] + 7) / 8;

      // Read and unpack values
      long[] deltas = new long[vectorLen];
      if (bitWidths[v] > 0) {
        ByteBuffer packedBuf = stream.slice(packedBytes).order(ByteOrder.LITTLE_ENDIAN);
        unpackLongs(packedBuf, deltas, vectorLen, bitWidths[v]);
      }
      // else: all deltas are 0 (bitWidth == 0)

      // Reverse FOR and decode
      for (int i = 0; i < vectorLen; i++) {
        long encoded = deltas[i] + frameOfReference[v];
        decodedValues[vectorStart + i] = AlpEncoderDecoder.decodeDouble(encoded, exponents[v], factors[v]);
      }

      // Apply exceptions
      if (numExceptions[v] > 0) {
        ByteBuffer excPosBuf = stream.slice(numExceptions[v] * 2).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer excValBuf =
            stream.slice(numExceptions[v] * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);

        for (int e = 0; e < numExceptions[v]; e++) {
          int pos = excPosBuf.getShort() & 0xFFFF;
          double val = excValBuf.getDouble();
          decodedValues[vectorStart + pos] = val;
        }
      }
    }
  }

  /**
   * Unpack longs from a byte buffer using the specified bit width.
   * Note: This method reads from the buffer's current position, using absolute indexing
   * relative to that position.
   */
  private void unpackLongs(ByteBuffer packed, long[] values, int count, int bitWidth) {
    int basePosition = packed.position();
    int bitPos = 0;
    for (int i = 0; i < count; i++) {
      long value = 0;
      int bitsToRead = bitWidth;
      int destBit = 0;

      while (bitsToRead > 0) {
        int byteIdx = bitPos / 8;
        int bitIdx = bitPos % 8;
        int bitsAvailable = 8 - bitIdx;
        int bitsThisRound = Math.min(bitsAvailable, bitsToRead);
        // Use long arithmetic to avoid overflow for bit widths > 31
        long mask = (1L << bitsThisRound) - 1;
        // Use basePosition + byteIdx to account for sliced buffer position
        long bits = ((packed.get(basePosition + byteIdx) & 0xFFL) >>> bitIdx) & mask;
        value |= (bits << destBit);
        bitPos += bitsThisRound;
        destBit += bitsThisRound;
        bitsToRead -= bitsThisRound;
      }

      values[i] = value;
    }
  }

  @Override
  public double readDouble() {
    if (currentIndex >= totalCount) {
      throw new ParquetDecodingException("ALP double data was already exhausted.");
    }
    return decodedValues[currentIndex++];
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    if (n < 0 || currentIndex + n > totalCount) {
      throw new ParquetDecodingException(String.format(
          "Cannot skip this many elements. Current index: %d. Skip %d. Total count: %d",
          currentIndex, n, totalCount));
    }
    currentIndex += n;
  }
}
