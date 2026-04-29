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

/**
 * Abstract base class for ALP values readers with lazy per-vector decoding.
 *
 * <p>Reads ALP-encoded values from the interleaved page layout:
 * <pre>
 * ┌─────────┬──────────────────────┬──────────────┬──────────────┬─────┐
 * │ Header  │ Offset Array         │ Vector 0     │ Vector 1     │ ... │
 * │ 7 bytes │ 4B &times; numVectors │ (interleaved)│ (interleaved)│     │
 * └─────────┴──────────────────────┴──────────────┴──────────────┴─────┘
 * </pre>
 *
 * <p>Each vector is decoded lazily on first access. Skipping values does not
 * trigger decoding of intermediate vectors.
 */
abstract class AlpValuesReader extends ValuesReader {

  protected int vectorSize;
  protected int totalCount;
  protected int numVectors;
  protected int currentIndex;
  protected int currentVectorIndex;

  protected int[] vectorOffsets;
  protected ByteBuffer vectorsData;
  protected int offsetArraySize;

  AlpValuesReader() {
    this.currentIndex = 0;
    this.totalCount = 0;
    this.currentVectorIndex = -1;
  }

  @Override
  public void initFromPage(int valuesCount, ByteBufferInputStream stream)
      throws ParquetDecodingException, IOException {
    ByteBuffer headerBuf = stream.slice(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    int compressionMode = headerBuf.get() & 0xFF;
    int integerEncoding = headerBuf.get() & 0xFF;
    int logVectorSize = headerBuf.get() & 0xFF;
    int numElements = headerBuf.getInt();

    if (compressionMode != ALP_COMPRESSION_MODE) {
      throw new ParquetDecodingException("Unsupported ALP compression mode: " + compressionMode);
    }
    if (integerEncoding != ALP_INTEGER_ENCODING_FOR) {
      throw new ParquetDecodingException("Unsupported ALP integer encoding: " + integerEncoding);
    }
    if (logVectorSize < MIN_LOG_VECTOR_SIZE || logVectorSize > MAX_LOG_VECTOR_SIZE) {
      throw new ParquetDecodingException("Invalid ALP log vector size: " + logVectorSize + ", must be between "
          + MIN_LOG_VECTOR_SIZE + " and " + MAX_LOG_VECTOR_SIZE);
    }
    if (numElements < 0) {
      throw new ParquetDecodingException("Invalid ALP element count: " + numElements);
    }
    if (numElements != valuesCount) {
      throw new ParquetDecodingException(
          "ALP header element count " + numElements + " does not match page valuesCount " + valuesCount);
    }

    this.vectorSize = 1 << logVectorSize;
    this.totalCount = numElements;
    this.numVectors = (numElements + vectorSize - 1) / vectorSize;
    this.currentIndex = 0;
    this.currentVectorIndex = -1;

    this.offsetArraySize = numVectors * Integer.BYTES;
    ByteBuffer offsetBuf = stream.slice(offsetArraySize).order(ByteOrder.LITTLE_ENDIAN);
    this.vectorOffsets = new int[numVectors];
    for (int v = 0; v < numVectors; v++) {
      vectorOffsets[v] = offsetBuf.getInt();
    }

    // Slice remaining bytes into a 0-based view so decodeVector can use
    // absolute get methods (vectorsData.get(pos)) directly.
    int remainingBytes = (int) stream.available();
    ByteBuffer rawSlice = stream.slice(remainingBytes);
    this.vectorsData = rawSlice.slice().order(ByteOrder.LITTLE_ENDIAN);

    allocateDecodedBuffer(vectorSize);
  }

  protected int getVectorLength(int vectorIdx) {
    if (vectorIdx < numVectors - 1) {
      return vectorSize;
    }
    // Last vector may be partial
    int lastVectorLen = totalCount % vectorSize;
    return lastVectorLen == 0 ? vectorSize : lastVectorLen;
  }

  // Offsets in the page are relative to the compression body (after header),
  // but vectorsData starts after the offset array, so adjust.
  protected int getVectorDataPosition(int vectorIdx) {
    return vectorOffsets[vectorIdx] - offsetArraySize;
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

  protected void ensureVectorDecoded() {
    int vectorIdx = currentIndex / vectorSize;
    if (vectorIdx != currentVectorIndex) {
      decodeVector(vectorIdx);
      currentVectorIndex = vectorIdx;
    }
  }

  protected abstract void allocateDecodedBuffer(int capacity);

  protected abstract void decodeVector(int vectorIdx);

  // Explicit little-endian reads using absolute get(), since absolute get() ignores ByteBuffer order.
  protected static int getShortLE(ByteBuffer buf, int pos) {
    return (buf.get(pos) & 0xFF) | ((buf.get(pos + 1) & 0xFF) << 8);
  }

  protected static int getIntLE(ByteBuffer buf, int pos) {
    return (buf.get(pos) & 0xFF)
        | ((buf.get(pos + 1) & 0xFF) << 8)
        | ((buf.get(pos + 2) & 0xFF) << 16)
        | ((buf.get(pos + 3) & 0xFF) << 24);
  }

  protected static long getLongLE(ByteBuffer buf, int pos) {
    return (buf.get(pos) & 0xFFL)
        | ((buf.get(pos + 1) & 0xFFL) << 8)
        | ((buf.get(pos + 2) & 0xFFL) << 16)
        | ((buf.get(pos + 3) & 0xFFL) << 24)
        | ((buf.get(pos + 4) & 0xFFL) << 32)
        | ((buf.get(pos + 5) & 0xFFL) << 40)
        | ((buf.get(pos + 6) & 0xFFL) << 48)
        | ((buf.get(pos + 7) & 0xFFL) << 56);
  }
}
