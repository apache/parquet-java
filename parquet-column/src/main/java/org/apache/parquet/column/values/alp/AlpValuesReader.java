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
 * │ 8 bytes │ 4B &times; numVectors │ (interleaved)│ (interleaved)│     │
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

  // Offset array: byte offset of each vector relative to start of compression body
  protected int[] vectorOffsets;

  // Raw vector data (everything after the header)
  protected ByteBuffer vectorsData;

  // Size of the offset array in bytes
  protected int offsetArraySize;

  AlpValuesReader() {
    this.currentIndex = 0;
    this.totalCount = 0;
    this.currentVectorIndex = -1;
  }

  @Override
  public void initFromPage(int valuesCount, ByteBufferInputStream stream)
      throws ParquetDecodingException, IOException {
    // Read 8-byte header sequentially
    ByteBuffer headerBuf = stream.slice(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    int version = headerBuf.get() & 0xFF;
    int compressionMode = headerBuf.get() & 0xFF;
    int integerEncoding = headerBuf.get() & 0xFF;
    int logVectorSize = headerBuf.get() & 0xFF;
    int numElements = headerBuf.getInt();

    // Validate header
    if (version != ALP_VERSION) {
      throw new ParquetDecodingException("Unsupported ALP version: " + version + ", expected " + ALP_VERSION);
    }
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

    this.vectorSize = 1 << logVectorSize;
    this.totalCount = numElements;
    this.numVectors = (numElements + vectorSize - 1) / vectorSize;
    this.currentIndex = 0;
    this.currentVectorIndex = -1;

    // Read offset array
    this.offsetArraySize = numVectors * Integer.BYTES;
    ByteBuffer offsetBuf = stream.slice(offsetArraySize).order(ByteOrder.LITTLE_ENDIAN);
    this.vectorOffsets = new int[numVectors];
    for (int v = 0; v < numVectors; v++) {
      vectorOffsets[v] = offsetBuf.getInt();
    }

    // Slice all remaining bytes as vectors data.
    // We call ByteBuffer.slice() to create a 0-based view because decodeVector
    // uses absolute get methods (e.g., vectorsData.get(pos)) which index from 0.
    int remainingBytes = (int) stream.available();
    ByteBuffer rawSlice = stream.slice(remainingBytes);
    this.vectorsData = rawSlice.slice().order(ByteOrder.LITTLE_ENDIAN);

    // Allocate decoded values buffer
    allocateDecodedBuffer(vectorSize);
  }

  /**
   * Get the number of values in a specific vector.
   */
  protected int getVectorLength(int vectorIdx) {
    if (vectorIdx < numVectors - 1) {
      return vectorSize;
    }
    // Last vector may be partial
    int lastVectorLen = totalCount % vectorSize;
    return lastVectorLen == 0 ? vectorSize : lastVectorLen;
  }

  /**
   * Get the byte position in vectorsData where a vector starts.
   * The offset is relative to the compression body (after header),
   * but vectorsData starts after the offset array, so we subtract offsetArraySize.
   */
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

  /**
   * Ensure the vector containing currentIndex is decoded.
   */
  protected void ensureVectorDecoded() {
    int vectorIdx = currentIndex / vectorSize;
    if (vectorIdx != currentVectorIndex) {
      decodeVector(vectorIdx);
      currentVectorIndex = vectorIdx;
    }
  }

  /**
   * Allocate the decoded values buffer with the given capacity.
   */
  protected abstract void allocateDecodedBuffer(int capacity);

  /**
   * Decode a specific vector from the raw data.
   */
  protected abstract void decodeVector(int vectorIdx);
}
