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
  protected int pageValueIndex;
  protected int currentVectorNumber;

  protected int[] vectorOffsets;
  protected ByteBuffer vectorsData;
  protected int offsetArraySize;

  // Scratch buffer for exception positions within a vector; shared by both readers (int[] in each).
  protected int[] excPositionsBuffer;

  AlpValuesReader() {
    this.pageValueIndex = 0;
    this.totalCount = 0;
    this.currentVectorNumber = -1;
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
    // ALP's num_elements is the count of non-null values that went through encoding;
    // valuesCount is the page row count, which is larger when the column has nulls.
    // The two are equal only for required (non-null) columns.
    if (numElements > valuesCount) {
      throw new ParquetDecodingException(
          "ALP header element count " + numElements + " exceeds page valuesCount " + valuesCount);
    }

    this.vectorSize = 1 << logVectorSize;
    this.totalCount = numElements;
    this.numVectors = (numElements + vectorSize - 1) / vectorSize;
    this.pageValueIndex = 0;
    this.currentVectorNumber = -1;

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
    this.excPositionsBuffer = new int[vectorSize];
  }

  protected int getVectorLength(int vectorNumber) {
    if (vectorNumber < numVectors - 1) {
      return vectorSize;
    }
    // Last vector may be partial
    int lastVectorLen = totalCount % vectorSize;
    return lastVectorLen == 0 ? vectorSize : lastVectorLen;
  }

  // Offsets in the page are relative to the compression body (after header),
  // but vectorsData starts after the offset array, so adjust.
  protected int getVectorDataPosition(int vectorNumber) {
    return vectorOffsets[vectorNumber] - offsetArraySize;
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    if (n < 0 || pageValueIndex + n > totalCount) {
      throw new ParquetDecodingException(String.format(
          "Cannot skip this many elements. Current index: %d. Skip %d. Total count: %d",
          pageValueIndex, n, totalCount));
    }
    pageValueIndex += n;
  }

  protected void ensureVectorDecoded() {
    int vectorNumber = pageValueIndex / vectorSize;
    if (vectorNumber != currentVectorNumber) {
      decodeVector(vectorNumber);
      currentVectorNumber = vectorNumber;
    }
  }

  /**
   * Decodes one vector: parses and validates the shared ALP/FOR header fields, delegates the
   * type-specific numeric work (FOR read, bit-unpacking, decode loop, exception values) to hooks,
   * and reads the shared exception-position list in between. Runs once per vector, so the virtual
   * hook calls are off the per-value hot path.
   */
  protected final void decodeVector(int vectorNumber) {
    int vectorLen = getVectorLength(vectorNumber);
    int pos = getVectorDataPosition(vectorNumber);

    int exponent = vectorsData.get(pos) & 0xFF;
    int factor = vectorsData.get(pos + 1) & 0xFF;
    int numExceptions = vectorsData.getShort(pos + 2) & 0xFFFF;
    pos += ALP_INFO_SIZE;

    if (exponent > maxExponent()) {
      throw new ParquetDecodingException("Invalid ALP " + typeName() + " exponent " + exponent + " in vector "
          + vectorNumber + ", max is " + maxExponent());
    }
    if (factor > exponent) {
      throw new ParquetDecodingException("Invalid ALP " + typeName() + " factor " + factor + " > exponent "
          + exponent + " in vector " + vectorNumber);
    }
    if (numExceptions > vectorLen) {
      throw new ParquetDecodingException("Invalid ALP numExceptions " + numExceptions + " > vectorLen "
          + vectorLen + " in vector " + vectorNumber);
    }

    pos = decodeBody(pos, vectorNumber, vectorLen, exponent, factor);

    if (numExceptions > 0) {
      pos = readExceptionPositions(pos, numExceptions, vectorLen);
      applyExceptionValues(pos, numExceptions);
    }
  }

  /** Reads {@code numExceptions} little-endian uint16 positions into {@link #excPositionsBuffer}. */
  private int readExceptionPositions(int pos, int numExceptions, int vectorLen) {
    for (int e = 0; e < numExceptions; e++) {
      excPositionsBuffer[e] = vectorsData.getShort(pos) & 0xFFFF;
      if (excPositionsBuffer[e] >= vectorLen) {
        throw new ParquetDecodingException("ALP exception position " + excPositionsBuffer[e]
            + " out of bounds for vectorLen " + vectorLen);
      }
      pos += Short.BYTES;
    }
    return pos;
  }

  protected abstract void allocateDecodedBuffer(int capacity);

  /** The maximum exponent for this value type (used to validate the header). */
  protected abstract int maxExponent();

  /** The value type name ("float" / "double") used in decoding error messages. */
  protected abstract String typeName();

  /**
   * Reads the FOR header and bit-packed body starting at {@code pos}, decodes every slot into the
   * type-specific decoded buffer, and returns the position just past the packed body.
   */
  protected abstract int decodeBody(int pos, int vectorNumber, int vectorLen, int exponent, int factor);

  /**
   * Reads {@code numExceptions} original values starting at {@code pos} and writes them into the
   * decoded buffer at the positions previously read into {@link #excPositionsBuffer}.
   */
  protected abstract void applyExceptionValues(int pos, int numExceptions);
}
