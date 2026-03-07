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
 * <p>On {@link #initFromPage}, reads the 7-byte header and offset array but does NOT
 * decode any vectors. Vectors are decoded on demand when values are accessed.
 * {@link #skip()} is O(1) — it just advances the index.
 */
abstract class AlpValuesReader extends ValuesReader {

  protected int vectorSize;
  protected int totalCount;
  protected int numVectors;
  protected int currentIndex;
  protected int decodedVectorIndex = -1;
  protected int[] vectorOffsets;
  protected byte[] rawData; // all data after header

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    int available = (int) stream.available();
    if (available < HEADER_SIZE) {
      throw new ParquetDecodingException("ALP page too small for header: " + available + " bytes");
    }

    // Read header
    byte[] headerBytes = new byte[HEADER_SIZE];
    stream.read(headerBytes);
    ByteBuffer header = ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);

    int compressionMode = header.get() & 0xFF;
    int integerEncoding = header.get() & 0xFF;
    int logVectorSize = header.get() & 0xFF;
    totalCount = header.getInt();

    if (compressionMode != COMPRESSION_MODE_ALP) {
      throw new ParquetDecodingException("Unsupported ALP compression mode: " + compressionMode);
    }
    if (integerEncoding != INTEGER_ENCODING_FOR) {
      throw new ParquetDecodingException("Unsupported ALP integer encoding: " + integerEncoding);
    }

    vectorSize = 1 << logVectorSize;
    numVectors = (totalCount + vectorSize - 1) / vectorSize;
    currentIndex = 0;
    decodedVectorIndex = -1;

    if (numVectors == 0) {
      vectorOffsets = new int[0];
      rawData = new byte[0];
      return;
    }

    // Read remaining data (offsets + vectors)
    int remaining = (int) stream.available();
    rawData = new byte[remaining];
    stream.read(rawData);

    // Parse offsets from rawData
    ByteBuffer body = ByteBuffer.wrap(rawData, 0, numVectors * OFFSET_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    vectorOffsets = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      vectorOffsets[i] = body.getInt();
    }
  }

  @Override
  public void skip() {
    currentIndex++;
  }

  @Override
  public void skip(int n) {
    currentIndex += n;
  }

  /** Number of elements in the given vector (last vector may be partial). */
  protected int elementsInVector(int vectorIdx) {
    if (vectorIdx < totalCount / vectorSize) {
      return vectorSize;
    }
    int rem = totalCount % vectorSize;
    return (rem == 0) ? vectorSize : rem;
  }
}
