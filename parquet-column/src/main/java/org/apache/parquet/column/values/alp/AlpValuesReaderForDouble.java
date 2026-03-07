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

import org.apache.parquet.io.ParquetDecodingException;

/**
 * ALP values reader for double columns with lazy per-vector decoding.
 */
public class AlpValuesReaderForDouble extends AlpValuesReader {

  private double[] decodedBuffer;

  @Override
  public double readDouble() {
    if (currentIndex >= totalCount) {
      throw new ParquetDecodingException("ALP double reader exhausted at index " + currentIndex);
    }
    int vectorIdx = currentIndex / vectorSize;
    int posInVector = currentIndex % vectorSize;
    ensureVectorDecoded(vectorIdx);
    currentIndex++;
    return decodedBuffer[posInVector];
  }

  private void ensureVectorDecoded(int vectorIdx) {
    if (vectorIdx == decodedVectorIndex) {
      return;
    }
    int numElements = elementsInVector(vectorIdx);
    int dataOffset = vectorOffsets[vectorIdx];
    AlpCompression.DoubleCompressedVector cv =
        AlpCompression.DoubleCompressedVector.load(rawData, dataOffset, numElements);
    decodedBuffer = new double[numElements];
    AlpCompression.decompressDoubleVector(cv, decodedBuffer);
    decodedVectorIndex = vectorIdx;
  }
}
