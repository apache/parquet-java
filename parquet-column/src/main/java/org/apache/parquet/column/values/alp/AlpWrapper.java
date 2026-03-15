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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.Preconditions;

/**
 * Top-level API for ALP page-level encoding and decoding.
 *
 * <p>Page layout (offset-based interleaved, matching C++ AlpWrapper):
 * <pre>
 * [Header(7B)][Offset0..OffsetN-1][Vector0][Vector1]...[VectorN-1]
 * </pre>
 * where each Vector = [AlpInfo(4B)][ForInfo(5B/9B)][PackedValues][ExcPositions][ExcValues]
 *
 * <p>Header format (7 bytes, little-endian):
 * <pre>
 * [compression_mode(1B)][integer_encoding(1B)][log_vector_size(1B)][num_elements(4B LE)]
 * </pre>
 */
public final class AlpWrapper {

  private AlpWrapper() {}

  // ========== Sampling presets ==========

  /** Create a sampling-based encoding preset for float data. */
  public static AlpCompression.AlpEncodingPreset createFloatSamplingPreset(float[] data, int count) {
    AlpSampler.FloatSampler sampler = new AlpSampler.FloatSampler();
    sampler.addSample(data, count);
    return sampler.finalizeSampling();
  }

  /** Create a sampling-based encoding preset for double data. */
  public static AlpCompression.AlpEncodingPreset createDoubleSamplingPreset(double[] data, int count) {
    AlpSampler.DoubleSampler sampler = new AlpSampler.DoubleSampler();
    sampler.addSample(data, count);
    return sampler.finalizeSampling();
  }

  // ========== Encode floats ==========

  /**
   * Encode float data into ALP compressed page format.
   *
   * @param input the float values to encode
   * @param count number of values
   * @param output output byte array (must be at least maxCompressedSizeFloat(count) bytes)
   * @param preset the encoding preset from sampling
   * @return the number of compressed bytes written
   */
  public static int encodeFloats(float[] input, int count, byte[] output, AlpCompression.AlpEncodingPreset preset) {
    Preconditions.checkArgument(count >= 0, "count must be non-negative, got: %s", count);
    if (count == 0) {
      writeHeader(output, 0, COMPRESSION_MODE_ALP, INTEGER_ENCODING_FOR, DEFAULT_VECTOR_SIZE_LOG, 0);
      return HEADER_SIZE;
    }

    int vectorSize = DEFAULT_VECTOR_SIZE;
    int numVectors = (count + vectorSize - 1) / vectorSize;

    // Phase 1: Compress all vectors
    List<AlpCompression.FloatCompressedVector> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      int offset = i * vectorSize;
      int elementsInVector = Math.min(vectorSize, count - offset);
      float[] vectorInput = new float[elementsInVector];
      System.arraycopy(input, offset, vectorInput, 0, elementsInVector);
      vectors.add(AlpCompression.compressFloatVector(vectorInput, elementsInVector, preset));
    }

    // Phase 2: Calculate offsets
    int offsetsSectionSize = numVectors * OFFSET_SIZE;
    int[] vectorOffsets = new int[numVectors];
    int currentOffset = offsetsSectionSize;
    for (int i = 0; i < numVectors; i++) {
      vectorOffsets[i] = currentOffset;
      currentOffset +=
          ALP_INFO_SIZE + FLOAT_FOR_INFO_SIZE + vectors.get(i).dataStoredSize();
    }
    int bodySize = currentOffset;
    int totalSize = HEADER_SIZE + bodySize;

    // Phase 3: Write header
    writeHeader(output, 0, COMPRESSION_MODE_ALP, INTEGER_ENCODING_FOR, DEFAULT_VECTOR_SIZE_LOG, count);

    // Phase 4: Write offsets
    ByteBuffer buf = ByteBuffer.wrap(output, HEADER_SIZE, bodySize).order(ByteOrder.LITTLE_ENDIAN);
    for (int offset : vectorOffsets) {
      buf.putInt(offset);
    }

    // Phase 5: Write interleaved vectors
    for (int i = 0; i < numVectors; i++) {
      AlpCompression.FloatCompressedVector v = vectors.get(i);
      int pos = HEADER_SIZE + vectorOffsets[i];
      v.store(output, pos);
    }

    return totalSize;
  }

  // ========== Encode doubles ==========

  public static int encodeDoubles(double[] input, int count, byte[] output, AlpCompression.AlpEncodingPreset preset) {
    Preconditions.checkArgument(count >= 0, "count must be non-negative, got: %s", count);
    if (count == 0) {
      writeHeader(output, 0, COMPRESSION_MODE_ALP, INTEGER_ENCODING_FOR, DEFAULT_VECTOR_SIZE_LOG, 0);
      return HEADER_SIZE;
    }

    int vectorSize = DEFAULT_VECTOR_SIZE;
    int numVectors = (count + vectorSize - 1) / vectorSize;

    List<AlpCompression.DoubleCompressedVector> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      int offset = i * vectorSize;
      int elementsInVector = Math.min(vectorSize, count - offset);
      double[] vectorInput = new double[elementsInVector];
      System.arraycopy(input, offset, vectorInput, 0, elementsInVector);
      vectors.add(AlpCompression.compressDoubleVector(vectorInput, elementsInVector, preset));
    }

    int offsetsSectionSize = numVectors * OFFSET_SIZE;
    int[] vectorOffsets = new int[numVectors];
    int currentOffset = offsetsSectionSize;
    for (int i = 0; i < numVectors; i++) {
      vectorOffsets[i] = currentOffset;
      currentOffset +=
          ALP_INFO_SIZE + DOUBLE_FOR_INFO_SIZE + vectors.get(i).dataStoredSize();
    }
    int bodySize = currentOffset;
    int totalSize = HEADER_SIZE + bodySize;

    writeHeader(output, 0, COMPRESSION_MODE_ALP, INTEGER_ENCODING_FOR, DEFAULT_VECTOR_SIZE_LOG, count);

    ByteBuffer buf = ByteBuffer.wrap(output, HEADER_SIZE, bodySize).order(ByteOrder.LITTLE_ENDIAN);
    for (int offset : vectorOffsets) {
      buf.putInt(offset);
    }

    for (int i = 0; i < numVectors; i++) {
      AlpCompression.DoubleCompressedVector v = vectors.get(i);
      int pos = HEADER_SIZE + vectorOffsets[i];
      v.store(output, pos);
    }

    return totalSize;
  }

  // ========== Decode floats ==========

  /**
   * Decode ALP compressed page to float values.
   *
   * @param compressed the compressed page bytes
   * @param compSize number of compressed bytes
   * @param output output float array (must hold numElements values)
   * @param numElements number of elements to decode
   */
  public static void decodeFloats(byte[] compressed, int compSize, float[] output, int numElements) {
    Preconditions.checkArgument(compSize >= HEADER_SIZE, "compressed size too small for header: %s", compSize);

    ByteBuffer header = ByteBuffer.wrap(compressed, 0, HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    int compressionMode = header.get() & 0xFF;
    int integerEncoding = header.get() & 0xFF;
    int logVectorSize = header.get() & 0xFF;
    int storedNumElements = header.getInt();

    Preconditions.checkArgument(
        compressionMode == COMPRESSION_MODE_ALP, "unsupported compression mode: %s", compressionMode);
    Preconditions.checkArgument(
        integerEncoding == INTEGER_ENCODING_FOR, "unsupported integer encoding: %s", integerEncoding);

    int vectorSize = 1 << logVectorSize;
    int numVectors = (storedNumElements + vectorSize - 1) / vectorSize;

    if (numVectors == 0) return;

    // Read offsets
    ByteBuffer body =
        ByteBuffer.wrap(compressed, HEADER_SIZE, compSize - HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    int[] vectorOffsets = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      vectorOffsets[i] = body.getInt();
    }

    // Decode each vector
    int outputOffset = 0;
    for (int vi = 0; vi < numVectors; vi++) {
      int elementsInVector;
      if (vi < storedNumElements / vectorSize) {
        elementsInVector = vectorSize;
      } else {
        elementsInVector = storedNumElements % vectorSize;
        if (elementsInVector == 0) elementsInVector = vectorSize;
      }

      int vectorPos = HEADER_SIZE + vectorOffsets[vi];
      AlpCompression.FloatCompressedVector cv =
          AlpCompression.FloatCompressedVector.load(compressed, vectorPos, elementsInVector);

      float[] vectorOutput = new float[elementsInVector];
      AlpCompression.decompressFloatVector(cv, vectorOutput);
      System.arraycopy(
          vectorOutput, 0, output, outputOffset, Math.min(elementsInVector, numElements - outputOffset));
      outputOffset += elementsInVector;
    }
  }

  // ========== Decode doubles ==========

  public static void decodeDoubles(byte[] compressed, int compSize, double[] output, int numElements) {
    Preconditions.checkArgument(compSize >= HEADER_SIZE, "compressed size too small for header: %s", compSize);

    ByteBuffer header = ByteBuffer.wrap(compressed, 0, HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    int compressionMode = header.get() & 0xFF;
    int integerEncoding = header.get() & 0xFF;
    int logVectorSize = header.get() & 0xFF;
    int storedNumElements = header.getInt();

    Preconditions.checkArgument(
        compressionMode == COMPRESSION_MODE_ALP, "unsupported compression mode: %s", compressionMode);
    Preconditions.checkArgument(
        integerEncoding == INTEGER_ENCODING_FOR, "unsupported integer encoding: %s", integerEncoding);

    int vectorSize = 1 << logVectorSize;
    int numVectors = (storedNumElements + vectorSize - 1) / vectorSize;

    if (numVectors == 0) return;

    ByteBuffer body =
        ByteBuffer.wrap(compressed, HEADER_SIZE, compSize - HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    int[] vectorOffsets = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      vectorOffsets[i] = body.getInt();
    }

    int outputOffset = 0;
    for (int vi = 0; vi < numVectors; vi++) {
      int elementsInVector;
      if (vi < storedNumElements / vectorSize) {
        elementsInVector = vectorSize;
      } else {
        elementsInVector = storedNumElements % vectorSize;
        if (elementsInVector == 0) elementsInVector = vectorSize;
      }

      int vectorPos = HEADER_SIZE + vectorOffsets[vi];
      AlpCompression.DoubleCompressedVector cv =
          AlpCompression.DoubleCompressedVector.load(compressed, vectorPos, elementsInVector);

      double[] vectorOutput = new double[elementsInVector];
      AlpCompression.decompressDoubleVector(cv, vectorOutput);
      System.arraycopy(
          vectorOutput, 0, output, outputOffset, Math.min(elementsInVector, numElements - outputOffset));
      outputOffset += elementsInVector;
    }
  }

  // ========== Max compressed size ==========

  /** Maximum compressed size for float data of given element count. */
  public static long maxCompressedSizeFloat(int numElements) {
    long size = HEADER_SIZE;
    long numVectors = (numElements + DEFAULT_VECTOR_SIZE - 1) / DEFAULT_VECTOR_SIZE;
    size += numVectors * OFFSET_SIZE;
    size += numVectors * (ALP_INFO_SIZE + FLOAT_FOR_INFO_SIZE);
    // Worst case: all values bit-packed at full width + all exceptions
    size += (long) numElements * Float.BYTES; // packed values worst case
    size += (long) numElements * Float.BYTES; // exception values
    size += (long) numElements * POSITION_SIZE; // exception positions
    return size;
  }

  /** Maximum compressed size for double data of given element count. */
  public static long maxCompressedSizeDouble(int numElements) {
    long size = HEADER_SIZE;
    long numVectors = (numElements + DEFAULT_VECTOR_SIZE - 1) / DEFAULT_VECTOR_SIZE;
    size += numVectors * OFFSET_SIZE;
    size += numVectors * (ALP_INFO_SIZE + DOUBLE_FOR_INFO_SIZE);
    size += (long) numElements * Double.BYTES;
    size += (long) numElements * Double.BYTES;
    size += (long) numElements * POSITION_SIZE;
    return size;
  }

  // ========== Header helpers ==========

  private static void writeHeader(
      byte[] output, int offset, int compressionMode, int integerEncoding, int logVectorSize, int numElements) {
    ByteBuffer buf = ByteBuffer.wrap(output, offset, HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) compressionMode);
    buf.put((byte) integerEncoding);
    buf.put((byte) logVectorSize);
    buf.putInt(numElements);
  }
}
