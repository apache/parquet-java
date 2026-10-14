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
 * Encodes and decodes ALP pages.
 *
 * <p>The little-endian page body contains an offset table followed by encoded vectors:
 * <pre>
 * [header (7 bytes)][vector offsets][encoded vectors]
 * </pre>
 * The header contains:
 * <pre>
 * [compression mode (1 byte)][integer encoding (1 byte)][log2 vector size (1 byte)][element count (4 bytes)]
 * </pre>
 */
public final class AlpPageCodec {

  private AlpPageCodec() {}

  /** Create a sampling-based encoding preset for float data. */
  public static AlpCompression.AlpEncodingPreset createFloatSamplingPreset(float[] data, int count) {
    AlpSampler.FloatSampler sampler = new AlpSampler.FloatSampler();
    sampler.addSample(data, count);
    return sampler.selectPreset();
  }

  /** Create a sampling-based encoding preset for double data. */
  public static AlpCompression.AlpEncodingPreset createDoubleSamplingPreset(double[] data, int count) {
    AlpSampler.DoubleSampler sampler = new AlpSampler.DoubleSampler();
    sampler.addSample(data, count);
    return sampler.selectPreset();
  }

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

    List<AlpCompression.FloatCompressedVector> vectors = new ArrayList<>(numVectors);
    for (int i = 0; i < numVectors; i++) {
      int offset = i * vectorSize;
      int elementsInVector = Math.min(vectorSize, count - offset);
      float[] vectorInput = new float[elementsInVector];
      System.arraycopy(input, offset, vectorInput, 0, elementsInVector);
      vectors.add(AlpCompression.compressFloatVector(vectorInput, elementsInVector, preset));
    }

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

    writeHeader(output, 0, COMPRESSION_MODE_ALP, INTEGER_ENCODING_FOR, DEFAULT_VECTOR_SIZE_LOG, count);

    ByteBuffer body = ByteBuffer.wrap(output, HEADER_SIZE, bodySize).order(ByteOrder.LITTLE_ENDIAN);
    for (int offset : vectorOffsets) {
      body.putInt(offset);
    }

    for (int i = 0; i < numVectors; i++) {
      AlpCompression.FloatCompressedVector vector = vectors.get(i);
      int position = HEADER_SIZE + vectorOffsets[i];
      vector.store(output, position);
    }

    return totalSize;
  }

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

    ByteBuffer body = ByteBuffer.wrap(output, HEADER_SIZE, bodySize).order(ByteOrder.LITTLE_ENDIAN);
    for (int offset : vectorOffsets) {
      body.putInt(offset);
    }

    for (int i = 0; i < numVectors; i++) {
      AlpCompression.DoubleCompressedVector vector = vectors.get(i);
      int position = HEADER_SIZE + vectorOffsets[i];
      vector.store(output, position);
    }

    return totalSize;
  }

  /**
   * Decode ALP compressed page to float values.
   *
   * @param compressed the compressed page bytes
   * @param compressedSize number of compressed bytes
   * @param output output float array (must hold numElements values)
   * @param numElements number of elements to decode
   */
  public static void decodeFloats(byte[] compressed, int compressedSize, float[] output, int numElements) {
    Preconditions.checkArgument(
        compressedSize >= HEADER_SIZE, "compressed size too small for header: %s", compressedSize);

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

    ByteBuffer body = ByteBuffer.wrap(compressed, HEADER_SIZE, compressedSize - HEADER_SIZE)
        .order(ByteOrder.LITTLE_ENDIAN);
    int[] vectorOffsets = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      vectorOffsets[i] = body.getInt();
    }

    int outputOffset = 0;
    for (int vectorIndex = 0; vectorIndex < numVectors; vectorIndex++) {
      int elementsInVector;
      if (vectorIndex < storedNumElements / vectorSize) {
        elementsInVector = vectorSize;
      } else {
        elementsInVector = storedNumElements % vectorSize;
        if (elementsInVector == 0) elementsInVector = vectorSize;
      }

      int vectorPosition = HEADER_SIZE + vectorOffsets[vectorIndex];
      AlpCompression.FloatCompressedVector compressedVector =
          AlpCompression.FloatCompressedVector.load(compressed, vectorPosition, elementsInVector);

      float[] vectorOutput = new float[elementsInVector];
      AlpCompression.decompressFloatVector(compressedVector, vectorOutput);
      System.arraycopy(
          vectorOutput, 0, output, outputOffset, Math.min(elementsInVector, numElements - outputOffset));
      outputOffset += elementsInVector;
    }
  }

  public static void decodeDoubles(byte[] compressed, int compressedSize, double[] output, int numElements) {
    Preconditions.checkArgument(
        compressedSize >= HEADER_SIZE, "compressed size too small for header: %s", compressedSize);

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

    ByteBuffer body = ByteBuffer.wrap(compressed, HEADER_SIZE, compressedSize - HEADER_SIZE)
        .order(ByteOrder.LITTLE_ENDIAN);
    int[] vectorOffsets = new int[numVectors];
    for (int i = 0; i < numVectors; i++) {
      vectorOffsets[i] = body.getInt();
    }

    int outputOffset = 0;
    for (int vectorIndex = 0; vectorIndex < numVectors; vectorIndex++) {
      int elementsInVector;
      if (vectorIndex < storedNumElements / vectorSize) {
        elementsInVector = vectorSize;
      } else {
        elementsInVector = storedNumElements % vectorSize;
        if (elementsInVector == 0) elementsInVector = vectorSize;
      }

      int vectorPosition = HEADER_SIZE + vectorOffsets[vectorIndex];
      AlpCompression.DoubleCompressedVector compressedVector =
          AlpCompression.DoubleCompressedVector.load(compressed, vectorPosition, elementsInVector);

      double[] vectorOutput = new double[elementsInVector];
      AlpCompression.decompressDoubleVector(compressedVector, vectorOutput);
      System.arraycopy(
          vectorOutput, 0, output, outputOffset, Math.min(elementsInVector, numElements - outputOffset));
      outputOffset += elementsInVector;
    }
  }

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

  private static void writeHeader(
      byte[] output, int offset, int compressionMode, int integerEncoding, int logVectorSize, int numElements) {
    ByteBuffer buf = ByteBuffer.wrap(output, offset, HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) compressionMode);
    buf.put((byte) integerEncoding);
    buf.put((byte) logVectorSize);
    buf.putInt(numElements);
  }
}
