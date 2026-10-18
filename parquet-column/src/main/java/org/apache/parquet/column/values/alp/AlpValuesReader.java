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

import static org.apache.parquet.column.values.alp.AlpConstants.ALP_COMPRESSION_MODE;
import static org.apache.parquet.column.values.alp.AlpConstants.ALP_HEADER_SIZE;
import static org.apache.parquet.column.values.alp.AlpConstants.ALP_INFO_SIZE;
import static org.apache.parquet.column.values.alp.AlpConstants.ALP_INTEGER_ENCODING_FOR;
import static org.apache.parquet.column.values.alp.AlpConstants.MAX_LOG_VECTOR_SIZE;
import static org.apache.parquet.column.values.alp.AlpConstants.MIN_LOG_VECTOR_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Abstract base class for ALP values readers with lazy per-vector decoding.
 *
 * <p>Reads ALP-encoded values from the interleaved page layout:
 * <pre>
 * ┌─────────┬──────────────────────┬──────────────┬──────────────┬─────┐
 * │ Header  │ Offset Array         │ Vector 0     │ Vector 1     │ ... │
 * │ 7 bytes │ 4B × numVectors      │ (interleaved)│ (interleaved)│     │
 * └─────────┴──────────────────────┴──────────────┴──────────────┴─────┘
 * </pre>
 *
 * <p>Each vector is decoded lazily on first access. Skipping values does not
 * trigger decoding of intermediate vectors.
 */
abstract class AlpValuesReader extends ValuesReader {

  private static final int BEFORE_FIRST_VECTOR = -1;

  protected int valuesPerVector;
  protected int totalValueCount = 0;
  protected int numVectors;
  protected int pageValueIndex = 0;
  protected int currentVectorIndex = BEFORE_FIRST_VECTOR;

  // One entry per vector plus a trailing entry for the end of the vector data, so the entry after
  // any vector's offset is always where that vector ends.
  protected int[] vectorOffsets;
  protected ByteBuffer vectorsData;

  // Scratch buffer for exception positions within a vector; shared by both readers (int[] in each).
  protected int[] excPositionsBuffer;

  /** Reused holder so readForInfo hands its result to decodePackedValues explicitly. */
  protected final ForInfo forInfo = new ForInfo();

  /**
   * A vector's ForInfo header: the frame-of-reference base and the bit width of the packed values.
   * The base is held as a long for both value types; the float reader narrows it on use.
   */
  protected static final class ForInfo {
    long base;
    int bitWidth;
  }

  @Override
  public void initFromPage(int valuesCount, ByteBufferInputStream stream)
      throws ParquetDecodingException, IOException {
    readPageHeader(stream, valuesCount);
    int minVectorBytes = ALP_INFO_SIZE + forInfoSize();
    readOffsetArray(stream, minVectorBytes);
    int vectorDataSize = sliceVectorData(stream);
    // The trailing entry is filled here rather than in readOffsetArray because the vector data
    // size is only known once the data has been sliced.
    vectorOffsets[numVectors] = offsetArraySize() + vectorDataSize;
    validateVectorOffsets(
        vectorOffsets, minVectorBytes, maxVectorBytes(valuesPerVector, minVectorBytes, exceptionValueSize()));

    ensureDecodedCapacity(valuesPerVector);
    if (excPositionsBuffer == null || excPositionsBuffer.length < valuesPerVector) {
      this.excPositionsBuffer = new int[valuesPerVector];
    }
  }

  /**
   * Reads and validates the fixed 7-byte page header, setting {@link #valuesPerVector} and
   * {@link #totalValueCount}.
   *
   * <p>The reads are relative on purpose. {@link ByteBufferInputStream#slice} does not promise a
   * buffer positioned at 0, so the absolute {@code get(index)} form would read the wrong bytes for
   * a page backed by a larger buffer.
   */
  private void readPageHeader(ByteBufferInputStream stream, int valuesCount) throws IOException {
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

    this.valuesPerVector = 1 << logVectorSize;
    this.totalValueCount = numElements;
    this.pageValueIndex = 0;
    this.currentVectorIndex = BEFORE_FIRST_VECTOR;
  }

  /**
   * Reads the offset array, one entry per vector, after bounding the vector count and checking the
   * first offset. Leaves room for the trailing end-of-data entry, which the caller fills in.
   */
  private void readOffsetArray(ByteBufferInputStream stream, int minVectorBytes) throws IOException {
    // Bound the vector count against the bytes actually present before allocating anything.
    // numElements is only capped by valuesCount, which a forged page can set as high as
    // Integer.MAX_VALUE, so computing this in int would overflow to a negative count, and even
    // without overflow a large count would drive a huge int[] and a huge stream.slice allocation.
    // Every vector costs a 4-byte offset entry plus at least its ALP and FOR headers, which gives
    // a hard ceiling on how many vectors the remaining bytes can possibly describe.
    long remainingBytes = stream.available();
    this.numVectors = Math.toIntExact(((long) totalValueCount + valuesPerVector - 1) / valuesPerVector);
    long minRemainingBytes = (long) numVectors * (Integer.BYTES + minVectorBytes);
    if (minRemainingBytes > remainingBytes) {
      throw new ParquetDecodingException("ALP header element count " + totalValueCount
          + " requires at least " + minRemainingBytes + " bytes but only " + remainingBytes
          + " remain in the page");
    }

    int offsetArraySize = offsetArraySize();
    ByteBuffer offsetBuf = stream.slice(offsetArraySize).order(ByteOrder.LITTLE_ENDIAN);
    if (numVectors == 0) {
      this.vectorOffsets = new int[1];
      return;
    }
    // Offsets are measured from the start of the offset array, so the first vector must begin
    // exactly where the array ends. Check that before allocating and decoding the rest.
    int firstOffset = offsetBuf.getInt();
    if (firstOffset != offsetArraySize) {
      throw new ParquetDecodingException(
          "ALP first vector offset " + firstOffset + " must equal the offset array size " + offsetArraySize);
    }
    this.vectorOffsets = new int[numVectors + 1];
    vectorOffsets[0] = firstOffset;
    for (int v = 1; v < numVectors; v++) {
      vectorOffsets[v] = offsetBuf.getInt();
    }
  }

  /**
   * Slices the remaining page bytes into a view positioned at 0, so the decode path can use
   * absolute {@code get(pos)} reads, and returns their size.
   */
  private int sliceVectorData(ByteBufferInputStream stream) throws IOException {
    int vectorDataSize = (int) stream.available();
    ByteBuffer rawSlice = stream.slice(vectorDataSize);
    this.vectorsData = rawSlice.slice().order(ByteOrder.LITTLE_ENDIAN);
    return vectorDataSize;
  }

  private int offsetArraySize() {
    return numVectors * Integer.BYTES;
  }

  /**
   * Validates the offset array before any vector is decoded. {@code vectorOffsets} holds one entry
   * per vector followed by the end of the vector data, all measured from the start of the offset
   * array. The first entry is checked when the array is read, so this starts at entry 1: each
   * vector, including the last, must span at least its fixed headers and no more than
   * {@code maxVectorBytes}. Without this a forged offset can point into another vector and
   * silently decode the wrong values.
   */
  static void validateVectorOffsets(int[] vectorOffsets, int minVectorBytes, int maxVectorBytes) {
    int numVectors = vectorOffsets.length - 1;
    for (int v = 1; v <= numVectors; v++) {
      long span = (long) vectorOffsets[v] - vectorOffsets[v - 1];
      if (span < minVectorBytes) {
        throw new ParquetDecodingException("ALP vector " + (v - 1) + " at offset " + vectorOffsets[v - 1]
            + " spans " + span + " bytes, fewer than the " + minVectorBytes + " its headers need");
      }
      if (span > maxVectorBytes) {
        throw new ParquetDecodingException("ALP vector " + (v - 1) + " at offset " + vectorOffsets[v - 1]
            + " spans " + span + " bytes, more than the " + maxVectorBytes + " any vector can occupy");
      }
    }
  }

  /**
   * The most bytes one vector can occupy: its fixed headers, every value bit-packed at the full
   * width of the type, and every value also stored as an exception. Vector sizes are multiples of
   * the 8-value packing group, so the packed section needs no padding allowance.
   */
  static int maxVectorBytes(int valuesPerVector, int minVectorBytes, int valueSize) {
    int packed = valuesPerVector * valueSize;
    int exceptions = valuesPerVector * (Short.BYTES + valueSize);
    return minVectorBytes + packed + exceptions;
  }

  /**
   * The position in the vector data just past the last byte vector {@code vectorIndex} may read,
   * taken from the next entry of the offset array; the trailing entry makes this valid for the last
   * vector too.
   */
  int getVectorEndPosition(int vectorIndex) {
    if (vectorIndex < 0 || vectorIndex >= numVectors) {
      throw new IllegalArgumentException(
          "ALP vector index " + vectorIndex + " is out of range for " + numVectors + " vectors");
    }
    return vectorOffsets[vectorIndex + 1] - offsetArraySize();
  }

  // Offsets in the page are relative to the compression body (after header),
  // but vectorsData starts after the offset array, so adjust.
  protected int getVectorDataPosition(int vectorIndex) {
    return vectorOffsets[vectorIndex] - offsetArraySize();
  }

  /** Whether {@code vectorIndex} is the last vector in the page. */
  static boolean isLast(int vectorIndex, int numVectors) {
    return vectorIndex == numVectors - 1;
  }

  /**
   * The number of values held by vector {@code vectorIndex}. Every vector holds
   * {@code valuesPerVector} values except the last, which holds whatever remains and so may be
   * partial. This is a value count, not a byte count.
   */
  static int getVectorSize(int vectorIndex, int valuesPerVector, int totalValueCount, int numVectors) {
    if (isLast(vectorIndex, numVectors)) {
      int lastVectorSize = totalValueCount % valuesPerVector;
      return lastVectorSize == 0 ? valuesPerVector : lastVectorSize;
    }
    return valuesPerVector;
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    // Compare against the remaining count rather than adding to pageValueIndex: a large n would
    // overflow the sum, pass the check, and leave pageValueIndex negative.
    if (n < 0 || n > totalValueCount - pageValueIndex) {
      throw new ParquetDecodingException(String.format(
          "Cannot skip this many elements. Current index: %d. Skip %d. Total count: %d",
          pageValueIndex, n, totalValueCount));
    }
    pageValueIndex += n;
  }

  protected void ensureVectorDecoded() {
    int vectorIndex = pageValueIndex / valuesPerVector;
    if (vectorIndex != currentVectorIndex) {
      decodeVector(vectorIndex);
      currentVectorIndex = vectorIndex;
    }
  }

  /**
   * Decodes one vector, following the division the ALP spec makes: a vector header of AlpInfo and
   * ForInfo, then a data section of PackedValues, ExceptionPositions and ExceptionValues. Runs once
   * per vector, so the virtual hook calls stay off the per-value hot path.
   */
  protected final void decodeVector(int vectorIndex) {
    int vectorSize = getVectorSize(vectorIndex, valuesPerVector, totalValueCount, numVectors);
    int vectorEnd = getVectorEndPosition(vectorIndex);
    int pos = getVectorDataPosition(vectorIndex);

    // Vector header, part one: AlpInfo, which is the same for both value types.
    int exponent = vectorsData.get(pos) & 0xFF;
    int factor = vectorsData.get(pos + 1) & 0xFF;
    int numExceptions = vectorsData.getShort(pos + 2) & 0xFFFF;
    pos += ALP_INFO_SIZE;
    validateAlpInfo(vectorIndex, exponent, factor, numExceptions, vectorSize);

    // Vector header, part two: ForInfo, whose width depends on the value type.
    pos = readForInfo(pos, vectorIndex, forInfo);

    // Data section, part one: PackedValues. Their width comes from this vector's own ForInfo, so
    // confirm they fit before unpacking; otherwise this vector would decode the next one's bytes,
    // or for the last vector read past the end of the page.
    int packedEnd = pos + BytesUtils.paddedByteCountFromBits(vectorSize * forInfo.bitWidth);
    if (packedEnd > vectorEnd) {
      throw new ParquetDecodingException("ALP vector " + vectorIndex + " packed values end at " + packedEnd
          + ", past the end of the vector at " + vectorEnd);
    }
    pos = decodePackedValues(pos, vectorSize, exponent, factor, forInfo);

    // Data section, parts two and three: ExceptionPositions then ExceptionValues.
    if (numExceptions > 0) {
      long exceptionBytes = (long) numExceptions * (Short.BYTES + exceptionValueSize());
      if (pos + exceptionBytes > vectorEnd) {
        throw new ParquetDecodingException(
            "ALP vector " + vectorIndex + " declares " + numExceptions + " exceptions needing "
                + exceptionBytes + " bytes, past the end of the vector at " + vectorEnd);
      }
      applyExceptions(pos, numExceptions, vectorSize);
    }
  }

  /** Validates the AlpInfo fields of a vector header against this type's limits. */
  private void validateAlpInfo(int vectorIndex, int exponent, int factor, int numExceptions, int vectorSize) {
    if (exponent > maxExponent()) {
      throw new ParquetDecodingException("Invalid ALP " + typeName() + " exponent " + exponent + " in vector "
          + vectorIndex + ", max is " + maxExponent());
    }
    if (factor > exponent) {
      throw new ParquetDecodingException("Invalid ALP " + typeName() + " factor " + factor + " > exponent "
          + exponent + " in vector " + vectorIndex);
    }
    if (numExceptions > vectorSize) {
      throw new ParquetDecodingException("Invalid ALP numExceptions " + numExceptions + " > vectorSize "
          + vectorSize + " in vector " + vectorIndex);
    }
  }

  /**
   * Reads {@code numExceptions} little-endian uint16 exception positions into {@code positionsOut}
   * and returns the position just past them. Static so the exception block's two halves can be
   * followed without tracing fields.
   */
  static int readExceptionPositions(ByteBuffer data, int pos, int numExceptions, int vectorSize, int[] positionsOut) {
    for (int e = 0; e < numExceptions; e++) {
      positionsOut[e] = data.getShort(pos) & 0xFFFF;
      if (positionsOut[e] >= vectorSize) {
        throw new ParquetDecodingException(
            "ALP exception position " + positionsOut[e] + " out of bounds for vectorSize " + vectorSize);
      }
      pos += Short.BYTES;
    }
    return pos;
  }

  /** Grows the decode buffers to hold {@code capacity} values, keeping them if they already do. */
  protected abstract void ensureDecodedCapacity(int capacity);

  /** The maximum exponent for this value type (used to validate the header). */
  protected abstract int maxExponent();

  /** The value type name ("float" / "double") used in decoding error messages. */
  protected abstract String typeName();

  /** Size in bytes of this type's frame-of-reference header (base + bit width). */
  protected abstract int forInfoSize();

  /** Size in bytes of one exception value of this type, as stored in the exception block. */
  protected abstract int exceptionValueSize();

  /**
   * Reads this vector's ForInfo header into {@code out} and returns the position just past it.
   */
  protected abstract int readForInfo(int pos, int vectorIndex, ForInfo out);

  /**
   * Decodes the PackedValues of the data section into the type-specific decoded buffer and returns
   * the position just past them.
   */
  protected abstract int decodePackedValues(int pos, int vectorSize, int exponent, int factor, ForInfo forInfo);

  /**
   * Reads the ExceptionPositions and ExceptionValues of the data section and applies them over the
   * decoded values.
   */
  protected abstract void applyExceptions(int pos, int numExceptions, int vectorSize);
}
