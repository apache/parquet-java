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
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * ALP (Adaptive Lossless floating-Point) values writer.
 *
 * <p>ALP encoding converts floating-point values to integers using decimal scaling,
 * then applies Frame of Reference (FOR) encoding and bit-packing.
 * Values that cannot be losslessly converted are stored as exceptions.
 *
 * <p>Page Layout:
 * <pre>
 * ┌─────────┬────────────────┬────────────────┬─────────────┐
 * │ Header  │ AlpInfo Array  │ ForInfo Array  │ Data Array  │
 * │ 8 bytes │ 4B × N vectors │ 5B/9B × N      │ Variable    │
 * └─────────┴────────────────┴────────────────┴─────────────┘
 * </pre>
 */
public abstract class AlpValuesWriter extends ValuesWriter {

  protected final int initialCapacity;
  protected final int pageSize;
  protected final ByteBufferAllocator allocator;
  protected CapacityByteArrayOutputStream outputStream;

  public AlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
    this.initialCapacity = initialCapacity;
    this.pageSize = pageSize;
    this.allocator = allocator;
    this.outputStream = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.ALP;
  }

  @Override
  public void close() {
    outputStream.close();
  }

  /**
   * Float-specific ALP values writer.
   */
  public static class FloatAlpValuesWriter extends AlpValuesWriter {
    private float[] buffer;
    private int count;

    public FloatAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      super(initialCapacity, pageSize, allocator);
      // Initial buffer size - will grow as needed
      this.buffer = new float[Math.max(ALP_VECTOR_SIZE, initialCapacity / Float.BYTES)];
      this.count = 0;
    }

    @Override
    public void writeFloat(float v) {
      if (count >= buffer.length) {
        // Grow buffer
        float[] newBuffer = new float[buffer.length * 2];
        System.arraycopy(buffer, 0, newBuffer, 0, count);
        buffer = newBuffer;
      }
      buffer[count++] = v;
    }

    @Override
    public long getBufferedSize() {
      // Estimate: each float value contributes roughly 2-4 bytes after compression
      // (actual size depends on data characteristics)
      return count * 3L; // Conservative estimate
    }

    @Override
    public BytesInput getBytes() {
      if (count == 0) {
        return BytesInput.empty();
      }

      outputStream.reset();

      // Calculate number of vectors
      int numVectors = (count + ALP_VECTOR_SIZE - 1) / ALP_VECTOR_SIZE;

      // Prepare metadata arrays
      int[] exponents = new int[numVectors];
      int[] factors = new int[numVectors];
      int[] numExceptions = new int[numVectors];
      int[] frameOfReference = new int[numVectors];
      int[] bitWidths = new int[numVectors];

      // Prepare encoded data arrays
      int[][] encodedValues = new int[numVectors][];
      short[][] exceptionPositions = new short[numVectors][];
      float[][] exceptionValues = new float[numVectors][];

      // Process each vector
      for (int v = 0; v < numVectors; v++) {
        int vectorStart = v * ALP_VECTOR_SIZE;
        int vectorEnd = Math.min(vectorStart + ALP_VECTOR_SIZE, count);
        int vectorLen = vectorEnd - vectorStart;

        // Find best encoding parameters
        AlpEncoderDecoder.EncodingParams params =
            AlpEncoderDecoder.findBestFloatParams(buffer, vectorStart, vectorLen);
        exponents[v] = params.exponent;
        factors[v] = params.factor;
        numExceptions[v] = params.numExceptions;

        // Encode values
        int[] encoded = new int[vectorLen];
        short[] excPositions = new short[params.numExceptions];
        float[] excValues = new float[params.numExceptions];
        int excIdx = 0;
        int placeholder = 0; // Will be set to first non-exception value
        boolean foundNonException = false;

        // First pass: find placeholder
        for (int i = 0; i < vectorLen; i++) {
          float value = buffer[vectorStart + i];
          if (!AlpEncoderDecoder.isFloatException(value, params.exponent, params.factor)) {
            placeholder = AlpEncoderDecoder.encodeFloat(value, params.exponent, params.factor);
            foundNonException = true;
            break;
          }
        }

        // Second pass: encode
        int minValue = Integer.MAX_VALUE;
        for (int i = 0; i < vectorLen; i++) {
          float value = buffer[vectorStart + i];
          if (AlpEncoderDecoder.isFloatException(value, params.exponent, params.factor)) {
            excPositions[excIdx] = (short) i;
            excValues[excIdx] = value;
            excIdx++;
            encoded[i] = placeholder; // Use placeholder for exceptions
          } else {
            encoded[i] = AlpEncoderDecoder.encodeFloat(value, params.exponent, params.factor);
          }
          if (encoded[i] < minValue) {
            minValue = encoded[i];
          }
        }

        // Apply Frame of Reference
        int maxDelta = 0;
        for (int i = 0; i < vectorLen; i++) {
          encoded[i] = encoded[i] - minValue;
          if (encoded[i] > maxDelta) {
            maxDelta = encoded[i];
          }
        }

        frameOfReference[v] = minValue;
        bitWidths[v] = AlpEncoderDecoder.bitWidth(maxDelta);
        encodedValues[v] = encoded;
        exceptionPositions[v] = excPositions;
        exceptionValues[v] = excValues;
      }

      // Write the page
      ByteBuffer headerBuffer = ByteBuffer.allocate(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      headerBuffer.put((byte) ALP_VERSION);
      headerBuffer.put((byte) ALP_COMPRESSION_MODE);
      headerBuffer.put((byte) ALP_INTEGER_ENCODING_FOR);
      headerBuffer.put((byte) ALP_VECTOR_SIZE_LOG);
      headerBuffer.putInt(count);
      outputStream.write(headerBuffer.array(), 0, ALP_HEADER_SIZE);

      // Write AlpInfo array
      ByteBuffer alpInfoBuffer =
          ByteBuffer.allocate(ALP_INFO_SIZE * numVectors).order(ByteOrder.LITTLE_ENDIAN);
      for (int v = 0; v < numVectors; v++) {
        alpInfoBuffer.put((byte) exponents[v]);
        alpInfoBuffer.put((byte) factors[v]);
        alpInfoBuffer.putShort((short) numExceptions[v]);
      }
      outputStream.write(alpInfoBuffer.array(), 0, ALP_INFO_SIZE * numVectors);

      // Write ForInfo array
      ByteBuffer forInfoBuffer =
          ByteBuffer.allocate(FLOAT_FOR_INFO_SIZE * numVectors).order(ByteOrder.LITTLE_ENDIAN);
      for (int v = 0; v < numVectors; v++) {
        forInfoBuffer.putInt(frameOfReference[v]);
        forInfoBuffer.put((byte) bitWidths[v]);
      }
      outputStream.write(forInfoBuffer.array(), 0, FLOAT_FOR_INFO_SIZE * numVectors);

      // Write Data array for each vector
      for (int v = 0; v < numVectors; v++) {
        int vectorStart = v * ALP_VECTOR_SIZE;
        int vectorEnd = Math.min(vectorStart + ALP_VECTOR_SIZE, count);
        int vectorLen = vectorEnd - vectorStart;

        // Write bit-packed values
        if (bitWidths[v] > 0) {
          byte[] packed = packInts(encodedValues[v], vectorLen, bitWidths[v]);
          outputStream.write(packed, 0, packed.length);
        }

        // Write exception positions
        if (numExceptions[v] > 0) {
          ByteBuffer excPosBuffer =
              ByteBuffer.allocate(numExceptions[v] * 2).order(ByteOrder.LITTLE_ENDIAN);
          for (int i = 0; i < numExceptions[v]; i++) {
            excPosBuffer.putShort(exceptionPositions[v][i]);
          }
          outputStream.write(excPosBuffer.array(), 0, numExceptions[v] * 2);

          // Write exception values
          ByteBuffer excValBuffer =
              ByteBuffer.allocate(numExceptions[v] * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
          for (int i = 0; i < numExceptions[v]; i++) {
            excValBuffer.putFloat(exceptionValues[v][i]);
          }
          outputStream.write(excValBuffer.array(), 0, numExceptions[v] * Float.BYTES);
        }
      }

      return BytesInput.from(outputStream);
    }

    /**
     * Pack integers into a byte array using the specified bit width.
     */
    private byte[] packInts(int[] values, int count, int bitWidth) {
      int totalBits = count * bitWidth;
      int totalBytes = (totalBits + 7) / 8;
      byte[] packed = new byte[totalBytes];

      long bitBuffer = 0;
      int bitCount = 0;
      int bytePos = 0;

      for (int i = 0; i < count; i++) {
        bitBuffer |= ((long) values[i] << bitCount);
        bitCount += bitWidth;

        while (bitCount >= 8) {
          packed[bytePos++] = (byte) bitBuffer;
          bitBuffer >>>= 8;
          bitCount -= 8;
        }
      }

      // Write remaining bits
      if (bitCount > 0) {
        packed[bytePos] = (byte) bitBuffer;
      }

      return packed;
    }

    @Override
    public void reset() {
      count = 0;
      outputStream.reset();
    }

    @Override
    public long getAllocatedSize() {
      return buffer.length * Float.BYTES + outputStream.getCapacity();
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s FloatAlpValuesWriter %d values, %d bytes allocated", prefix, count, getAllocatedSize());
    }
  }

  /**
   * Double-specific ALP values writer.
   */
  public static class DoubleAlpValuesWriter extends AlpValuesWriter {
    private double[] buffer;
    private int count;

    public DoubleAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      super(initialCapacity, pageSize, allocator);
      this.buffer = new double[Math.max(ALP_VECTOR_SIZE, initialCapacity / Double.BYTES)];
      this.count = 0;
    }

    @Override
    public void writeDouble(double v) {
      if (count >= buffer.length) {
        double[] newBuffer = new double[buffer.length * 2];
        System.arraycopy(buffer, 0, newBuffer, 0, count);
        buffer = newBuffer;
      }
      buffer[count++] = v;
    }

    @Override
    public long getBufferedSize() {
      return count * 5L; // Conservative estimate
    }

    @Override
    public BytesInput getBytes() {
      if (count == 0) {
        return BytesInput.empty();
      }

      outputStream.reset();

      // Calculate number of vectors
      int numVectors = (count + ALP_VECTOR_SIZE - 1) / ALP_VECTOR_SIZE;

      // Prepare metadata arrays
      int[] exponents = new int[numVectors];
      int[] factors = new int[numVectors];
      int[] numExceptions = new int[numVectors];
      long[] frameOfReference = new long[numVectors];
      int[] bitWidths = new int[numVectors];

      // Prepare encoded data arrays
      long[][] encodedValues = new long[numVectors][];
      short[][] exceptionPositions = new short[numVectors][];
      double[][] exceptionValues = new double[numVectors][];

      // Process each vector
      for (int v = 0; v < numVectors; v++) {
        int vectorStart = v * ALP_VECTOR_SIZE;
        int vectorEnd = Math.min(vectorStart + ALP_VECTOR_SIZE, count);
        int vectorLen = vectorEnd - vectorStart;

        // Find best encoding parameters
        AlpEncoderDecoder.EncodingParams params =
            AlpEncoderDecoder.findBestDoubleParams(buffer, vectorStart, vectorLen);
        exponents[v] = params.exponent;
        factors[v] = params.factor;
        numExceptions[v] = params.numExceptions;

        // Encode values
        long[] encoded = new long[vectorLen];
        short[] excPositions = new short[params.numExceptions];
        double[] excValues = new double[params.numExceptions];
        int excIdx = 0;
        long placeholder = 0;
        boolean foundNonException = false;

        // First pass: find placeholder
        for (int i = 0; i < vectorLen; i++) {
          double value = buffer[vectorStart + i];
          if (!AlpEncoderDecoder.isDoubleException(value, params.exponent, params.factor)) {
            placeholder = AlpEncoderDecoder.encodeDouble(value, params.exponent, params.factor);
            foundNonException = true;
            break;
          }
        }

        // Second pass: encode
        long minValue = Long.MAX_VALUE;
        for (int i = 0; i < vectorLen; i++) {
          double value = buffer[vectorStart + i];
          if (AlpEncoderDecoder.isDoubleException(value, params.exponent, params.factor)) {
            excPositions[excIdx] = (short) i;
            excValues[excIdx] = value;
            excIdx++;
            encoded[i] = placeholder;
          } else {
            encoded[i] = AlpEncoderDecoder.encodeDouble(value, params.exponent, params.factor);
          }
          if (encoded[i] < minValue) {
            minValue = encoded[i];
          }
        }

        // Apply Frame of Reference
        // Use unsigned comparison because the delta range may exceed Long.MAX_VALUE
        long maxDelta = 0;
        for (int i = 0; i < vectorLen; i++) {
          encoded[i] = encoded[i] - minValue;
          // Use unsigned comparison to handle overflow when range > Long.MAX_VALUE
          if (Long.compareUnsigned(encoded[i], maxDelta) > 0) {
            maxDelta = encoded[i];
          }
        }

        frameOfReference[v] = minValue;
        // bitWidth works correctly for unsigned values (uses numberOfLeadingZeros)
        bitWidths[v] = AlpEncoderDecoder.bitWidth(maxDelta);
        encodedValues[v] = encoded;
        exceptionPositions[v] = excPositions;
        exceptionValues[v] = excValues;
      }

      // Write the page
      ByteBuffer headerBuffer = ByteBuffer.allocate(ALP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      headerBuffer.put((byte) ALP_VERSION);
      headerBuffer.put((byte) ALP_COMPRESSION_MODE);
      headerBuffer.put((byte) ALP_INTEGER_ENCODING_FOR);
      headerBuffer.put((byte) ALP_VECTOR_SIZE_LOG);
      headerBuffer.putInt(count);
      outputStream.write(headerBuffer.array(), 0, ALP_HEADER_SIZE);

      // Write AlpInfo array
      ByteBuffer alpInfoBuffer =
          ByteBuffer.allocate(ALP_INFO_SIZE * numVectors).order(ByteOrder.LITTLE_ENDIAN);
      for (int v = 0; v < numVectors; v++) {
        alpInfoBuffer.put((byte) exponents[v]);
        alpInfoBuffer.put((byte) factors[v]);
        alpInfoBuffer.putShort((short) numExceptions[v]);
      }
      outputStream.write(alpInfoBuffer.array(), 0, ALP_INFO_SIZE * numVectors);

      // Write ForInfo array (9 bytes per vector for double)
      ByteBuffer forInfoBuffer =
          ByteBuffer.allocate(DOUBLE_FOR_INFO_SIZE * numVectors).order(ByteOrder.LITTLE_ENDIAN);
      for (int v = 0; v < numVectors; v++) {
        forInfoBuffer.putLong(frameOfReference[v]);
        forInfoBuffer.put((byte) bitWidths[v]);
      }
      outputStream.write(forInfoBuffer.array(), 0, DOUBLE_FOR_INFO_SIZE * numVectors);

      // Write Data array for each vector
      for (int v = 0; v < numVectors; v++) {
        int vectorStart = v * ALP_VECTOR_SIZE;
        int vectorEnd = Math.min(vectorStart + ALP_VECTOR_SIZE, count);
        int vectorLen = vectorEnd - vectorStart;

        // Write bit-packed values
        if (bitWidths[v] > 0) {
          byte[] packed = packLongs(encodedValues[v], vectorLen, bitWidths[v]);
          outputStream.write(packed, 0, packed.length);
        }

        // Write exception positions
        if (numExceptions[v] > 0) {
          ByteBuffer excPosBuffer =
              ByteBuffer.allocate(numExceptions[v] * 2).order(ByteOrder.LITTLE_ENDIAN);
          for (int i = 0; i < numExceptions[v]; i++) {
            excPosBuffer.putShort(exceptionPositions[v][i]);
          }
          outputStream.write(excPosBuffer.array(), 0, numExceptions[v] * 2);

          // Write exception values
          ByteBuffer excValBuffer =
              ByteBuffer.allocate(numExceptions[v] * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
          for (int i = 0; i < numExceptions[v]; i++) {
            excValBuffer.putDouble(exceptionValues[v][i]);
          }
          outputStream.write(excValBuffer.array(), 0, numExceptions[v] * Double.BYTES);
        }
      }

      return BytesInput.from(outputStream);
    }

    /**
     * Pack longs into a byte array using the specified bit width.
     */
    private byte[] packLongs(long[] values, int count, int bitWidth) {
      int totalBits = count * bitWidth;
      int totalBytes = (totalBits + 7) / 8;
      byte[] packed = new byte[totalBytes];

      int bitPos = 0;
      for (int i = 0; i < count; i++) {
        long value = values[i];
        int bitsToWrite = bitWidth;
        while (bitsToWrite > 0) {
          int byteIdx = bitPos / 8;
          int bitIdx = bitPos % 8;
          int bitsAvailable = 8 - bitIdx;
          int bitsThisRound = Math.min(bitsAvailable, bitsToWrite);
          int mask = (1 << bitsThisRound) - 1;
          packed[byteIdx] |= (byte) ((value & mask) << bitIdx);
          value >>>= bitsThisRound;
          bitPos += bitsThisRound;
          bitsToWrite -= bitsThisRound;
        }
      }

      return packed;
    }

    @Override
    public void reset() {
      count = 0;
      outputStream.reset();
    }

    @Override
    public long getAllocatedSize() {
      return buffer.length * Double.BYTES + outputStream.getCapacity();
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s DoubleAlpValuesWriter %d values, %d bytes allocated", prefix, count, getAllocatedSize());
    }
  }
}
