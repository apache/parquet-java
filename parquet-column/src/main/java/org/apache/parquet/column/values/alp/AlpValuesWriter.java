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
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * Incremental ALP values writer for float and double columns.
 *
 * <p>Buffers values into fixed-size vectors (default 1024). When a vector is full,
 * it is compressed via {@link AlpCompression} and stored. On {@link #getBytes()},
 * assembles the ALP page: [Header(7B)][Offsets...][Vector0][Vector1]...
 *
 * <p>Uses {@link CapacityByteArrayOutputStream} for encoded vector storage
 * and {@link BytesInput#concat} for zero-copy page assembly, integrating
 * with the Parquet pipeline's memory management.
 *
 * <p>Sampling: the first vector's data is used to create an encoding preset via
 * {@link AlpSampler}. The preset is cached for subsequent vectors.
 */
public abstract class AlpValuesWriter extends ValuesWriter {

  protected final int vectorSize;
  protected int bufferedCount; // values in current partial vector
  protected int totalCount; // total values written
  protected AlpCompression.AlpEncodingPreset preset;
  protected boolean presetReady;

  protected AlpValuesWriter(int vectorSize) {
    this.vectorSize = AlpConstants.validateVectorSize(vectorSize);
  }

  protected AlpValuesWriter() {
    this(DEFAULT_VECTOR_SIZE);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.ALP;
  }

  // ========== FloatAlpValuesWriter ==========

  public static class FloatAlpValuesWriter extends AlpValuesWriter {
    private float[] vectorBuffer;
    private float[] samplerBuffer;
    private int samplerCount;
    private CapacityByteArrayOutputStream encodedVectors;
    private final List<Integer> vectorByteSizes = new ArrayList<>();

    public FloatAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      this(initialCapacity, pageSize, allocator, DEFAULT_VECTOR_SIZE);
    }

    public FloatAlpValuesWriter(
        int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
      super(vectorSize);
      this.vectorBuffer = new float[this.vectorSize];
      this.samplerBuffer = new float[SAMPLER_ROWGROUP_SIZE];
      this.encodedVectors = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
    }

    /** No-arg constructor for tests and benchmarks. */
    public FloatAlpValuesWriter() {
      this(DEFAULT_VECTOR_SIZE);
    }

    public FloatAlpValuesWriter(int vectorSize) {
      super(vectorSize);
      this.vectorBuffer = new float[this.vectorSize];
      this.samplerBuffer = new float[SAMPLER_ROWGROUP_SIZE];
      this.encodedVectors =
          new CapacityByteArrayOutputStream(64, 1024 * 1024, HeapByteBufferAllocator.getInstance());
    }

    @Override
    public void writeFloat(float v) {
      // Collect for sampling if preset not ready
      if (!presetReady && samplerCount < samplerBuffer.length) {
        samplerBuffer[samplerCount++] = v;
      }

      vectorBuffer[bufferedCount++] = v;
      totalCount++;

      if (bufferedCount == vectorSize) {
        ensurePreset();
        flushVector();
      }
    }

    private void ensurePreset() {
      if (!presetReady) {
        AlpSampler.FloatSampler sampler = new AlpSampler.FloatSampler();
        sampler.addSample(samplerBuffer, samplerCount);
        preset = sampler.finalizeSampling();
        presetReady = true;
        samplerBuffer = null; // free sampling buffer
      }
    }

    private void flushVector() {
      AlpCompression.FloatCompressedVector cv =
          AlpCompression.compressFloatVector(vectorBuffer, bufferedCount, preset);
      int size = cv.storedSize();
      byte[] encoded = new byte[size];
      cv.store(encoded, 0);
      encodedVectors.write(encoded, 0, size);
      vectorByteSizes.add(size);
      bufferedCount = 0;
    }

    @Override
    public BytesInput getBytes() {
      // Flush any partial vector
      if (bufferedCount > 0) {
        ensurePreset();
        flushVector();
      }

      if (totalCount == 0) {
        byte[] header = new byte[HEADER_SIZE];
        writeAlpHeader(header, vectorSize, 0);
        return BytesInput.from(header);
      }

      // Build header
      byte[] header = new byte[HEADER_SIZE];
      writeAlpHeader(header, vectorSize, totalCount);

      // Build offset array
      int numVectors = vectorByteSizes.size();
      int offsetsSectionSize = numVectors * OFFSET_SIZE;
      ByteBuffer offsets = ByteBuffer.allocate(offsetsSectionSize).order(ByteOrder.LITTLE_ENDIAN);
      int currentOffset = offsetsSectionSize;
      for (int i = 0; i < numVectors; i++) {
        offsets.putInt(currentOffset);
        currentOffset += vectorByteSizes.get(i);
      }

      return BytesInput.concat(
          BytesInput.from(header), BytesInput.from(offsets.array()), BytesInput.from(encodedVectors));
    }

    @Override
    public long getBufferedSize() {
      long size = HEADER_SIZE + encodedVectors.size();
      size += (long) vectorByteSizes.size() * OFFSET_SIZE;
      size += (long) bufferedCount * Float.BYTES;
      return size;
    }

    @Override
    public long getAllocatedSize() {
      long size = (long) vectorBuffer.length * Float.BYTES;
      if (samplerBuffer != null) {
        size += (long) samplerBuffer.length * Float.BYTES;
      }
      size += encodedVectors.getCapacity();
      return size;
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s ALPFloatWriter: %d values, %d vectors, %d bytes allocated",
          prefix, totalCount, vectorByteSizes.size(), getAllocatedSize());
    }

    @Override
    public void reset() {
      bufferedCount = 0;
      totalCount = 0;
      encodedVectors.reset();
      vectorByteSizes.clear();
      preset = null;
      presetReady = false;
      vectorBuffer = new float[vectorSize];
      samplerBuffer = new float[SAMPLER_ROWGROUP_SIZE];
      samplerCount = 0;
    }

    @Override
    public void close() {
      encodedVectors.close();
    }
  }

  // ========== DoubleAlpValuesWriter ==========

  public static class DoubleAlpValuesWriter extends AlpValuesWriter {
    private double[] vectorBuffer;
    private double[] samplerBuffer;
    private int samplerCount;
    private CapacityByteArrayOutputStream encodedVectors;
    private final List<Integer> vectorByteSizes = new ArrayList<>();

    public DoubleAlpValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
      this(initialCapacity, pageSize, allocator, DEFAULT_VECTOR_SIZE);
    }

    public DoubleAlpValuesWriter(
        int initialCapacity, int pageSize, ByteBufferAllocator allocator, int vectorSize) {
      super(vectorSize);
      this.vectorBuffer = new double[this.vectorSize];
      this.samplerBuffer = new double[SAMPLER_ROWGROUP_SIZE];
      this.encodedVectors = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
    }

    /** No-arg constructor for tests and benchmarks. */
    public DoubleAlpValuesWriter() {
      this(DEFAULT_VECTOR_SIZE);
    }

    public DoubleAlpValuesWriter(int vectorSize) {
      super(vectorSize);
      this.vectorBuffer = new double[this.vectorSize];
      this.samplerBuffer = new double[SAMPLER_ROWGROUP_SIZE];
      this.encodedVectors =
          new CapacityByteArrayOutputStream(64, 1024 * 1024, HeapByteBufferAllocator.getInstance());
    }

    @Override
    public void writeDouble(double v) {
      if (!presetReady && samplerCount < samplerBuffer.length) {
        samplerBuffer[samplerCount++] = v;
      }

      vectorBuffer[bufferedCount++] = v;
      totalCount++;

      if (bufferedCount == vectorSize) {
        ensurePreset();
        flushVector();
      }
    }

    private void ensurePreset() {
      if (!presetReady) {
        AlpSampler.DoubleSampler sampler = new AlpSampler.DoubleSampler();
        sampler.addSample(samplerBuffer, samplerCount);
        preset = sampler.finalizeSampling();
        presetReady = true;
        samplerBuffer = null;
      }
    }

    private void flushVector() {
      AlpCompression.DoubleCompressedVector cv =
          AlpCompression.compressDoubleVector(vectorBuffer, bufferedCount, preset);
      int size = cv.storedSize();
      byte[] encoded = new byte[size];
      cv.store(encoded, 0);
      encodedVectors.write(encoded, 0, size);
      vectorByteSizes.add(size);
      bufferedCount = 0;
    }

    @Override
    public BytesInput getBytes() {
      if (bufferedCount > 0) {
        ensurePreset();
        flushVector();
      }

      if (totalCount == 0) {
        byte[] header = new byte[HEADER_SIZE];
        writeAlpHeader(header, vectorSize, 0);
        return BytesInput.from(header);
      }

      byte[] header = new byte[HEADER_SIZE];
      writeAlpHeader(header, vectorSize, totalCount);

      int numVectors = vectorByteSizes.size();
      int offsetsSectionSize = numVectors * OFFSET_SIZE;
      ByteBuffer offsets = ByteBuffer.allocate(offsetsSectionSize).order(ByteOrder.LITTLE_ENDIAN);
      int currentOffset = offsetsSectionSize;
      for (int i = 0; i < numVectors; i++) {
        offsets.putInt(currentOffset);
        currentOffset += vectorByteSizes.get(i);
      }

      return BytesInput.concat(
          BytesInput.from(header), BytesInput.from(offsets.array()), BytesInput.from(encodedVectors));
    }

    @Override
    public long getBufferedSize() {
      long size = HEADER_SIZE + encodedVectors.size();
      size += (long) vectorByteSizes.size() * OFFSET_SIZE;
      size += (long) bufferedCount * Double.BYTES;
      return size;
    }

    @Override
    public long getAllocatedSize() {
      long size = (long) vectorBuffer.length * Double.BYTES;
      if (samplerBuffer != null) {
        size += (long) samplerBuffer.length * Double.BYTES;
      }
      size += encodedVectors.getCapacity();
      return size;
    }

    @Override
    public String memUsageString(String prefix) {
      return String.format(
          "%s ALPDoubleWriter: %d values, %d vectors, %d bytes allocated",
          prefix, totalCount, vectorByteSizes.size(), getAllocatedSize());
    }

    @Override
    public void reset() {
      bufferedCount = 0;
      totalCount = 0;
      encodedVectors.reset();
      vectorByteSizes.clear();
      preset = null;
      presetReady = false;
      vectorBuffer = new double[vectorSize];
      samplerBuffer = new double[SAMPLER_ROWGROUP_SIZE];
      samplerCount = 0;
    }

    @Override
    public void close() {
      encodedVectors.close();
    }
  }

  // ========== Header helpers ==========

  static void writeAlpHeader(byte[] output, int vectorSize, int numElements) {
    int logVs = Integer.numberOfTrailingZeros(vectorSize);
    output[0] = (byte) COMPRESSION_MODE_ALP;
    output[1] = (byte) INTEGER_ENCODING_FOR;
    output[2] = (byte) logVs;
    writeLittleEndianInt(output, 3, numElements);
  }

  static void writeLittleEndianInt(byte[] output, int pos, int value) {
    output[pos] = (byte) value;
    output[pos + 1] = (byte) (value >> 8);
    output[pos + 2] = (byte) (value >> 16);
    output[pos + 3] = (byte) (value >> 24);
  }
}
