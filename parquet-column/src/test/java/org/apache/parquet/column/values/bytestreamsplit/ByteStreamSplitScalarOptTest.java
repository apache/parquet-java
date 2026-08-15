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
package org.apache.parquet.column.values.bytestreamsplit;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

/**
 * Tests for the BYTE_STREAM_SPLIT scalar performance optimizations:
 * internal scatter-buffer batching, decodeData transpose specializations,
 * direct ByteBuffer decode path, getBufferedSize accuracy, and close/reset
 * edge cases.
 */
public class ByteStreamSplitScalarOptTest {

  private static final int BATCH_SIZE = 64; // matches ByteStreamSplitValuesWriter.BATCH_SIZE

  // ---------------------------------------------------------------------------
  // decodeData transpose specializations: element sizes 2, 12, 16
  // (sizes 4 and 8 are already covered by int/float and long/double tests)
  // ---------------------------------------------------------------------------

  @Test
  public void testFlbaTransposeSize2() throws Exception {
    flbaRoundTrip(2, 512);
  }

  @Test
  public void testFlbaTransposeSize12() throws Exception {
    flbaRoundTrip(12, 256);
  }

  @Test
  public void testFlbaTransposeSize16() throws Exception {
    flbaRoundTrip(16, 256);
  }

  /** Generic FLBA round-trip used to exercise specific element-size transpose paths. */
  private void flbaRoundTrip(int typeLength, int numElements) throws Exception {
    Random rand = new Random(42);
    Binary[] values = new Binary[numElements];
    for (int i = 0; i < numElements; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      values[i] = Binary.fromConstantByteArray(bytes);
    }

    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength,
            numElements * typeLength,
            numElements * typeLength,
            new DirectByteBufferAllocator());
    // Use scalar writes to test the writer's scatter path independently from batch writes.
    for (Binary v : values) {
      writer.writeBytes(v);
    }
    BytesInput input = writer.getBytes();
    assertEquals(numElements * typeLength, input.size());

    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(typeLength);
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    // Scalar read to verify each value
    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readBytes());
    }

    writer.reset();
    writer.close();
  }

  /** Also test the generic fallback path with an odd element size (e.g. 5, 7). */
  @Test
  public void testFlbaTransposeGenericFallback() throws Exception {
    flbaRoundTrip(5, 256);
    flbaRoundTrip(7, 256);
  }

  // ---------------------------------------------------------------------------
  // BATCH_SIZE boundary crossing: tests that internal batch flush works correctly
  // when writing exactly BATCH_SIZE, BATCH_SIZE+1, and multi-BATCH_SIZE counts
  // ---------------------------------------------------------------------------

  @Test
  public void testIntegerWriteExactBatchSize() throws Exception {
    intScalarRoundTrip(BATCH_SIZE);
  }

  @Test
  public void testIntegerWriteBatchSizePlusOne() throws Exception {
    intScalarRoundTrip(BATCH_SIZE + 1);
  }

  @Test
  public void testIntegerWriteMultipleBatches() throws Exception {
    intScalarRoundTrip(BATCH_SIZE * 3 + 17);
  }

  @Test
  public void testLongWriteExactBatchSize() throws Exception {
    longScalarRoundTrip(BATCH_SIZE);
  }

  @Test
  public void testLongWriteBatchSizePlusOne() throws Exception {
    longScalarRoundTrip(BATCH_SIZE + 1);
  }

  @Test
  public void testLongWriteMultipleBatches() throws Exception {
    longScalarRoundTrip(BATCH_SIZE * 3 + 17);
  }

  private void intScalarRoundTrip(int numElements) throws Exception {
    Random rand = new Random(42);
    int[] values = rand.ints(numElements).toArray();

    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    for (int v : values) {
      writer.writeInteger(v);
    }
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readInteger());
    }

    writer.reset();
    writer.close();
  }

  private void longScalarRoundTrip(int numElements) throws Exception {
    Random rand = new Random(42);
    long[] values = rand.longs(numElements).toArray();

    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    for (long v : values) {
      writer.writeLong(v);
    }
    BytesInput input = writer.getBytes();

    ByteStreamSplitValuesReaderForLong reader = new ByteStreamSplitValuesReaderForLong();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readLong());
    }

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // getBufferedSize accounts for unflushed batch
  // ---------------------------------------------------------------------------

  @Test
  public void testGetBufferedSizeWithPartialBatch() throws Exception {
    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            256, 256, new DirectByteBufferAllocator());

    // Write fewer than BATCH_SIZE values -- they sit in the internal batch buffer
    for (int i = 0; i < 10; i++) {
      writer.writeInteger(i);
    }
    assertEquals(10 * 4, writer.getBufferedSize());

    // Write more to cross a batch boundary
    for (int i = 0; i < BATCH_SIZE; i++) {
      writer.writeInteger(i);
    }
    assertEquals((10 + BATCH_SIZE) * 4, writer.getBufferedSize());

    writer.reset();
    assertEquals(0, writer.getBufferedSize());
    writer.close();
  }

  @Test
  public void testGetBufferedSizeWithPartialLongBatch() throws Exception {
    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            256, 256, new DirectByteBufferAllocator());

    for (int i = 0; i < 10; i++) {
      writer.writeLong(i);
    }
    assertEquals(10 * 8, writer.getBufferedSize());

    writer.reset();
    assertEquals(0, writer.getBufferedSize());
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // decodeData direct ByteBuffer path (no backing array)
  // ---------------------------------------------------------------------------

  @Test
  public void testDecodeFromDirectByteBuffer() throws Exception {
    Random rand = new Random(42);
    final int numElements = 256;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; i++) {
      values[i] = rand.nextFloat();
    }

    // Encode using standard writer
    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
            numElements * 4, numElements * 4, new DirectByteBufferAllocator());
    for (float v : values) {
      writer.writeFloat(v);
    }
    byte[] encoded = writer.getBytes().toByteArray();

    // Copy into a direct ByteBuffer (no backing array) to exercise the else branch in decodeData
    ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length);
    direct.put(encoded);
    direct.flip();

    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(direct));

    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readFloat(), 0.0f);
    }

    writer.reset();
    writer.close();
  }

  @Test
  public void testDecodeFromDirectByteBufferLong() throws Exception {
    Random rand = new Random(42);
    final int numElements = 256;
    long[] values = rand.longs(numElements).toArray();

    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    for (long v : values) {
      writer.writeLong(v);
    }
    byte[] encoded = writer.getBytes().toByteArray();

    ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length);
    direct.put(encoded);
    direct.flip();

    ByteStreamSplitValuesReaderForLong reader = new ByteStreamSplitValuesReaderForLong();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(direct));

    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readLong());
    }

    writer.reset();
    writer.close();
  }

  @Test
  public void testDecodeFromDirectByteBufferFlba() throws Exception {
    Random rand = new Random(42);
    final int numElements = 256;
    final int typeLength = 12;
    Binary[] values = new Binary[numElements];
    for (int i = 0; i < numElements; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      values[i] = Binary.fromConstantByteArray(bytes);
    }

    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength,
            numElements * typeLength,
            numElements * typeLength,
            new DirectByteBufferAllocator());
    for (Binary v : values) {
      writer.writeBytes(v);
    }
    byte[] encoded = writer.getBytes().toByteArray();

    ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length);
    direct.put(encoded);
    direct.flip();

    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(typeLength);
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(direct));

    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readBytes());
    }

    writer.reset();
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // FLBA getBufferedSize with partial batch
  // ---------------------------------------------------------------------------

  @Test
  public void testFlbaGetBufferedSizeWithPartialBatch() throws Exception {
    final int typeLength = 12;
    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength, 256, 256, new DirectByteBufferAllocator());

    Random rand = new Random(42);
    // Write fewer than BATCH_SIZE values -- they sit in batchBufs
    for (int i = 0; i < 10; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      writer.writeBytes(Binary.fromConstantByteArray(bytes));
    }
    assertEquals(10 * typeLength, writer.getBufferedSize());

    // Write more to cross a batch boundary
    for (int i = 0; i < BATCH_SIZE; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      writer.writeBytes(Binary.fromConstantByteArray(bytes));
    }
    assertEquals((10 + BATCH_SIZE) * typeLength, writer.getBufferedSize());

    writer.reset();
    assertEquals(0, writer.getBufferedSize());
    writer.close();
  }

  // ---------------------------------------------------------------------------
  // close() with a pending (unflushed) batch resets batch state
  // ---------------------------------------------------------------------------

  @Test
  public void testIntCloseWithPendingBatch() throws Exception {
    ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
            256, 256, new DirectByteBufferAllocator());
    // Write fewer than BATCH_SIZE values so the batch stays unflushed
    for (int i = 0; i < 10; i++) {
      writer.writeInteger(i);
    }
    assertEquals(10 * 4, writer.getBufferedSize());
    writer.close();
    assertEquals(0, writer.getBufferedSize());
  }

  @Test
  public void testLongCloseWithPendingBatch() throws Exception {
    ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
            256, 256, new DirectByteBufferAllocator());
    for (int i = 0; i < 10; i++) {
      writer.writeLong(i);
    }
    assertEquals(10 * 8, writer.getBufferedSize());
    writer.close();
    assertEquals(0, writer.getBufferedSize());
  }

  @Test
  public void testFlbaCloseWithPendingBatch() throws Exception {
    final int typeLength = 12;
    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength, 256, 256, new DirectByteBufferAllocator());
    Random rand = new Random(42);
    for (int i = 0; i < 10; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      writer.writeBytes(Binary.fromConstantByteArray(bytes));
    }
    assertEquals(10 * typeLength, writer.getBufferedSize());
    writer.close();
    assertEquals(0, writer.getBufferedSize());
  }

  // ---------------------------------------------------------------------------
  // Direct ByteBuffer decode for 2-byte and 16-byte element sizes
  // ---------------------------------------------------------------------------

  @Test
  public void testDecodeFromDirectByteBufferFlba2() throws Exception {
    directByteBufferFlbaRoundTrip(2, 256);
  }

  @Test
  public void testDecodeFromDirectByteBufferFlba16() throws Exception {
    directByteBufferFlbaRoundTrip(16, 256);
  }

  private void directByteBufferFlbaRoundTrip(int typeLength, int numElements) throws Exception {
    Random rand = new Random(42);
    Binary[] values = new Binary[numElements];
    for (int i = 0; i < numElements; i++) {
      byte[] bytes = new byte[typeLength];
      rand.nextBytes(bytes);
      values[i] = Binary.fromConstantByteArray(bytes);
    }

    ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
            typeLength,
            numElements * typeLength,
            numElements * typeLength,
            new DirectByteBufferAllocator());
    for (Binary v : values) {
      writer.writeBytes(v);
    }
    byte[] encoded = writer.getBytes().toByteArray();

    // Use a direct ByteBuffer (no backing array) to exercise the else branch in decodeData
    ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length);
    direct.put(encoded);
    direct.flip();

    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(typeLength);
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(direct));

    for (int i = 0; i < numElements; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readBytes());
    }

    writer.reset();
    writer.close();
  }
}
