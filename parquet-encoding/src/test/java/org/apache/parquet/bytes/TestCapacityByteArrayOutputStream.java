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
package org.apache.parquet.bytes;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCapacityByteArrayOutputStream {

  private TrackingByteBufferAllocator allocator;

  @BeforeEach
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @AfterEach
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void testWrite() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      final int expectedSize = 54;
      for (int i = 0; i < expectedSize; i++) {
        capacityByteArrayOutputStream.write(i);
        assertThat(capacityByteArrayOutputStream.size()).isEqualTo(i + 1);
      }
      validate(capacityByteArrayOutputStream, expectedSize);
    }
  }

  @Test
  public void testWriteArray() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      int v = 23;
      writeArraysOf3(capacityByteArrayOutputStream, v);
      validate(capacityByteArrayOutputStream, v * 3);
    }
  }

  @Test
  public void testWriteArrayExpand() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(2)) {
      assertThat(capacityByteArrayOutputStream.getCapacity()).isZero();

      byte[] toWrite = bytes(1, 2, 3, 4);
      int toWriteOffset = 0;
      int writeLength = 2;
      // write 2 bytes array
      capacityByteArrayOutputStream.write(toWrite, toWriteOffset, writeLength);
      toWriteOffset += writeLength;
      assertThat(capacityByteArrayOutputStream.size()).isEqualTo(2);
      assertThat(capacityByteArrayOutputStream.getCapacity()).isEqualTo(2);

      // write 1 byte array, expand capacity to 4
      writeLength = 1;
      capacityByteArrayOutputStream.write(toWrite, toWriteOffset, writeLength);
      toWriteOffset += writeLength;
      assertThat(capacityByteArrayOutputStream.size()).isEqualTo(3);
      assertThat(capacityByteArrayOutputStream.getCapacity()).isEqualTo(4);

      // write 1 byte array, not expand
      capacityByteArrayOutputStream.write(toWrite, toWriteOffset, writeLength);
      assertThat(capacityByteArrayOutputStream.size()).isEqualTo(4);
      assertThat(capacityByteArrayOutputStream.getCapacity()).isEqualTo(4);
      assertThat(BytesInput.from(capacityByteArrayOutputStream).toByteArray())
          .containsExactly(toWrite);
    }
  }

  @Test
  public void testWriteArrayAndInt() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      for (int i = 0; i < 23; i++) {
        capacityByteArrayOutputStream.write(bytes(i * 3, i * 3 + 1));
        capacityByteArrayOutputStream.write(i * 3 + 2);
        assertThat(capacityByteArrayOutputStream.size()).isEqualTo((i + 1) * 3);
      }
      validate(capacityByteArrayOutputStream, 23 * 3);
    }
  }

  protected CapacityByteArrayOutputStream newCapacityBAOS(int initialSize) {
    return new CapacityByteArrayOutputStream(initialSize, 1000000, allocator);
  }

  @Test
  public void testReset() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      for (int i = 0; i < 54; i++) {
        capacityByteArrayOutputStream.write(i);
        assertThat(capacityByteArrayOutputStream.size()).isEqualTo(i + 1);
      }
      capacityByteArrayOutputStream.reset();
      for (int i = 0; i < 54; i++) {
        capacityByteArrayOutputStream.write(54 + i);
        assertThat(capacityByteArrayOutputStream.size()).isEqualTo(i + 1);
      }
      assertThat(BytesInput.from(capacityByteArrayOutputStream).toByteArray())
          .isEqualTo(byteRange(54, 54));
    }
  }

  @Test
  public void testWriteArrayBiggerThanSlab() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      int v = 23;
      writeArraysOf3(capacityByteArrayOutputStream, v);
      int n = v * 3;
      byte[] toWrite = byteRange(n, 21);
      capacityByteArrayOutputStream.write(toWrite);
      n = n + toWrite.length;
      assertThat(capacityByteArrayOutputStream.size()).isEqualTo(n);
      validate(capacityByteArrayOutputStream, n);
      capacityByteArrayOutputStream.reset();
      // check it works after reset too
      capacityByteArrayOutputStream.write(toWrite);
      assertThat(capacityByteArrayOutputStream.size()).isEqualTo(toWrite.length);
      assertThat(BytesInput.from(capacityByteArrayOutputStream).toByteArray())
          .isEqualTo(toWrite);
    }
  }

  @Test
  public void testWriteArrayManySlabs() throws Throwable {
    try (CapacityByteArrayOutputStream capacityByteArrayOutputStream = newCapacityBAOS(10)) {
      int it = 500;
      int v = 23;
      for (int j = 0; j < it; j++) {
        for (int i = 0; i < v; i++) {
          capacityByteArrayOutputStream.write(bytes(i * 3, i * 3 + 1, i * 3 + 2));
          assertThat(capacityByteArrayOutputStream.size()).isEqualTo((i + 1) * 3 + v * 3 * j);
        }
      }
      byte[] byteArray = BytesInput.from(capacityByteArrayOutputStream).toByteArray();
      assertThat(byteArray).hasSize(v * 3 * it);
      assertThat(byteArray).isEqualTo(repeatedByteRange(0, v * 3, v * 3 * it));
      // verifying we have not created 500 * 23 / 10 slabs
      assertThat(capacityByteArrayOutputStream.getSlabCount())
          .as("slab count: " + capacityByteArrayOutputStream.getSlabCount())
          .isLessThanOrEqualTo(20);
      capacityByteArrayOutputStream.reset();
      writeArraysOf3(capacityByteArrayOutputStream, v);
      validate(capacityByteArrayOutputStream, v * 3);
      // verifying we use less slabs now
      assertThat(capacityByteArrayOutputStream.getSlabCount())
          .as("slab count: " + capacityByteArrayOutputStream.getSlabCount())
          .isLessThanOrEqualTo(2);
    }
  }

  @Test
  public void testReplaceByte() throws Throwable {
    // test replace the first value
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {
      cbaos.write(10);
      assertThat(cbaos.getCurrentIndex()).isZero();
      cbaos.setByte(0, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertThat(baos.toByteArray()).isEqualTo(bytes(7));
    }

    // test replace value in the first slab
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {
      cbaos.write(10);
      cbaos.write(13);
      cbaos.write(15);
      cbaos.write(17);
      assertThat(cbaos.getCurrentIndex()).isEqualTo(3);
      cbaos.write(19);
      cbaos.setByte(3, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertThat(baos.toByteArray()).isEqualTo(bytes(10, 13, 15, 7, 19));
    }

    // test replace in *not* the first slab
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {
      writeRange(cbaos, 100, 12);
      assertThat(cbaos.getCurrentIndex()).isEqualTo(11);

      cbaos.setByte(6, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertThat(baos.toByteArray()).isEqualTo(bytes(100, 101, 102, 103, 104, 105, 7, 107, 108, 109, 110, 111));
    }

    // test replace last value of a slab
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {
      writeRange(cbaos, 100, 12);
      assertThat(cbaos.getCurrentIndex()).isEqualTo(11);

      cbaos.setByte(9, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertThat(baos.toByteArray()).isEqualTo(bytes(100, 101, 102, 103, 104, 105, 106, 107, 108, 7, 110, 111));
    }

    // test replace last value
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(5)) {
      writeRange(cbaos, 100, 12);
      assertThat(cbaos.getCurrentIndex()).isEqualTo(11);

      cbaos.setByte(11, (byte) 7);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cbaos.writeTo(baos);
      assertThat(baos.toByteArray()).isEqualTo(bytes(100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 7));
    }
  }

  private void writeArraysOf3(CapacityByteArrayOutputStream capacityByteArrayOutputStream, int n) throws IOException {
    for (int i = 0; i < n; i++) {
      capacityByteArrayOutputStream.write(bytes(i * 3, i * 3 + 1, i * 3 + 2));
      assertThat(capacityByteArrayOutputStream.size()).isEqualTo((i + 1) * 3);
    }
  }

  private void validate(CapacityByteArrayOutputStream capacityByteArrayOutputStream, final int expectedSize)
      throws IOException {
    assertThat(BytesInput.from(capacityByteArrayOutputStream).toByteArray()).isEqualTo(byteRange(0, expectedSize));
  }

  private static void writeRange(CapacityByteArrayOutputStream stream, int start, int count) {
    for (int i = 0; i < count; i++) {
      stream.write(start + i);
    }
  }

  // ---- Scalar write methods (writeInt, writeLong) ----

  /**
   * Reads a little-endian int from the byte array at position {@code pos}.
   */
  private static int readIntLE(byte[] b, int pos) {
    return (b[pos] & 0xFF) | ((b[pos + 1] & 0xFF) << 8) | ((b[pos + 2] & 0xFF) << 16) | ((b[pos + 3] & 0xFF) << 24);
  }

  /**
   * Reads a little-endian long from the byte array at position {@code pos}.
   */
  private static long readLongLE(byte[] b, int pos) {
    return (b[pos] & 0xFFL)
        | ((b[pos + 1] & 0xFFL) << 8)
        | ((b[pos + 2] & 0xFFL) << 16)
        | ((b[pos + 3] & 0xFFL) << 24)
        | ((b[pos + 4] & 0xFFL) << 32)
        | ((b[pos + 5] & 0xFFL) << 40)
        | ((b[pos + 6] & 0xFFL) << 48)
        | ((b[pos + 7] & 0xFFL) << 56);
  }

  @Test
  public void testWriteInt() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      int[] values = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 42};
      for (int v : values) {
        cbaos.writeInt(v);
      }
      assertThat(cbaos.size()).isEqualTo(values.length * 4);

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      for (int i = 0; i < values.length; i++) {
        assertThat(readIntLE(bytes, i * 4)).as("value at index " + i).isEqualTo(values[i]);
      }
    }
  }

  @Test
  public void testWriteLong() throws Throwable {
    try (CapacityByteArrayOutputStream cbaos = newCapacityBAOS(10)) {
      long[] values = {0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 123456789L};
      for (long v : values) {
        cbaos.writeLong(v);
      }
      assertThat(cbaos.size()).isEqualTo(values.length * 8);

      byte[] bytes = BytesInput.from(cbaos).toByteArray();
      for (int i = 0; i < values.length; i++) {
        assertThat(readLongLE(bytes, i * 8)).as("value at index " + i).isEqualTo(values[i]);
      }
    }
  }

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }

  private static byte[] byteRange(int start, int length) {
    return bytes(range(start, start + length));
  }

  private static byte[] repeatedByteRange(int start, int period, int totalLength) {
    int[] values = new int[totalLength];
    for (int i = 0; i < totalLength; i++) {
      values[i] = start + (i % period);
    }
    return bytes(values);
  }

  private static int[] range(int startInclusive, int endExclusive) {
    int[] values = new int[endExclusive - startInclusive];
    for (int i = 0; i < values.length; i++) {
      values[i] = startInclusive + i;
    }
    return values;
  }
}
