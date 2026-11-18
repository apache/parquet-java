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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that the content serialized by {@link CapacityByteArrayOutputStream} always matches its {@link
 * CapacityByteArrayOutputStream#size()}, which is used for length prefixes and page headers (GH-3712).
 */
public class TestCapacityByteArrayOutputStream {

  private TrackingByteBufferAllocator allocator;

  static Stream<ByteBufferAllocator> allocators() {
    return Stream.of(new HeapByteBufferAllocator(), new DirectByteBufferAllocator());
  }

  @AfterEach
  public void closeAllocator() {
    if (allocator != null) {
      allocator.close();
    }
  }

  private CapacityByteArrayOutputStream newStream(ByteBufferAllocator baseAllocator, int initialSlabSize) {
    allocator = TrackingByteBufferAllocator.wrap(baseAllocator);
    return new CapacityByteArrayOutputStream(initialSlabSize, 1024 * 1024, allocator);
  }

  private static byte[] writeTo(CapacityByteArrayOutputStream stream) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    stream.writeTo(out);
    return out.toByteArray();
  }

  private static byte[] writeInto(CapacityByteArrayOutputStream stream) {
    ByteBuffer buffer = ByteBuffer.allocate((int) stream.size());
    stream.writeInto(buffer);
    assertThat(buffer.hasRemaining()).isFalse();
    return buffer.array();
  }

  /**
   * writeInt and writeLong add a new slab when the current one has fewer bytes left than they need, so a slab may be
   * followed by another one before it is full. setByte, as used by the RLE encoder to patch a bit-packed run header,
   * must address the written bytes, not the capacity of the slabs.
   */
  @ParameterizedTest
  @MethodSource("allocators")
  public void testSetByteAfterPartiallyFilledSlab(ByteBufferAllocator baseAllocator) throws IOException {
    try (CapacityByteArrayOutputStream stream = newStream(baseAllocator, 8)) {
      stream.write(new byte[] {0, 1, 2, 3, 4, 5}, 0, 6);
      // Only 2 bytes left in the first slab: the int goes to a new slab
      stream.writeInt(0x09080706);
      stream.write(0);
      long headerIndex = stream.getCurrentIndex();
      stream.write(11);
      stream.setByte(headerIndex, (byte) 10);
      stream.writeLong(0x1312111009080706L);

      byte[] expected = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 6, 7, 8, 9, 0x10, 0x11, 0x12, 0x13};
      assertThat(stream.size()).isEqualTo(expected.length);
      assertThat(stream.getSlabCount()).isGreaterThan(1);
      assertThat(writeTo(stream)).containsExactly(expected);
      assertThat(writeInto(stream)).containsExactly(expected);
    }
  }

  /** Serializing the content must not change the state of the slabs, so that it can be serialized again. */
  @ParameterizedTest
  @MethodSource("allocators")
  public void testSerializingDoesNotChangeTheContent(ByteBufferAllocator baseAllocator) throws IOException {
    try (CapacityByteArrayOutputStream stream = newStream(baseAllocator, 4)) {
      ByteArrayOutputStream expected = new ByteArrayOutputStream();
      for (int i = 0; i < 100; i++) {
        stream.write(i);
        expected.write(i);
        if (i % 7 == 0) {
          assertThat(writeInto(stream)).containsExactly(expected.toByteArray());
        }
        if (i % 11 == 0) {
          assertThat(writeTo(stream)).containsExactly(expected.toByteArray());
        }
      }
      assertThat(writeTo(stream)).containsExactly(expected.toByteArray());
      assertThat(writeInto(stream)).containsExactly(expected.toByteArray());
      assertThat(BytesInput.from(stream).toByteArray()).containsExactly(expected.toByteArray());
    }
  }

  /**
   * Reproduces the state of the corrupt pages of GH-3712: a full slab holds fewer bytes than were written to it,
   * while size() is correct. The content must not be serialized, because it doesn't match the length prefixes and
   * the page header computed from size().
   */
  @ParameterizedTest
  @MethodSource("allocators")
  public void testSerializingFailsIfTheContentDoesNotMatchTheSize(ByteBufferAllocator baseAllocator)
      throws IOException {
    List<ByteBuffer> slabs = new ArrayList<>();
    ByteBufferAllocator recordingAllocator = new ByteBufferAllocator() {
      @Override
      public ByteBuffer allocate(int size) {
        ByteBuffer slab = baseAllocator.allocate(size);
        slabs.add(slab);
        return slab;
      }

      @Override
      public void release(ByteBuffer b) {
        baseAllocator.release(b);
      }

      @Override
      public boolean isDirect() {
        return baseAllocator.isDirect();
      }
    };
    try (CapacityByteArrayOutputStream stream = newStream(recordingAllocator, 16)) {
      byte[] data = new byte[10];
      for (int i = 0; i < 10; i++) {
        stream.write(data, 0, data.length);
      }
      assertThat(slabs).hasSizeGreaterThan(2);
      ByteBuffer fullSlab = slabs.get(1);
      assertThat(fullSlab.hasRemaining()).isFalse();
      fullSlab.position(fullSlab.position() - 2);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      assertThatThrownBy(() -> stream.writeTo(out))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageStartingWith("The slabs of this stream hold 98 bytes, but 100 bytes were written to it");
      assertThat(out.size()).isZero();
      assertThatThrownBy(() -> stream.writeInto(ByteBuffer.allocate(100)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageStartingWith("The slabs of this stream hold 98 bytes, but 100 bytes were written to it");
      assertThatThrownBy(() -> BytesInput.from(stream).toByteArray())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageStartingWith("The slabs of this stream hold 98 bytes, but 100 bytes were written to it");
    }
  }
}
