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

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.parquet.util.AutoCloseables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

/**
 * Unit tests for the {@link BytesInput} class and its descendants.
 */
@RunWith(Parameterized.class)
public class TestBytesInput {

  private static final Random RANDOM = new Random(2024_02_20_16_28L);
  private TrackingByteBufferAllocator allocator;
  private final ByteBufferAllocator innerAllocator;

  @Parameters(name = "{0}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        new HeapByteBufferAllocator() {
          @Override
          public String toString() {
            return "heap-allocator";
          }
        }
      },
      {
        new DirectByteBufferAllocator() {
          @Override
          public String toString() {
            return "direct-allocator";
          }
        }
      }
    };
  }

  public TestBytesInput(ByteBufferAllocator innerAllocator) {
    this.innerAllocator = innerAllocator;
  }

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(innerAllocator);
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void testFromSingleByteBuffer() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    Supplier<BytesInput> factory = () -> BytesInput.from(toByteBuffer(data));

    validate(data, factory);

    validateToByteBufferIsInternal(factory);
  }

  @Test
  public void testFromMultipleByteBuffers() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    Supplier<BytesInput> factory = () -> BytesInput.from(
        toByteBuffer(data, 0, 250),
        toByteBuffer(data, 250, 250),
        toByteBuffer(data, 500, 250),
        toByteBuffer(data, 750, 250));

    validate(data, factory);
  }

  @Test
  public void testFromByteArray() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    byte[] input = new byte[data.length + 20];
    RANDOM.nextBytes(input);
    System.arraycopy(data, 0, input, 10, data.length);
    Supplier<BytesInput> factory = () -> BytesInput.from(input, 10, data.length);

    validate(data, factory);
  }

  @Test
  public void testFromInputStream() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    byte[] input = new byte[data.length + 10];
    RANDOM.nextBytes(input);
    System.arraycopy(data, 0, input, 0, data.length);
    Supplier<BytesInput> factory = () -> BytesInput.from(new ByteArrayInputStream(input), 1000);

    validate(data, factory);
  }

  @Test
  public void testFromByteArrayOutputStream() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(data);
    Supplier<BytesInput> factory = () -> BytesInput.from(baos);

    validate(data, factory);
  }

  @Test
  public void testFromCapacityByteArrayOutputStreamOneSlab() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    List<CapacityByteArrayOutputStream> toClose = new ArrayList<>();
    Supplier<BytesInput> factory = () -> {
      CapacityByteArrayOutputStream cbaos = new CapacityByteArrayOutputStream(10, 1000, allocator);
      toClose.add(cbaos);
      try {
        cbaos.write(data);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return BytesInput.from(cbaos);
    };

    try {
      validate(data, factory);

      validateToByteBufferIsInternal(factory);
    } finally {
      AutoCloseables.uncheckedClose(toClose);
    }
  }

  @Test
  public void testFromCapacityByteArrayOutputStreamMultipleSlabs() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    List<CapacityByteArrayOutputStream> toClose = new ArrayList<>();
    Supplier<BytesInput> factory = () -> {
      CapacityByteArrayOutputStream cbaos = new CapacityByteArrayOutputStream(10, 1000, allocator);
      toClose.add(cbaos);
      for (byte b : data) {
        cbaos.write(b);
      }
      return BytesInput.from(cbaos);
    };

    try {
      validate(data, factory);
    } finally {
      AutoCloseables.uncheckedClose(toClose);
    }
  }

  @Test
  public void testFromInts() throws IOException {
    int[] values = new int[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, Integer.MIN_VALUE, Integer.MAX_VALUE};
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4 * values.length);
    for (int value : values) {
      BytesUtils.writeIntLittleEndian(baos, value);
    }
    byte[] data = baos.toByteArray();
    Supplier<BytesInput> factory = () -> BytesInput.fromInts(values);
    validate(data, factory);
  }

  @Test
  public void testFromInt() throws IOException {
    int value = RANDOM.nextInt();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4);
    BytesUtils.writeIntLittleEndian(baos, value);
    byte[] data = baos.toByteArray();
    Supplier<BytesInput> factory = () -> BytesInput.fromInt(value);

    validate(data, factory);
  }

  @Test
  public void testFromUnsignedVarInt() throws IOException {
    int value = RANDOM.nextInt(Short.MAX_VALUE);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(2);
    BytesUtils.writeUnsignedVarInt(value, baos);
    byte[] data = baos.toByteArray();
    Supplier<BytesInput> factory = () -> BytesInput.fromUnsignedVarInt(value);

    validate(data, factory);
  }

  @Test
  public void testFromUnsignedVarLong() throws IOException {
    long value = RANDOM.nextInt(Integer.MAX_VALUE);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4);
    BytesUtils.writeUnsignedVarLong(value, baos);
    byte[] data = baos.toByteArray();
    Supplier<BytesInput> factory = () -> BytesInput.fromUnsignedVarLong(value);

    validate(data, factory);
  }

  @Test
  public void testFromZigZagVarInt() throws IOException {
    int value = RANDOM.nextInt() % Short.MAX_VALUE;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BytesUtils.writeZigZagVarInt(value, baos);
    byte[] data = baos.toByteArray();
    Supplier<BytesInput> factory = () -> BytesInput.fromZigZagVarInt(value);

    validate(data, factory);
  }

  @Test
  public void testFromZigZagVarLong() throws IOException {
    long value = RANDOM.nextInt();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BytesUtils.writeZigZagVarLong(value, baos);
    byte[] data = baos.toByteArray();
    Supplier<BytesInput> factory = () -> BytesInput.fromZigZagVarLong(value);

    validate(data, factory);
  }

  @Test
  public void testEmpty() throws IOException {
    byte[] data = new byte[0];
    Supplier<BytesInput> factory = () -> BytesInput.empty();

    validate(data, factory);
  }

  @Test
  public void testConcatenatingByteBufferCollectorOneSlab() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    List<ConcatenatingByteBufferCollector> toClose = new ArrayList<>();

    Supplier<BytesInput> factory = () -> {
      ConcatenatingByteBufferCollector collector = new ConcatenatingByteBufferCollector(allocator);
      toClose.add(collector);
      collector.collect(BytesInput.from(toByteBuffer(data)));
      return collector;
    };

    try {
      validate(data, factory);

      validateToByteBufferIsInternal(factory);
    } finally {
      AutoCloseables.uncheckedClose(toClose);
    }
  }

  @Test
  public void testConcatenatingByteBufferCollectorMultipleSlabs() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);
    List<ConcatenatingByteBufferCollector> toClose = new ArrayList<>();

    Supplier<BytesInput> factory = () -> {
      ConcatenatingByteBufferCollector collector = new ConcatenatingByteBufferCollector(allocator);
      toClose.add(collector);
      collector.collect(BytesInput.from(toByteBuffer(data, 0, 250)));
      collector.collect(BytesInput.from(toByteBuffer(data, 250, 250)));
      collector.collect(BytesInput.from(toByteBuffer(data, 500, 250)));
      collector.collect(BytesInput.from(toByteBuffer(data, 750, 250)));
      return collector;
    };

    try {
      validate(data, factory);
    } finally {
      AutoCloseables.uncheckedClose(toClose);
    }
  }

  @Test
  public void testConcat() throws IOException {
    byte[] data = new byte[1000];
    RANDOM.nextBytes(data);

    Supplier<BytesInput> factory = () -> BytesInput.concat(
        BytesInput.from(toByteBuffer(data, 0, 250)),
        BytesInput.empty(),
        BytesInput.from(toByteBuffer(data, 250, 250)),
        BytesInput.from(data, 500, 250),
        BytesInput.from(new ByteArrayInputStream(data, 750, 250), 250));

    validate(data, factory);
  }

  private ByteBuffer toByteBuffer(byte[] data) {
    return toByteBuffer(data, 0, data.length);
  }

  private ByteBuffer toByteBuffer(byte[] data, int offset, int length) {
    ByteBuffer buf = innerAllocator.allocate(length);
    buf.put(data, offset, length);
    buf.flip();
    return buf;
  }

  private void validate(byte[] data, Supplier<BytesInput> factory) throws IOException {
    assertEquals(data.length, factory.get().size());
    validateToByteBuffer(data, factory);
    validateCopy(data, factory);
    validateToInputStream(data, factory);
    validateWriteAllTo(data, factory);
  }

  private void validateToByteBuffer(byte[] data, Supplier<BytesInput> factory) {
    BytesInput bi = factory.get();
    try (ByteBufferReleaser releaser = new ByteBufferReleaser(allocator)) {
      ByteBuffer buf = bi.toByteBuffer(releaser);
      int index = 0;
      while (buf.hasRemaining()) {
        if (buf.get() != data[index++]) {
          fail("Data mismatch at position " + index);
        }
      }
    }
  }

  private void validateCopy(byte[] data, Supplier<BytesInput> factory) throws IOException {
    BytesInput bi = factory.get();
    try (ByteBufferReleaser releaser = new ByteBufferReleaser(allocator);
        InputStream is = bi.copy(releaser).toInputStream()) {
      assertContentEquals(data, is);
    }
  }

  private void validateToInputStream(byte[] data, Supplier<BytesInput> factory) throws IOException {
    BytesInput bi = factory.get();
    try (InputStream is = bi.toInputStream()) {
      assertContentEquals(data, is);
    }
  }

  private void assertContentEquals(byte[] expected, InputStream is) throws IOException {
    byte[] actual = new byte[expected.length];
    is.read(actual);
    assertArrayEquals(expected, actual);
  }

  private void validateWriteAllTo(byte[] data, Supplier<BytesInput> factory) throws IOException {
    BytesInput bi = factory.get();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      bi.writeAllTo(baos);
      assertArrayEquals(data, baos.toByteArray());
    }
  }

  private void validateToByteBufferIsInternal(Supplier<BytesInput> factory) {
    ByteBufferAllocator allocatorMock = Mockito.mock(ByteBufferAllocator.class);
    when(allocatorMock.isDirect()).thenReturn(innerAllocator.isDirect());
    Consumer<ByteBuffer> callbackMock = Mockito.mock(Consumer.class);
    factory.get().toByteBuffer(allocatorMock, callbackMock);
    verify(allocatorMock, never()).allocate(anyInt());
    verify(callbackMock, never()).accept(anyObject());
  }
}
