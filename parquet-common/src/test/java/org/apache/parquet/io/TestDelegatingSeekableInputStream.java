/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.io;

import static org.apache.parquet.io.MockInputStream.TEST_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.Test;

public class TestDelegatingSeekableInputStream {

  @Test
  public void testReadFully() throws Exception {
    byte[] buffer = new byte[5];

    MockInputStream stream = new MockInputStream();
    DelegatingSeekableInputStream.readFully(stream, buffer, 0, buffer.length);

    assertThat(buffer).as("Byte array contents should match").isEqualTo(Arrays.copyOfRange(TEST_ARRAY, 0, 5));
    assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(5);
  }

  @Test
  public void testReadFullySmallReads() throws Exception {
    byte[] buffer = new byte[5];

    MockInputStream stream = new MockInputStream(2, 3, 3);
    DelegatingSeekableInputStream.readFully(stream, buffer, 0, buffer.length);

    assertThat(buffer).as("Byte array contents should match").isEqualTo(Arrays.copyOfRange(TEST_ARRAY, 0, 5));
    assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(5);
  }

  @Test
  public void testReadFullyJustRight() throws Exception {
    final byte[] buffer = new byte[10];

    final MockInputStream stream = new MockInputStream(2, 3, 3);
    DelegatingSeekableInputStream.readFully(stream, buffer, 0, buffer.length);

    assertThat(buffer).as("Byte array contents should match").isEqualTo(TEST_ARRAY);
    assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(10);

    assertThatThrownBy(() -> DelegatingSeekableInputStream.readFully(stream, buffer, 0, 1))
        .isInstanceOf(EOFException.class)
        .hasMessage("Reached the end of stream with 1 bytes left to read");
  }

  @Test
  public void testReadFullyUnderflow() throws Exception {
    final byte[] buffer = new byte[11];

    final MockInputStream stream = new MockInputStream(2, 3, 3);

    assertThatThrownBy(() -> DelegatingSeekableInputStream.readFully(stream, buffer, 0, buffer.length))
        .isInstanceOf(EOFException.class)
        .hasMessage("Reached the end of stream with 1 bytes left to read");

    assertThat(Arrays.copyOfRange(buffer, 0, 10))
        .as("Should have consumed bytes")
        .isEqualTo(TEST_ARRAY);
    assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(10);
  }

  @Test
  public void testReadFullyStartAndLength() throws IOException {
    byte[] buffer = new byte[10];

    MockInputStream stream = new MockInputStream();
    DelegatingSeekableInputStream.readFully(stream, buffer, 2, 5);

    assertThat(Arrays.copyOfRange(buffer, 2, 7))
        .as("Byte array contents should match")
        .isEqualTo(Arrays.copyOfRange(TEST_ARRAY, 0, 5));
    assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(5);
  }

  @Test
  public void testReadFullyZeroByteRead() throws IOException {
    byte[] buffer = new byte[0];

    MockInputStream stream = new MockInputStream();
    DelegatingSeekableInputStream.readFully(stream, buffer, 0, buffer.length);

    assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(0);
  }

  @Test
  public void testReadFullySmallReadsWithStartAndLength() throws IOException {
    byte[] buffer = new byte[10];

    MockInputStream stream = new MockInputStream(2, 2, 3);
    DelegatingSeekableInputStream.readFully(stream, buffer, 2, 5);

    assertThat(Arrays.copyOfRange(buffer, 2, 7))
        .as("Byte array contents should match")
        .isEqualTo(Arrays.copyOfRange(TEST_ARRAY, 0, 5));
    assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(5);
  }

  private static final ThreadLocal<byte[]> TEMP = ThreadLocal.withInitial(() -> new byte[8192]);

  @Test
  public void testHeapRead() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);

    MockInputStream stream = new MockInputStream();

    int len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(10);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(20);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(-1);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapSmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(5);

    MockInputStream stream = new MockInputStream();

    int len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(5);
    assertThat(readBuffer.position()).isEqualTo(5);
    assertThat(readBuffer.limit()).isEqualTo(5);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(0);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 5));
  }

  @Test
  public void testHeapSmallReads() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);

    MockInputStream stream = new MockInputStream(2, 3, 3);

    int len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(2);
    assertThat(readBuffer.position()).isEqualTo(2);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(3);
    assertThat(readBuffer.position()).isEqualTo(5);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(3);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(2);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapPosition() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);
    readBuffer.position(10);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(8);

    int len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(8);
    assertThat(readBuffer.position()).isEqualTo(18);
    assertThat(readBuffer.limit()).isEqualTo(20);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(2);
    assertThat(readBuffer.position()).isEqualTo(20);
    assertThat(readBuffer.limit()).isEqualTo(20);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(-1);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);
    readBuffer.limit(8);

    MockInputStream stream = new MockInputStream(7);

    int len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(7);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(8);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(1);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(0);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testHeapPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);
    readBuffer.position(5);
    readBuffer.limit(13);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(7);

    int len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(7);
    assertThat(readBuffer.position()).isEqualTo(12);
    assertThat(readBuffer.limit()).isEqualTo(13);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(1);
    assertThat(readBuffer.position()).isEqualTo(13);
    assertThat(readBuffer.limit()).isEqualTo(13);

    len = DelegatingSeekableInputStream.readHeapBuffer(stream, readBuffer);
    assertThat(len).isEqualTo(0);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testDirectRead() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);

    MockInputStream stream = new MockInputStream();

    int len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(10);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(20);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(-1);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectSmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(5);

    MockInputStream stream = new MockInputStream();

    int len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(5);
    assertThat(readBuffer.position()).isEqualTo(5);
    assertThat(readBuffer.limit()).isEqualTo(5);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(0);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 5));
  }

  @Test
  public void testDirectSmallReads() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    MockInputStream stream = new MockInputStream(2, 3, 3);

    int len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(2);
    assertThat(readBuffer.position()).isEqualTo(2);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(3);
    assertThat(readBuffer.position()).isEqualTo(5);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(3);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(2);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectPosition() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);
    readBuffer.position(10);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(8);

    int len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(8);
    assertThat(readBuffer.position()).isEqualTo(18);
    assertThat(readBuffer.limit()).isEqualTo(20);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(2);
    assertThat(readBuffer.position()).isEqualTo(20);
    assertThat(readBuffer.limit()).isEqualTo(20);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(-1);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);
    readBuffer.limit(8);

    MockInputStream stream = new MockInputStream(7);

    int len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(7);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(8);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(1);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(0);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testDirectPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);
    readBuffer.position(5);
    readBuffer.limit(13);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(7);

    int len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(7);
    assertThat(readBuffer.position()).isEqualTo(12);
    assertThat(readBuffer.limit()).isEqualTo(13);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(1);
    assertThat(readBuffer.position()).isEqualTo(13);
    assertThat(readBuffer.limit()).isEqualTo(13);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(len).isEqualTo(0);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testDirectSmallTempBufferSmallReads() throws Exception {
    byte[] temp = new byte[2]; // this will cause readDirectBuffer to loop

    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    MockInputStream stream = new MockInputStream(2, 3, 3);

    int len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, temp);
    assertThat(len).isEqualTo(2);
    assertThat(readBuffer.position()).isEqualTo(2);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, temp);
    assertThat(len).isEqualTo(3);
    assertThat(readBuffer.position()).isEqualTo(5);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, temp);
    assertThat(len).isEqualTo(3);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, temp);
    assertThat(len).isEqualTo(2);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, temp);
    assertThat(len).isEqualTo(-1);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectSmallTempBufferWithPositionAndLimit() throws Exception {
    byte[] temp = new byte[2]; // this will cause readDirectBuffer to loop

    ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);
    readBuffer.position(5);
    readBuffer.limit(13);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(7);

    int len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, temp);
    assertThat(len).isEqualTo(7);
    assertThat(readBuffer.position()).isEqualTo(12);
    assertThat(readBuffer.limit()).isEqualTo(13);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, temp);
    assertThat(len).isEqualTo(1);
    assertThat(readBuffer.position()).isEqualTo(13);
    assertThat(readBuffer.limit()).isEqualTo(13);

    len = DelegatingSeekableInputStream.readDirectBuffer(stream, readBuffer, temp);
    assertThat(len).isEqualTo(0);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testHeapReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(8);

    MockInputStream stream = new MockInputStream();

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testHeapReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(20);

    final MockInputStream stream = new MockInputStream();

    assertThatThrownBy(() -> DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer))
        .isInstanceOf(EOFException.class)
        .hasMessage("Reached the end of stream with 10 bytes left to read");

    assertThat(readBuffer.position()).isEqualTo(0);
    assertThat(readBuffer.limit()).isEqualTo(20);
  }

  @Test
  public void testHeapReadFullyJustRight() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);

    MockInputStream stream = new MockInputStream();

    // reads all of the bytes available without EOFException
    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    // trying to read 0 more bytes doesn't result in EOFException
    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapReadFullySmallReads() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapReadFullyPosition() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.position(3);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }

  @Test
  public void testHeapReadFullyLimit() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.limit(7);

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));

    readBuffer.position(7);
    readBuffer.limit(10);
    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapReadFullyPositionAndLimit() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 4));

    readBuffer.position(7);
    readBuffer.limit(10);
    DelegatingSeekableInputStream.readFullyHeapBuffer(stream, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }

  @Test
  public void testDirectReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(8);

    MockInputStream stream = new MockInputStream();

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testDirectReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);

    final MockInputStream stream = new MockInputStream();

    assertThatThrownBy(() -> DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get()))
        .isInstanceOf(EOFException.class)
        .hasMessage("Reached the end of stream with 10 bytes left to read");

    // NOTE: This behavior differs from readFullyHeapBuffer because direct uses
    // several read operations that will read up to the end of the input. This
    // is a correct value because the bytes in the buffer are valid. This
    // behavior can't be implemented for the heap buffer without using the read
    // method instead of the readFully method on the underlying
    // FSDataInputStream.
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(20);
  }

  @Test
  public void testDirectReadFullyJustRight() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    MockInputStream stream = new MockInputStream();

    // reads all of the bytes available without EOFException
    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    // trying to read 0 more bytes doesn't result in EOFException
    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectReadFullySmallReads() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectReadFullyPosition() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }

  @Test
  public void testDirectReadFullyLimit() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.limit(7);

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));

    readBuffer.position(7);
    readBuffer.limit(10);
    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectReadFullyPositionAndLimit() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 4));

    readBuffer.position(7);
    readBuffer.limit(10);
    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, TEMP.get());
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }

  @Test
  public void testDirectReadFullySmallTempBufferWithPositionAndLimit() throws Exception {
    byte[] temp = new byte[2]; // this will cause readFully to loop

    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    MockInputStream stream = new MockInputStream(2, 3, 3);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, temp);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, temp);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 4));

    readBuffer.position(7);
    readBuffer.limit(10);
    DelegatingSeekableInputStream.readFullyDirectBuffer(stream, readBuffer, temp);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }
}
