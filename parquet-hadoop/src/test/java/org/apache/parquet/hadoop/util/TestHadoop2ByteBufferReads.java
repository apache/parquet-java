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

package org.apache.parquet.hadoop.util;

import static org.apache.parquet.hadoop.util.HadoopStreams.wrap;
import static org.apache.parquet.hadoop.util.MockHadoopInputStream.TEST_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

public class TestHadoop2ByteBufferReads {

  /**
   * This mimics ByteBuffer reads from streams in Hadoop 2
   */
  private static class MockBufferReader implements H2SeekableInputStream.Reader {
    private final FSDataInputStream stream;

    public MockBufferReader(FSDataInputStream stream) {
      this.stream = stream;
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      // this is inefficient, but simple for correctness tests of
      // readFully(ByteBuffer)
      byte[] temp = new byte[buf.remaining()];
      int bytesRead = stream.read(temp, 0, temp.length);
      if (bytesRead > 0) {
        buf.put(temp, 0, bytesRead);
      }
      return bytesRead;
    }
  }

  @Test
  public void testHeapReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(8);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream());
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testHeapReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(20);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream());
    final MockBufferReader reader = new MockBufferReader(hadoopStream);

    assertThatThrownBy(() -> H2SeekableInputStream.readFully(reader, readBuffer))
        .isInstanceOf(EOFException.class);

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
  public void testHeapReadFullyJustRight() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream());
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    // reads all of the bytes available without EOFException
    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    // trying to read 0 more bytes doesn't result in EOFException
    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapReadFullySmallReads() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream(2, 3, 3));
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapReadFullyPosition() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.position(3);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream(2, 3, 3));
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }

  @Test
  public void testHeapReadFullyLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.limit(7);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream(2, 3, 3));
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));

    readBuffer.position(7);
    readBuffer.limit(10);
    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testHeapReadFullyPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream(2, 3, 3));
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 4));

    readBuffer.position(7);
    readBuffer.limit(10);
    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }

  @Test
  public void testDirectReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(8);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream());
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(8);
    assertThat(readBuffer.limit()).isEqualTo(8);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 8));
  }

  @Test
  public void testDirectReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream());
    final MockBufferReader reader = new MockBufferReader(hadoopStream);

    assertThatThrownBy(() -> H2SeekableInputStream.readFully(reader, readBuffer))
        .isInstanceOf(EOFException.class);

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
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream());
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    // reads all of the bytes available without EOFException
    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    // trying to read 0 more bytes doesn't result in EOFException
    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectReadFullySmallReads() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream(2, 3, 3));
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectReadFullyPosition() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream(2, 3, 3));
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }

  @Test
  public void testDirectReadFullyLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.limit(7);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream(2, 3, 3));
    H2SeekableInputStream.Reader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));

    readBuffer.position(7);
    readBuffer.limit(10);
    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.flip();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY));
  }

  @Test
  public void testDirectReadFullyPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockHadoopInputStream(2, 3, 3));
    MockBufferReader reader = new MockBufferReader(hadoopStream);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(7);
    assertThat(readBuffer.limit()).isEqualTo(7);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 4));

    readBuffer.position(7);
    readBuffer.limit(10);
    H2SeekableInputStream.readFully(reader, readBuffer);
    assertThat(readBuffer.position()).isEqualTo(10);
    assertThat(readBuffer.limit()).isEqualTo(10);

    readBuffer.reset();
    assertThat(readBuffer).as("Buffer contents should match").isEqualTo(ByteBuffer.wrap(TEST_ARRAY, 0, 7));
  }

  @Test
  public void testCreateStreamNoByteBufferReadable() {
    final SeekableInputStream s = wrap(new FSDataInputStream(new MockHadoopInputStream()));
    assertThat(s).as("Wrong wrapper: " + s).isInstanceOf(H1SeekableInputStream.class);
  }

  @Test
  public void testDoubleWrapNoByteBufferReadable() {
    final SeekableInputStream s = wrap(new FSDataInputStream(new FSDataInputStream(new MockHadoopInputStream())));
    assertThat(s).as("Wrong wrapper: " + s).isInstanceOf(H1SeekableInputStream.class);
  }

  @Test
  public void testCreateStreamWithByteBufferReadable() {
    final SeekableInputStream s = wrap(new FSDataInputStream(new MockByteBufferInputStream()));
    assertThat(s).as("Wrong wrapper: " + s).isInstanceOf(H2SeekableInputStream.class);
  }

  @Test
  public void testDoubleWrapByteBufferReadable() {
    final SeekableInputStream s =
        wrap(new FSDataInputStream(new FSDataInputStream(new MockByteBufferInputStream())));
    assertThat(s).as("Wrong wrapper: " + s).isInstanceOf(H2SeekableInputStream.class);
  }

  /**
   * Input stream which claims to implement ByteBufferReadable.
   */
  private static final class MockByteBufferInputStream extends MockHadoopInputStream implements ByteBufferReadable {

    @Override
    public int read(final ByteBuffer buf) {
      return 0;
    }
  }
}
