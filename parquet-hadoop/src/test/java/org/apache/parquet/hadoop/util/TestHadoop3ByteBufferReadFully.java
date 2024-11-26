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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.StringUtils;
import org.apache.parquet.hadoop.TestUtils;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@code ByteBufferPositionedReadable.readFully()} reads.
 */
public class TestHadoop3ByteBufferReadFully {

  @Test
  public void testHeapReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(8);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testHeapReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(20);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    assertThrowsEOFException(hadoopStream, 0, readBuffer);

    // NOTE: This behavior differs from readFullyHeapBuffer because direct uses
    // several read operations that will read up to the end of the input. This
    // is a correct value because the bytes in the buffer are valid. This
    // behavior can't be implemented for the heap buffer without using the read
    // method instead of the readFully method on the underlying
    // FSDataInputStream.
    assertPositionAndLimit(readBuffer, 10, 20);
  }

  private static void assertPositionAndLimit(ByteBuffer readBuffer, int pos, int limit) {
    assertPosition(readBuffer, pos);
    assertLimit(readBuffer, limit);
  }

  private static void assertPosition(final ByteBuffer readBuffer, final int pos) {
    Assert.assertEquals("Buffer Position", pos, readBuffer.position());
  }

  private static void assertLimit(final ByteBuffer readBuffer, final int limit) {
    Assert.assertEquals("Buffer Limit", limit, readBuffer.limit());
  }

  @Test
  public void testHeapReadFullyJustRight() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    // reads all of the bytes available without EOFException
    hadoopStream.readFully(0, readBuffer);
    assertPosition(readBuffer, 10);

    // trying to read 0 more bytes doesn't result in EOFException
    hadoopStream.readFully(11, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapReadFullySmallReads() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapReadFullyPosition() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.position(3);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  @Test
  public void testHeapReadFullyLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.limit(7);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapReadFullyPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 4), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  @Test
  public void testDirectReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(8);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testDirectReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    final int position = 0;
    assertThrowsEOFException(hadoopStream, position, readBuffer);

    // NOTE: This behavior differs from readFullyHeapBuffer because direct uses
    // several read operations that will read up to the end of the input. This
    // is a correct value because the bytes in the buffer are valid. This
    // behavior can't be implemented for the heap buffer without using the read
    // method instead of the readFully method on the underlying
    // FSDataInputStream.
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());
  }

  private static void assertThrowsEOFException(
      final FSDataInputStream hadoopStream, final int position, final ByteBuffer readBuffer) {
    TestUtils.assertThrows("Should throw EOFException", EOFException.class, () -> {
      hadoopStream.readFully(position, readBuffer);
      return null;
    });
  }

  @Test
  public void testDirectReadFullyJustRight() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    // reads all of the bytes available without EOFException
    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    // trying to read 0 more bytes doesn't result in EOFException
    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectReadFullySmallReads() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectReadFullyPosition() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  @Test
  public void testDirectReadFullyLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.limit(7);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectReadFullyPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockByteBufferReadFullyInputStream());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 4), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    hadoopStream.readFully(0, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  @Test
  public void testCreateStreamNoByteBufferPositionedReadable() {
    final SeekableInputStream s = wrap(new FSDataInputStream(new MockHadoopInputStream()));
    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H1SeekableInputStream);
  }

  @Test
  public void testDoubleWrapNoByteBufferPositionedReadable() {
    final SeekableInputStream s =
        wrap(new FSDataInputStream(new FSDataInputStream(new MockByteBufferReadFullyInputStream())));
    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H1SeekableInputStream);
  }

  @Test
  public void testCreateStreamWithByteBufferPositionedReadable() {
    final SeekableInputStream s = wrap(new FSDataInputStream(new MockByteBufferReadFullyInputStream()));
    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H3ByteBufferInputStream);
  }

  @Test
  public void testDoubleWrapByteBufferPositionedReadable() {
    final SeekableInputStream s =
        wrap(new FSDataInputStream(new FSDataInputStream(new MockByteBufferReadFullyInputStream())));
    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H3ByteBufferInputStream);
  }

  /**
   * Input stream which claims to implement ByteBufferPositionedReadable
   */
  private static final class MockByteBufferReadFullyInputStream extends MockHadoopInputStream
      implements ByteBufferPositionedReadable, StreamCapabilities {

    @Override
    public int read(final long position, final ByteBuffer buf) throws IOException {
      rejectNegativePosition(position);
      return 0;
    }

    @Override
    public void readFully(final long position, final ByteBuffer buf) throws IOException {

      // validation
      rejectNegativePosition(position);
      final int toRead = buf.remaining();
      if (getPos() + length() > toRead) {
        throw new EOFException("Read past " + length());
      }
      // return the subset of the data
      byte[] result = new byte[toRead];
      System.arraycopy(data(), 0, result, 0, toRead);
      buf.put(result);
    }

    public boolean hasCapability(final String capability) {
      switch (StringUtils.toLowerCase(capability)) {
        case StreamCapabilities.READBYTEBUFFER:
        case StreamCapabilities.PREADBYTEBUFFER:
          return true;
        default:
          return false;
      }
    }
  }
}
