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

import static org.apache.parquet.hadoop.util.H3ByteBufferInputStream.performRead;
import static org.apache.parquet.hadoop.util.HadoopStreams.wrap;
import static org.apache.parquet.hadoop.util.MockHadoopInputStream.TEST_ARRAY;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.StringUtils;
import org.apache.parquet.hadoop.TestUtils;
import org.apache.parquet.io.SeekableInputStream;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test {@code ByteBufferPositionedReadable.readFully()} reads.
 * Parameterized on heap vs. direct buffers.
 */
@RunWith(Parameterized.class)
public class TestHadoop3ByteBufferReadFully {

  public static final int LEN = 10;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {{"heap", true}, {"direct", false}};
    return Arrays.asList(data);
  }

  /**
   * Use a heap buffer?
   */
  private final boolean useHeap;

  public TestHadoop3ByteBufferReadFully(final String type, final boolean useHeap) {
    this.useHeap = useHeap;
  }

  /**
   * Allocate a buffer; choice of on/off heap depends on test suite options.
   * @param capacity buffer capacity.
   * @return the buffer.
   */
  private ByteBuffer allocate(int capacity) {
    return useHeap ? ByteBuffer.allocate(capacity) : ByteBuffer.allocateDirect(capacity);
  }

  /**
   * Read a buffer smaller than the source file.
   */
  @Test
  public void testReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = allocate(8);

    FSDataInputStream hadoopStream = stream();

    assertBufferRead(hadoopStream, readBuffer, 8, 8);
    assertPositionAndLimit(readBuffer, 8, 8);
    // buffer is full so no more data is read.
    assertBufferRead(hadoopStream, readBuffer, 8, 8);
    assertBufferMatches(readBuffer, 0);
  }

  /**
   * Read more than the file size, require EOF exceptions to be raised.
   */
  @Test
  public void testReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = allocate(20);

    FSDataInputStream hadoopStream = stream();

    assertThrowsEOFException(hadoopStream, readBuffer);

    // EOF check happened before the read -at least with this test stream.
    assertPositionAndLimit(readBuffer, 0, 20);
  }

  /**
   * Seek to the file, try to read a buffer more than allowed.
   */
  @Test
  public void testReadFullyFromOffset() throws Exception {
    final int size = 5;
    final ByteBuffer readBuffer = allocate(size);

    FSDataInputStream hadoopStream = stream();
    hadoopStream.seek(6);

    // read past EOF is a failure
    assertThrowsEOFException(hadoopStream, readBuffer);
    // stream does not change position
    assertStreamAt(hadoopStream, 6);

    // reduce buffer limit
    readBuffer.limit(4);
    // now the read works.
    assertBufferRead(hadoopStream, readBuffer, 4, 4);
  }

  @NotNull private static FSDataInputStream stream() {
    return new FSDataInputStream(new MockByteBufferReadFullyInputStream());
  }

  /**
   * Read exactly the size of the file.
   */
  @Test
  public void testReadFullyJustRight() throws Exception {
    ByteBuffer readBuffer = allocate(LEN);

    FSDataInputStream hadoopStream = stream();

    // reads all of the bytes available without EOFException
    assertBufferRead(hadoopStream, readBuffer, LEN, LEN);

    // trying to read 0 more bytes doesn't result in EOFException
    hadoopStream.readFully(11, readBuffer);

    assertBufferMatches(readBuffer, 0);
  }

  /**
   * Read with the buffer position set to a value within the buffer.
   */
  @Test
  public void testReadFullyPosition() throws Exception {
    ByteBuffer readBuffer = allocate(LEN);
    readBuffer.position(3);
    readBuffer.mark();

    FSDataInputStream hadoopStream = stream();
    assertBufferRead(hadoopStream, readBuffer, LEN, LEN);
    assertBufferRead(hadoopStream, readBuffer, LEN, LEN);

    // reset to where the mark is.
    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  /**
   * Limit the buffer size, read with it
   * @throws Exception
   */
  @Test
  public void testReadFullyLimit() throws Exception {
    ByteBuffer readBuffer = allocate(LEN);
    final int smallLimit = 7;
    readBuffer.limit(smallLimit);

    FSDataInputStream hadoopStream = stream();

    assertBufferRead(hadoopStream, readBuffer, smallLimit, smallLimit);
    hadoopStream.seek(0);
    assertBufferRead(hadoopStream, readBuffer, smallLimit, smallLimit);

    assertBufferMatches(readBuffer, 0);

    // recycle the buffer with a larger value and continue
    // reading from the end of the last read.
    readBuffer.position(smallLimit);
    readBuffer.limit(LEN);
    assertBufferRead(hadoopStream, readBuffer, LEN, LEN);
    assertBufferMatches(readBuffer, 0);
  }

  @Test
  public void testReadFullyPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = allocate(LEN);
    readBuffer.position(3);
    final int smallLimit = 7;
    readBuffer.limit(smallLimit);
    readBuffer.mark();

    FSDataInputStream hadoopStream = stream();

    assertBufferRead(hadoopStream, readBuffer, smallLimit, smallLimit);
    assertBufferRead(hadoopStream, readBuffer, smallLimit, smallLimit);

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, 4), readBuffer);

    readBuffer.position(smallLimit);
    readBuffer.limit(LEN);

    assertBufferRead(hadoopStream, readBuffer, LEN, LEN);
    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match", ByteBuffer.wrap(TEST_ARRAY, 0, smallLimit), readBuffer);
    // assertBufferMatches(readBuffer, 0);
  }

  private static void assertThrowsEOFException(final FSDataInputStream hadoopStream, final ByteBuffer readBuffer) {
    TestUtils.assertThrows("Should throw EOFException", EOFException.class, () -> {
      performRead(hadoopStream, readBuffer);
      return null;
    });
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

    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H3ByteBufferInputStream);
  }

  @Test
  public void testCreateStreamWithByteBufferPositionedReadable() {
    final SeekableInputStream s = wrap(stream());
    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H3ByteBufferInputStream);
  }

  @Test
  public void testDoubleWrapByteBufferPositionedReadable() {
    final SeekableInputStream s =
        wrap(new FSDataInputStream(new FSDataInputStream(new MockByteBufferReadFullyInputStream())));
    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H3ByteBufferInputStream);
  }

  /**
   * The buffer reading stream is only selected if the stream declares support;
   * implementing the interface is not enough.
   */
  @Test
  public void testPositionedReadableNoCapability() {
    class IncapableStream extends MockByteBufferReadFullyInputStream {
      @Override
      public boolean hasCapability(final String capability) {
        return false;
      }
    }
    final InputStream in = new IncapableStream();
    final SeekableInputStream s = wrap(new FSDataInputStream(in));
    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H1SeekableInputStream);
  }

  /**
   * What happens if a stream declares support for the interface,
   * but doesn't actually do it?
   * The check is based on trust: if the stream lied -it doesn't work.
   */
  @Test
  public void testCapabilityWithoutInterface() {
    class InconsistentStream extends MockHadoopInputStream
        implements ByteBufferPositionedReadable, StreamCapabilities {
      @Override
      public boolean hasCapability(final String capability) {
        return StringUtils.toLowerCase(capability).equals(StreamCapabilities.PREADBYTEBUFFER);
      }

      @Override
      public int read(final long position, final ByteBuffer buf) throws IOException {
        return 0;
      }

      @Override
      public void readFully(final long position, final ByteBuffer buf) throws IOException {}
    }

    final InputStream in = new InconsistentStream();
    final SeekableInputStream s = wrap(new FSDataInputStream(in));
    Assert.assertTrue("Wrong wrapper: " + s, s instanceof H3ByteBufferInputStream);
  }

  public static void assertBufferMatches(ByteBuffer readBuffer, int filePosition) {
    readBuffer.flip();
    final int remaining = readBuffer.remaining();
    byte[] actual = getBytes(readBuffer);
    byte[] expected = Arrays.copyOfRange(TEST_ARRAY, filePosition, remaining);
    Assert.assertEquals(
        "Buffer contents from data offset " + filePosition + " with length " + remaining,
        stringify(expected),
        stringify(actual));
  }

  /**
   * Gets the bytes of the buffer. This sets the buffer remaining
   * value to 0.
   * @param buffer buffer.
   * @return buffer contents as bytes.
   */
  public static byte[] getBytes(ByteBuffer buffer) {
    byte[] byteArray = new byte[buffer.remaining()];
    buffer.get(byteArray);
    return byteArray;
  }

  /**
   * Map a byte array to hex values.
   * Of limited value once the byte value is greater than 15
   * as the string is hard to read.
   * @param array source data
   * @return string list.
   */
  private static String stringify(byte[] array) {
    // convert to offset of lower case A, to make those assertions meaningful
    final int l = array.length;
    StringBuilder chars = new StringBuilder(l);
    for (byte b : array) {
      chars.append(Integer.toHexString(b));
    }
    return chars.toString();
  }

  /**
   * Assert the current buffer position and limit.
   * @param readBuffer buffer
   * @param bufferPosition buffer position.
   * @param limit buffer limit
   */
  private static void assertPositionAndLimit(ByteBuffer readBuffer, int bufferPosition, int limit) {
    Assert.assertEquals("Buffer Position", bufferPosition, readBuffer.position());
    Assert.assertEquals("Buffer Limit", limit, readBuffer.limit());
  }

  /**
   * Assert the stream position is at the expected value.
   * @param hadoopStream stream
   * @param pos expected position
   * @throws IOException exception raised on getPos()
   */
  private static void assertStreamAt(final FSDataInputStream hadoopStream, long pos) throws IOException {
    Assert.assertEquals("Read position of stream", pos, hadoopStream.getPos());
  }

  /**
   * Read a buffer at the current position through {@link H3ByteBufferInputStream#performRead(FSDataInputStream, ByteBuffer)}.
   * Assert that the stream buffer position and limit are what is expected
   *
   * @param hadoopStream stream
   * @param readBuffer buffer to fill
   * @param bufferPosition final buffer position
   * @param limit final buffer limit
   *
   * @throws IOException read failure
   */
  private static void assertBufferRead(
      final FSDataInputStream hadoopStream,
      final ByteBuffer readBuffer,
      final int bufferPosition,
      final int limit)
      throws IOException {
    final long pos = hadoopStream.getPos();
    final int remaining = readBuffer.remaining();
    performRead(hadoopStream, readBuffer);
    assertPositionAndLimit(readBuffer, bufferPosition, limit);
    assertStreamAt(hadoopStream, pos + remaining);
  }

  /**
   * Input stream which claims to implement ByteBufferPositionedReadable
   */
  private static class MockByteBufferReadFullyInputStream extends MockHadoopInputStream
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
      if (toRead == 0) {
        return;
      }
      if (toRead + position > length()) {
        throw new EOFException("ByteBuffer.readFully(" + position
            + ") buffer size: " + toRead
            + " reads past file length: " + length());
      }
      // return the subset of the data
      byte[] result = new byte[toRead];
      System.arraycopy(data(), (int) position, result, 0, toRead);
      buf.put(result);
    }

    /**
     * Declare support for ByteBufferPositionedReadable.
     * This is the only way that an implementation wil be picked up.
     * @param capability string to query the stream support for.
     * @return
     */
    public boolean hasCapability(final String capability) {
      return StringUtils.toLowerCase(capability).equals(StreamCapabilities.PREADBYTEBUFFER);
    }
  }
}
