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

import static org.apache.hadoop.fs.StreamCapabilities.READBYTEBUFFER;
import static org.apache.parquet.hadoop.util.H3ByteBufferInputStream.performRead;
import static org.apache.parquet.hadoop.util.HadoopStreams.wrap;
import static org.apache.parquet.hadoop.util.MockHadoopInputStream.TEST_ARRAY;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.StringUtils;
import org.apache.parquet.hadoop.TestUtils;
import org.apache.parquet.io.SeekableInputStream;
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

  /**
   * The size of the stream.
   */
  private static final int LEN = TEST_ARRAY.length;

  @Parameterized.Parameters(name = "heap={0}")
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {{true}, {false}};
    return Arrays.asList(data);
  }

  /**
   * Use a heap buffer?
   */
  private final boolean useHeap;

  /**
   * Instantiate test suite.
   *
   * @param useHeap use a heap buffer?
   */
  public TestHadoop3ByteBufferReadFully(final boolean useHeap) {
    this.useHeap = useHeap;
  }

  /**
   * Allocate a buffer; choice of on/off heap depends on test suite options.
   *
   * @param capacity buffer capacity.
   *
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
    verifyBufferMatches(readBuffer, 0);
  }

  /**
   * Read more than the file size, require an EOF exception to be raised.
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
   * This fails because readFully() requires the whole buffer to be filled.
   * When the buffer limit is reduced it will work.
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

  /**
   * Create a data input stream wrapping an {@link MockByteBufferReadFullyInputStream}.
   *
   * @return in input stream.
   */
  private static FSDataInputStream stream() {
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

    // buffer unchanged
    verifyBufferMatches(readBuffer, 0);
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
   * Limit the buffer size, read it.
   */
  @Test
  public void testReadFullyLimit() throws Exception {
    ByteBuffer readBuffer = allocate(LEN);
    final int smallLimit = 7;
    readBuffer.limit(smallLimit);

    // read up to the limit, twice.
    FSDataInputStream hadoopStream = stream();
    assertBufferRead(hadoopStream, readBuffer, smallLimit, smallLimit);
    hadoopStream.seek(0);
    // the buffer is now full, so no bytes are read.
    // the the position and the limit are unchanged.
    assertBufferRead(hadoopStream, readBuffer, smallLimit, smallLimit);
    // and the stream is still at position zero.
    assertStreamAt(hadoopStream, 0);

    verifyBufferMatches(readBuffer, 0);

    // recycle the buffer with a larger value and continue
    // reading from the end of the last read.
    readBuffer.position(smallLimit);
    readBuffer.limit(LEN);
    hadoopStream.seek(smallLimit);

    assertStreamAt(hadoopStream, smallLimit);
    assertBufferRead(hadoopStream, readBuffer, LEN, LEN);
    verifyBufferMatches(readBuffer, 0);
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
  }

  /**
   * Assert that a buffer read raises EOFException.
   *
   * @param hadoopStream stream to read
   * @param readBuffer target buffer.
   */
  private static void assertThrowsEOFException(final FSDataInputStream hadoopStream, final ByteBuffer readBuffer) {
    TestUtils.assertThrows("Must throw EOFException", EOFException.class, () -> {
      performRead(hadoopStream, readBuffer);
      return null;
    });
  }

  /**
   * Regression test: verify that creating a stream for {@link MockHadoopInputStream}
   * still generates an {@link H1SeekableInputStream}.
   */
  @Test
  public void testCreateH1Stream() {
    assertStreamClass(H1SeekableInputStream.class, wrap(new FSDataInputStream(new MockHadoopInputStream())));
  }

  /**
   * Regression test: verify that creating a stream which implements
   * ByteBufferReadable but doesn't declare the capability generates {@link H2SeekableInputStream}.
   */
  @Test
  public void testDoubleWrapByteBufferStream() {
    assertStreamClass(
        H2SeekableInputStream.class,
        wrap(new FSDataInputStream(new FSDataInputStream(new MockByteBufferInputStream()))));
  }

  /**
   * Regression test: verify that creating a stream which implements
   * ByteBufferReadable generates {@link H2SeekableInputStream}.
   */
  @Test
  public void testDoubleWrapByteBufferStreamWithCapability() {
    assertStreamClass(
        H2SeekableInputStream.class,
        wrap(new FSDataInputStream(new FSDataInputStream(new MockByteBufferInputStream(READBYTEBUFFER)))));
  }

  /**
   * Assert that an instantiated stream class matches the expected class.
   * @param expected expected class
   * @param stream stream to validate
   */
  private static void assertStreamClass(
      final Class<? extends SeekableInputStream> expected, final SeekableInputStream stream) {
    Assert.assertEquals("Wrong stream class: " + stream, expected, stream.getClass());
  }

  /**
   * If a stream implements "in:preadbytebuffer" it gets bound to a H3ByteBufferInputStream.
   */
  @Test
  public void testCreateStreamWithByteBufferPositionedReadable() {
    assertStreamClass(H3ByteBufferInputStream.class, wrap(stream()));
  }

  /**
   *
   */
  @Test
  public void testDoubleWrapByteBufferPositionedReadable() {
    assertStreamClass(
        H3ByteBufferInputStream.class,
        wrap(new FSDataInputStream(new FSDataInputStream(new MockByteBufferReadFullyInputStream()))));
  }

  /**
   * The buffer reading stream is only selected if the stream declares support;
   * implementing the interface is not enough.
   */
  @Test
  public void testPositionedReadableNoCapability() {
    assertStreamClass(
        H2SeekableInputStream.class,
        wrap(new FSDataInputStream(new MockByteBufferReadFullyInputStream(READBYTEBUFFER))));
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
        return StringUtils.toLowerCase(capability).equals(PREADBYTEBUFFER);
      }

      @Override
      public int read(final long position, final ByteBuffer buf) throws IOException {
        return 0;
      }

      @Override
      public void readFully(final long position, final ByteBuffer buf) throws IOException {}
    }

    assertStreamClass(H3ByteBufferInputStream.class, wrap(new FSDataInputStream(new InconsistentStream())));
  }

  /**
   * Assert that the buffer contents match those of the input data from
   * the offset filePosition.
   * This operation reads the buffer data, so must be used after any other
   * assertions about buffer, size, position etc.
   *
   * @param readBuffer buffer to examine
   * @param filePosition file position.
   */
  public static void verifyBufferMatches(ByteBuffer readBuffer, int filePosition) {
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
   * Gets the bytes of the buffer. This sets the buffer.remaining()
   * value to 0.
   *
   * @param buffer buffer.
   *
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
   *
   * @param array source data
   *
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
   * Assert the current buffer position and limit are as expected
   *
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
   *
   * @param hadoopStream stream
   * @param pos expected position
   *
   * @throws IOException exception raised on getPos()
   */
  private static void assertStreamAt(final FSDataInputStream hadoopStream, long pos) throws IOException {
    Assert.assertEquals("Read position of stream", pos, hadoopStream.getPos());
  }

  /**
   * Read a buffer at the current position through {@link H3ByteBufferInputStream#performRead(FSDataInputStream, ByteBuffer)}.
   * Assert that the stream buffer position and limit are as expected.
   * That is: the stream position has been moved forwards by the
   * size of the buffer.
   *
   * @param hadoopStream stream
   * @param readBuffer buffer to fill
   * @param expectedBufferPosition final buffer position
   * @param expectedLimit final buffer limit
   *
   * @throws IOException read failure
   */
  private static void assertBufferRead(
      final FSDataInputStream hadoopStream,
      final ByteBuffer readBuffer,
      final int expectedBufferPosition,
      final int expectedLimit)
      throws IOException {
    final long pos = hadoopStream.getPos();
    final int remaining = readBuffer.remaining();
    final int read = performRead(hadoopStream, readBuffer);
    // the bytes read MUST match the buffer size, as this is a full buffer read.
    Assert.assertEquals("bytes read from stream", remaining, read);
    // the buffer position and limit match what was expected.
    assertPositionAndLimit(readBuffer, expectedBufferPosition, expectedLimit);
    // the stream has moved forwards.
    assertStreamAt(hadoopStream, pos + remaining);
  }

  /**
   * Input stream which claims to implement ByteBufferReadable in both interfaces and, optionally,
   * in {@code hasCapability()}.
   */
  private static class MockByteBufferInputStream extends MockHadoopInputStream
      implements ByteBufferReadable, StreamCapabilities {

    private final String[] capabilities;

    /**
     * Constructor.
     * @param capabilities an array of capabilities to declare support for.
     */
    private MockByteBufferInputStream(String... capabilities) {
      this.capabilities = capabilities;
    }

    @Override
    public int read(final ByteBuffer buf) {
      return 0;
    }

    /**
     * Does a stream have the
     * @param capability string to query the stream support for.
     * @return true if there is an entry in the capability list matching the argument.
     */
    @Override
    public boolean hasCapability(final String capability) {
      return Arrays.stream(capabilities).anyMatch(c -> c.equals(capability));
    }
  }

  /**
   * Input stream which claims to implement ByteBufferPositionedReadable,
   * unless constructed with a capability list that excludes it.
   */
  private static class MockByteBufferReadFullyInputStream extends MockByteBufferInputStream
      implements ByteBufferPositionedReadable, StreamCapabilities {

    public MockByteBufferReadFullyInputStream() {
      this(READBYTEBUFFER, PREADBYTEBUFFER);
    }

    public MockByteBufferReadFullyInputStream(final String... capabilites) {
      super(capabilites);
    }

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
  }
}
