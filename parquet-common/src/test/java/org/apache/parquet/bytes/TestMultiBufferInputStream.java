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

package org.apache.parquet.bytes;

import org.junit.Assert;
import org.junit.Test;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class TestMultiBufferInputStream {
  private static final List<ByteBuffer> DATA = Arrays.asList(
      ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }),
      ByteBuffer.wrap(new byte[] { 9, 10, 11, 12 }),
      ByteBuffer.wrap(new byte[] {  }),
      ByteBuffer.wrap(new byte[] { 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 }),
      ByteBuffer.wrap(new byte[] { 25 }),
      ByteBuffer.wrap(new byte[] { 26, 27, 28, 29, 30, 31, 32 }),
      ByteBuffer.wrap(new byte[] { 33, 34 })
  );

  @Test
  public void testRead0() {
    byte[] bytes = new byte[0];

    MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    Assert.assertEquals("Should read 0 bytes", 0, stream.read(bytes));

    int bytesRead = stream.read(new byte[100]);
    Assert.assertTrue("Should read to end of stream", bytesRead < 100);

    Assert.assertEquals("Should read 0 bytes at end of stream",
        0, stream.read(bytes));
  }

  @Test
  public void testReadAll() {
    byte[] bytes = new byte[35];

    MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    int bytesRead = stream.read(bytes);
    Assert.assertEquals("Should read the entire buffer",
        bytes.length, bytesRead);

    for (int i = 0; i < bytes.length; i += 1) {
      Assert.assertEquals("Byte i should be i", i, bytes[i]);
      Assert.assertEquals("Should advance position", 35, stream.position());
    }

    Assert.assertEquals("Should have no more remaining content",
        0, stream.available());

    Assert.assertEquals("Should return -1 at end of stream",
        -1, stream.read(bytes));

    Assert.assertEquals("Should have no more remaining content",
        0, stream.available());

    for (ByteBuffer buffer : DATA) {
      Assert.assertEquals("Position should not change", 0, buffer.position());
      Assert.assertEquals("Limit should not change",
          buffer.array().length, buffer.limit());
    }
  }

  @Test
  public void testSmallReads() {
    for (int size = 1; size < 36; size += 1) {
      byte[] bytes = new byte[size];

      MultiBufferInputStream stream = new MultiBufferInputStream(DATA);
      long length = stream.available();

      int lastBytesRead = bytes.length;
      for (int offset = 0; offset < length; offset += bytes.length) {
        Assert.assertEquals("Should read requested len",
            bytes.length, lastBytesRead);

        lastBytesRead = stream.read(bytes, 0, bytes.length);

        Assert.assertEquals("Should advance position",
            offset + lastBytesRead, stream.position());

        // validate the bytes that were read
        for (int i = 0; i < lastBytesRead; i += 1) {
          Assert.assertEquals("Byte i should be i", offset + i, bytes[i]);
        }
      }

      Assert.assertEquals("Should read fewer bytes at end of buffer",
          length % bytes.length, lastBytesRead % bytes.length);

      Assert.assertEquals("Should have no more remaining content",
          0, stream.available());

      Assert.assertEquals("Should return -1 at end of stream",
          -1, stream.read(bytes));

      Assert.assertEquals("Should have no more remaining content",
          0, stream.available());
    }

    for (ByteBuffer buffer : DATA) {
      Assert.assertEquals("Position should not change", 0, buffer.position());
      Assert.assertEquals("Limit should not change",
          buffer.array().length, buffer.limit());
    }
  }

  @Test
  public void testPartialBufferReads() {
    for (int size = 1; size < 35; size += 1) {
      byte[] bytes = new byte[33];

      MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

      int lastBytesRead = size;
      for (int offset = 0; offset < bytes.length; offset += size) {
        Assert.assertEquals("Should read requested len", size, lastBytesRead);

        lastBytesRead = stream.read(
            bytes, offset, Math.min(size, bytes.length - offset));

        Assert.assertEquals("Should advance position",
            lastBytesRead > 0 ? offset + lastBytesRead : offset,
            stream.position());
      }

      Assert.assertEquals("Should read fewer bytes at end of buffer",
          bytes.length % size, lastBytesRead % size);

      for (int i = 0; i < bytes.length; i += 1) {
        Assert.assertEquals("Byte i should be i", i, bytes[i]);
      }

      Assert.assertEquals("Should have no more remaining content",
          2, stream.available());

      Assert.assertEquals("Should return 2 more bytes",
          2, stream.read(bytes));

      Assert.assertEquals("Should have no more remaining content",
          0, stream.available());

      Assert.assertEquals("Should return -1 at end of stream",
          -1, stream.read(bytes));

      Assert.assertEquals("Should have no more remaining content",
          0, stream.available());
    }

    for (ByteBuffer buffer : DATA) {
      Assert.assertEquals("Position should not change", 0, buffer.position());
      Assert.assertEquals("Limit should not change",
          buffer.array().length, buffer.limit());
    }
  }

  @Test
  public void testReadByte() throws Exception {
    final MultiBufferInputStream stream = new MultiBufferInputStream(DATA);
    int length = stream.available();

    for (int i = 0; i < length; i += 1) {
      Assert.assertEquals("Position should increment", i, stream.position());
      Assert.assertEquals(i, stream.read());
    }

    assertThrows("Should throw EOFException at end of stream",
        EOFException.class, new Callable<Integer>() {
          @Override
          public Integer call() throws IOException {
            return stream.read();
          }
        });

    for (ByteBuffer buffer : DATA) {
      Assert.assertEquals("Position should not change", 0, buffer.position());
      Assert.assertEquals("Limit should not change",
          buffer.array().length, buffer.limit());
    }
  }

  @Test
  public void testSlice0() throws Exception {
    MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    Assert.assertEquals("Should return an empty list",
        Collections.emptyList(), stream.sliceBuffers(0));
  }

  @Test
  public void testWholeSlice() throws Exception {
    final MultiBufferInputStream stream = new MultiBufferInputStream(DATA);
    final int length = stream.available();

    List<ByteBuffer> buffers = stream.sliceBuffers(stream.available());

    Assert.assertEquals("Should consume all buffers", length, stream.position());

    List<ByteBuffer> nonEmptyBuffers = new ArrayList<>();
    for (ByteBuffer buffer : DATA) {
      if (buffer.remaining() > 0) {
        nonEmptyBuffers.add(buffer);
      }
    }
    Assert.assertEquals("Should return duplicates of all non-empty buffers",
        nonEmptyBuffers, buffers);

    assertThrows("Should throw EOFException when empty",
        EOFException.class, new Callable<List<ByteBuffer>>() {
          @Override
          public List<ByteBuffer> call() throws Exception {
            return stream.sliceBuffers(length);
          }
        });

    MultiBufferInputStream copy = new MultiBufferInputStream(buffers);
    for (int i = 0; i < length; i += 1) {
      Assert.assertEquals("Slice should have identical data", i, copy.read());
    }

    for (ByteBuffer buffer : DATA) {
      Assert.assertEquals("Position should not change", 0, buffer.position());
      Assert.assertEquals("Limit should not change",
          buffer.array().length, buffer.limit());
    }
  }

  @Test
  public void testSliceCoverage() throws Exception {
    for (int size = 1; size < 36; size += 1) {
      MultiBufferInputStream stream = new MultiBufferInputStream(DATA);
      int length = stream.available();

      List<ByteBuffer> buffers = new ArrayList<>();
      while (stream.available() > 0) {
        buffers.addAll(stream.sliceBuffers(Math.min(size, stream.available())));
      }

      Assert.assertEquals("Should consume all content",
          length, stream.position());

      MultiBufferInputStream newStream = new MultiBufferInputStream(buffers);

      for (int i = 0; i < length; i += 1) {
        Assert.assertEquals("Data should be correct", i, newStream.read());
      }
    }

    for (ByteBuffer buffer : DATA) {
      Assert.assertEquals("Position should not change", 0, buffer.position());
      Assert.assertEquals("Limit should not change",
          buffer.array().length, buffer.limit());
    }
  }

  @Test
  public void testSliceModification() throws Exception {
    MultiBufferInputStream stream = new MultiBufferInputStream(DATA);
    int length = stream.available();

    int sliceLength = 5;
    List<ByteBuffer> buffers = stream.sliceBuffers(sliceLength);
    Assert.assertEquals("Should advance the original stream",
        length - sliceLength, stream.available());
    Assert.assertEquals("Should advance the original stream position",
        sliceLength, stream.position());

    Assert.assertEquals("Should return a slice of the first buffer",
        1, buffers.size());

    ByteBuffer buffer = buffers.get(0);
    Assert.assertEquals("Should have requested bytes",
        sliceLength, buffer.remaining());

    // read the buffer one past the returned limit. this should not change the
    // next value in the original stream
    buffer.limit(sliceLength + 1);
    for (int i = 0; i < sliceLength + 1; i += 1) {
      Assert.assertEquals("Should have correct data", i, buffer.get());
    }

    Assert.assertEquals("Reading a slice shouldn't advance the original stream",
        sliceLength, stream.position());
    Assert.assertEquals("Reading a slice shouldn't change the underlying data",
        sliceLength, stream.read());

    // change the underlying data buffer
    buffer.limit(sliceLength + 2);
    int originalValue = buffer.duplicate().get();
    ByteBuffer undoBuffer = buffer.duplicate();

    try {
      buffer.put((byte) 255);

      Assert.assertEquals(
          "Writing to a slice shouldn't advance the original stream",
          sliceLength + 1, stream.position());
      Assert.assertEquals(
          "Writing to a slice should change the underlying data",
          255, stream.read());

    } finally {
      undoBuffer.put((byte) originalValue);
    }
  }

  @Test
  public void testMark() throws Exception {
    MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    stream.read(new byte[7]);
    stream.mark(100);

    long mark = stream.position();

    byte[] expected = new byte[100];
    int expectedBytesRead = stream.read(expected);

    long end = stream.position();

    stream.reset();

    Assert.assertEquals("Position should return to the mark",
        mark, stream.position());

    byte[] afterReset = new byte[100];
    int bytesReadAfterReset = stream.read(afterReset);

    Assert.assertEquals("Should read the same number of bytes",
        expectedBytesRead, bytesReadAfterReset);

    Assert.assertEquals("Read should end at the same position",
        end, stream.position());

    Assert.assertArrayEquals("Content should be equal", expected, afterReset);
  }

  @Test
  public void testMarkAtStart() throws Exception {
    MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    stream.mark(100);

    long mark = stream.position();

    byte[] expected = new byte[10];
    Assert.assertEquals("Should read 10 bytes", 10, stream.read(expected));

    long end = stream.position();

    stream.reset();

    Assert.assertEquals("Position should return to the mark",
        mark, stream.position());

    byte[] afterReset = new byte[10];
    Assert.assertEquals("Should read 10 bytes", 10, stream.read(afterReset));

    Assert.assertEquals("Read should end at the same position",
        end, stream.position());

    Assert.assertArrayEquals("Content should be equal", expected, afterReset);
  }

  @Test
  public void testMarkAtEnd() throws Exception {
    MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    int bytesRead = stream.read(new byte[100]);
    Assert.assertTrue("Should read to end of stream", bytesRead < 100);

    stream.mark(100);

    long mark = stream.position();

    byte[] expected = new byte[10];
    Assert.assertEquals("Should read 0 bytes", -1, stream.read(expected));

    long end = stream.position();

    stream.reset();

    Assert.assertEquals("Position should return to the mark",
        mark, stream.position());

    byte[] afterReset = new byte[10];
    Assert.assertEquals("Should read 0 bytes", -1, stream.read(afterReset));

    Assert.assertEquals("Read should end at the same position",
        end, stream.position());

    Assert.assertArrayEquals("Content should be equal", expected, afterReset);
  }

  @Test
  public void testMarkUnset() {
    final MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    assertThrows("Should throw an error for reset() without mark()",
        IOException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            stream.reset();
            return null;
          }
        });
  }

  @Test
  public void testMarkAndResetTwiceOverSameRange() throws Exception {
    final MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    byte[] expected = new byte[6];
    stream.mark(10);
    Assert.assertEquals("Should read expected bytes",
        expected.length, stream.read(expected));

    stream.reset();
    stream.mark(10);

    byte[] firstRead = new byte[6];
    Assert.assertEquals("Should read firstRead bytes",
        firstRead.length, stream.read(firstRead));

    stream.reset();

    byte[] secondRead = new byte[6];
    Assert.assertEquals("Should read secondRead bytes",
        secondRead.length, stream.read(secondRead));

    Assert.assertArrayEquals("First read should be correct",
        expected, firstRead);

    Assert.assertArrayEquals("Second read should be correct",
        expected, secondRead);
  }

  @Test
  public void testMarkLimit() throws Exception {
    final MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    stream.mark(5);
    Assert.assertEquals("Should read 5 bytes", 5, stream.read(new byte[5]));

    stream.reset();

    Assert.assertEquals("Should read 6 bytes", 6, stream.read(new byte[6]));

    assertThrows("Should throw an error for reset() after limit",
        IOException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            stream.reset();
            return null;
          }
        });
  }

  @Test
  public void testMarkDoubleReset() throws Exception {
    final MultiBufferInputStream stream = new MultiBufferInputStream(DATA);

    stream.mark(5);
    Assert.assertEquals("Should read 5 bytes", 5, stream.read(new byte[5]));

    stream.reset();

    assertThrows("Should throw an error for double reset()",
        IOException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            stream.reset();
            return null;
          }
        });
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(
      String message, Class<? extends Exception> expected, Callable callable) {
    try {
      callable.call();
      Assert.fail("No exception was thrown (" + message + "), expected: " +
          expected.getName());
    } catch (Exception actual) {
      try {
        Assert.assertEquals(message, expected, actual.getClass());
      } catch (AssertionError e) {
        e.addSuppressed(actual);
        throw e;
      }
    }
  }
}
