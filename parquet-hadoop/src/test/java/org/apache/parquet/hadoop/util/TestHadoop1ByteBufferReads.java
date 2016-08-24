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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.hadoop.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import static org.apache.parquet.hadoop.util.MockInputStream.TEST_ARRAY;

public class TestHadoop1ByteBufferReads {

  private static final ThreadLocal<byte[]> TEMP = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[8192];
    }
  };

  @Test
  public void testHeapRead() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    int len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, len);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(-1, len);

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapSmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(5);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    int len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(5, len);
    Assert.assertEquals(5, readBuffer.position());
    Assert.assertEquals(5, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(0, len);

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 5), readBuffer);
  }

  @Test
  public void testHeapSmallReads() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    int len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(2, len);
    Assert.assertEquals(2, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(3, len);
    Assert.assertEquals(5, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(3, len);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(2, len);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapPosition() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);
    readBuffer.position(10);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(8));

    int len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(8, len);
    Assert.assertEquals(18, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(2, len);
    Assert.assertEquals(20, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(-1, len);

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);
    readBuffer.limit(8);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(7));

    int len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(7, len);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(1, len);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(0, len);

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testHeapPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);
    readBuffer.position(5);
    readBuffer.limit(13);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(7));

    int len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(7, len);
    Assert.assertEquals(12, readBuffer.position());
    Assert.assertEquals(13, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(1, len);
    Assert.assertEquals(13, readBuffer.position());
    Assert.assertEquals(13, readBuffer.limit());

    len = H1SeekableInputStream.readHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(0, len);

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testDirectRead() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    int len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, len);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(-1, len);

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectSmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(5);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    int len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(5, len);
    Assert.assertEquals(5, readBuffer.position());
    Assert.assertEquals(5, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(0, len);

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 5), readBuffer);
  }

  @Test
  public void testDirectSmallReads() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    int len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(2, len);
    Assert.assertEquals(2, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(3, len);
    Assert.assertEquals(5, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(3, len);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(2, len);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectPosition() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);
    readBuffer.position(10);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(8));

    int len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(8, len);
    Assert.assertEquals(18, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(2, len);
    Assert.assertEquals(20, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(-1, len);

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(20);
    readBuffer.limit(8);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(7));

    int len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(7, len);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(1, len);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(0, len);

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testDirectPositionAndLimit() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);
    readBuffer.position(5);
    readBuffer.limit(13);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(7));

    int len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(7, len);
    Assert.assertEquals(12, readBuffer.position());
    Assert.assertEquals(13, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(1, len);
    Assert.assertEquals(13, readBuffer.position());
    Assert.assertEquals(13, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(0, len);

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testDirectSmallTempBufferSmallReads() throws Exception {
    byte[] temp = new byte[2]; // this will cause readDirectBuffer to loop

    ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    int len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(2, len);
    Assert.assertEquals(2, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(3, len);
    Assert.assertEquals(5, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(3, len);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(2, len);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(-1, len);

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectSmallTempBufferWithPositionAndLimit() throws Exception {
    byte[] temp = new byte[2]; // this will cause readDirectBuffer to loop

    ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);
    readBuffer.position(5);
    readBuffer.limit(13);
    readBuffer.mark();

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(7));

    int len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(7, len);
    Assert.assertEquals(12, readBuffer.position());
    Assert.assertEquals(13, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(1, len);
    Assert.assertEquals(13, readBuffer.position());
    Assert.assertEquals(13, readBuffer.limit());

    len = H1SeekableInputStream.readDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(0, len);

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testHeapReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocate(8);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testHeapReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(20);

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    TestUtils.assertThrows("Should throw EOFException",
        EOFException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
            return null;
          }
        });

    Assert.assertEquals(0, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());
  }

  @Test
  public void testHeapReadFullyJustRight() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    // reads all of the bytes available without EOFException
    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    // trying to read 0 more bytes doesn't result in EOFException
    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapReadFullySmallReads() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapReadFullyPosition() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.position(3);
    readBuffer.mark();

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  @Test
  public void testHeapReadFullyLimit() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.limit(7);

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testHeapReadFullyPositionAndLimit() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocate(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 4), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    H1SeekableInputStream.readFullyHeapBuffer(hadoopStream, readBuffer);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  @Test
  public void testDirectReadFullySmallBuffer() throws Exception {
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(8);

    FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(8, readBuffer.position());
    Assert.assertEquals(8, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 8), readBuffer);
  }

  @Test
  public void testDirectReadFullyLargeBuffer() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(20);

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    TestUtils.assertThrows("Should throw EOFException",
        EOFException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
            return null;
          }
        });

    // NOTE: This behavior differs from readFullyHeapBuffer because direct uses
    // several read operations that will read up to the end of the input. This
    // is a correct value because the bytes in the buffer are valid. This
    // behavior can't be implemented for the heap buffer without using the read
    // method instead of the readFully method on the underlying
    // FSDataInputStream.
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(20, readBuffer.limit());
  }

  @Test
  public void testDirectReadFullyJustRight() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream());

    // reads all of the bytes available without EOFException
    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    // trying to read 0 more bytes doesn't result in EOFException
    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectReadFullySmallReads() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectReadFullyPosition() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.mark();

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  @Test
  public void testDirectReadFullyLimit() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.limit(7);

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.flip();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY), readBuffer);
  }

  @Test
  public void testDirectReadFullyPositionAndLimit() throws Exception {
    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 4), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, TEMP.get());
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }

  @Test
  public void testDirectReadFullySmallTempBufferWithPositionAndLimit() throws Exception {
    byte[] temp = new byte[2]; // this will cause readFully to loop

    final ByteBuffer readBuffer = ByteBuffer.allocateDirect(10);
    readBuffer.position(3);
    readBuffer.limit(7);
    readBuffer.mark();

    final FSDataInputStream hadoopStream = new FSDataInputStream(new MockInputStream(2, 3, 3));

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(7, readBuffer.position());
    Assert.assertEquals(7, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 4), readBuffer);

    readBuffer.position(7);
    readBuffer.limit(10);
    H1SeekableInputStream.readFullyDirectBuffer(hadoopStream, readBuffer, temp);
    Assert.assertEquals(10, readBuffer.position());
    Assert.assertEquals(10, readBuffer.limit());

    readBuffer.reset();
    Assert.assertEquals("Buffer contents should match",
        ByteBuffer.wrap(TEST_ARRAY, 0, 7), readBuffer);
  }
}
