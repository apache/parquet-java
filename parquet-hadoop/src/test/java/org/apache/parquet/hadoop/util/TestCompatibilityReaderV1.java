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
package org.apache.parquet.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.Test;

import junit.framework.Assert;

public class TestCompatibilityReaderV1 {

  private static final byte [] TEST_ARRAY = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

  private static class MockInputStream extends ByteArrayInputStream
    implements Seekable, PositionedReadable {
    public MockInputStream(byte[] buf) {
      super(buf);
    }

    // empty implementation for unused methods
    public int read(long position, byte[] buffer, int offset, int length) { return -1; }
    public void readFully(long position, byte[] buffer, int offset, int length) {}
    public void readFully(long position, byte[] buffer) {}
    public void seek(long position) {}
    public long getPos() { return 0; }
    public boolean seekToNewSource(long targetPos) { return false; }
  }

  // confirm writer version when flag = false
  @Test
  public void testReaderFlagOff() {
    CompatibilityReader reader = CompatibilityUtil.getHadoopReader(false);
    Assert.assertEquals("Incorrect CompatibilityReader instantiated", CompatibilityReaderV1.class, reader.getClass());
  }

  // confirm writer version when flag is true but we're on hadoop 1.x
  @Test
  public void testReaderFlagTrueHadoopV1() {
    CompatibilityReader reader = CompatibilityUtil.getHadoopReader(true);
    Assert.assertEquals("Incorrect CompatibilityReader instantiated", CompatibilityReaderV1.class, reader.getClass());
  }

  @Test
  public void testReadBufWithArray() throws Exception {
    CompatibilityReader reader = CompatibilityUtil.getHadoopReader(false);
    ByteBuffer byteBuffer = ByteBuffer.allocate(10);
    FSDataInputStream fsDataInputStream = new FSDataInputStream(new MockInputStream(TEST_ARRAY));

    int readCount = reader.readFully(fsDataInputStream, byteBuffer);
    Assert.assertEquals("Mismatching no of chars read", 10, readCount);
    Assert.assertFalse("Byte buffer not full", byteBuffer.hasRemaining());
  }

  @Test
  public void testReadBufWithoutArray() throws Exception {
    CompatibilityReader reader = CompatibilityUtil.getHadoopReader(false);
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10);
    FSDataInputStream fsDataInputStream = new FSDataInputStream(new MockInputStream(TEST_ARRAY));

    int readCount = reader.readFully(fsDataInputStream, byteBuffer);
    Assert.assertEquals("Mismatching no of chars read", 10, readCount);
    Assert.assertFalse("Byte buffer not full", byteBuffer.hasRemaining());
  }

  @Test
  public void testReadBufWithSmallerBuffer() throws Exception {
    CompatibilityReader reader = CompatibilityUtil.getHadoopReader(false);
    ByteBuffer byteBuffer = ByteBuffer.allocate(5);
    FSDataInputStream fsDataInputStream = new FSDataInputStream(new MockInputStream(TEST_ARRAY));

    int readCount = reader.readFully(fsDataInputStream, byteBuffer);
    Assert.assertEquals("Mismatching no of chars read", 5, readCount);
    Assert.assertFalse("Byte buffer not full", byteBuffer.hasRemaining());
  }

  @Test(expected = EOFException.class)
  public void testReadBufWithLargerBuffer() throws Exception {
    CompatibilityReader reader = CompatibilityUtil.getHadoopReader(false);
    ByteBuffer byteBuffer = ByteBuffer.allocate(50);
    FSDataInputStream fsDataInputStream = new FSDataInputStream(new MockInputStream(TEST_ARRAY));

    // this throws an exception as we are trying to read 50 chars and have only 10
    reader.readFully(fsDataInputStream, byteBuffer);
  }

}
