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

package org.apache.parquet.column.values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.api.Binary;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests how {@link ValuesReader} works in case of extending it.
 */
public class TestValuesReaderImpl {

  private static class InvalidValuesReaderImpl extends ValuesReader {
    @Override
    public void skip() {}
  }

  private static class ByteBufferValuesReaderImpl extends ValuesReader {
    private byte[] data;

    @Override
    public void initFromPage(int valueCount, ByteBuffer page, int offset) throws IOException {
      data = new byte[valueCount];
      ByteBuffer buffer = page.duplicate();
      buffer.position(offset);
      buffer.get(data);
    }

    public void assertPageEquals(String expected) {
      Assert.assertEquals(expected, new String(data));
    }

    @Override
    public void skip() {}

    @Override
    public Binary readBytes() {
      return Binary.fromConstantByteArray(data);
    }
  }

  private static class ByteBufferInputStreamValuesReaderImpl extends ValuesReader {
    private byte[] data;

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
      data = new byte[valueCount];
      int off = 0;
      int len = valueCount;
      int read;
      while ((read = in.read(data, off, len)) != -1 && len > 0) {
        off += read;
        len -= read;
      }
    }

    @Override
    public void skip() {}

    @Override
    public Binary readBytes() {
      return Binary.fromConstantByteArray(data);
    }
  }

  @Test
  public void testInvalidValuesReaderImpl() throws IOException {
    ValuesReader reader = new InvalidValuesReaderImpl();
    try {
      validateWithByteArray(reader);
      fail("An UnsupportedOperationException should have been thrown");
    } catch (UnsupportedOperationException e) {
    }
    try {
      validateWithByteBuffer(reader);
      fail("An UnsupportedOperationException should have been thrown");
    } catch (UnsupportedOperationException e) {
    }
    try {
      validateWithByteBufferInputStream(reader);
      fail("An UnsupportedOperationException should have been thrown");
    } catch (UnsupportedOperationException e) {
    }
  }

  @Test
  public void testByteBufferValuesReaderImpl() throws IOException {
    ValuesReader reader = new ByteBufferValuesReaderImpl();
    validateWithByteArray(reader);
    validateWithByteBuffer(reader);
    validateWithByteBufferInputStream(reader);
  }

  @Test
  public void testByteBufferInputStreamValuesReaderImpl() throws IOException {
    ValuesReader reader = new ByteBufferInputStreamValuesReaderImpl();
    validateWithByteArray(reader);
    validateWithByteBuffer(reader);
    validateWithByteBufferInputStream(reader);
  }

  private void validateWithByteArray(ValuesReader reader) throws IOException {
    reader.initFromPage(25, "==padding==The expected page content".getBytes(), 11);
    assertEquals("The expected page content", reader.readBytes().toStringUsingUTF8());
  }

  private void validateWithByteBuffer(ValuesReader reader) throws IOException {
    reader.initFromPage(25, ByteBuffer.wrap("==padding==The expected page content".getBytes()), 11);
    assertEquals("The expected page content", reader.readBytes().toStringUsingUTF8());
  }

  private void validateWithByteBufferInputStream(ValuesReader reader) throws IOException {
    ByteBufferInputStream bbis = ByteBufferInputStream.wrap(
        ByteBuffer.wrap("==padding==".getBytes()),
        ByteBuffer.wrap("The expected ".getBytes()),
        ByteBuffer.wrap("page content".getBytes()));
    bbis.skipFully(11);
    reader.initFromPage(25, bbis);
    assertEquals("The expected page content", reader.readBytes().toStringUsingUTF8());
  }
}
