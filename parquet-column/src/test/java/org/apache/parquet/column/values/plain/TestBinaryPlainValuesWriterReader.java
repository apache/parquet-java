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
package org.apache.parquet.column.values.plain;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.io.api.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link BinaryPlainValuesReader} (variable-length BINARY)
 * covering scalar round-trips via {@link PlainValuesWriter}.
 */
public class TestBinaryPlainValuesWriterReader {

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  private PlainValuesWriter newWriter() {
    return new PlainValuesWriter(1024, 64 * 1024, allocator);
  }

  private ByteBufferInputStream wrapForReading(PlainValuesWriter writer) throws IOException {
    byte[] bytes = writer.getBytes().toByteArray();
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));
  }

  private Binary binaryFromString(String s) {
    return Binary.fromConstantByteArray(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  // ---- Scalar round-trip ----

  @Test
  public void testScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      String[] strings = {"hello", "", "world", "a", "longer string value"};
      for (String s : strings) {
        writer.writeBytes(binaryFromString(s));
      }

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(strings.length, wrapForReading(writer));

      for (String s : strings) {
        assertEquals(s, reader.readBytes().toStringUsingUTF8());
      }
    }
  }

  // ---- Skip ----

  @Test
  public void testSkip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeBytes(binaryFromString("first"));
      writer.writeBytes(binaryFromString("second"));
      writer.writeBytes(binaryFromString("third"));

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      reader.skip(); // skip "first"
      assertEquals("second", reader.readBytes().toStringUsingUTF8());
    }
  }

  // ---- Empty page ----

  @Test
  public void testEmptyPage() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(0, wrapForReading(writer));
      // Should not throw
    }
  }

  // ---- Binary content (non-UTF8) ----

  @Test
  public void testBinaryContent() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      byte[] raw1 = {0, 1, 2, (byte) 0xFF, (byte) 0xFE};
      byte[] raw2 = {(byte) 0x80, 0};
      Binary bin1 = Binary.fromConstantByteArray(raw1);
      Binary bin2 = Binary.fromConstantByteArray(raw2);

      writer.writeBytes(bin1);
      writer.writeBytes(bin2);

      BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(2, wrapForReading(writer));

      assertArrayEquals(raw1, reader.readBytes().getBytes());
      assertArrayEquals(raw2, reader.readBytes().getBytes());
    }
  }
}
