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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FixedLenByteArrayPlainValuesWriter} and
 * {@link FixedLenByteArrayPlainValuesReader} covering scalar
 * round-trips for fixed-length byte arrays.
 */
public class TestFixedLenByteArrayPlainValuesWriterReader {

  private static final int FIXED_LEN = 12;

  private TrackingByteBufferAllocator allocator;

  @BeforeEach
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @AfterEach
  public void closeAllocator() {
    allocator.close();
  }

  private FixedLenByteArrayPlainValuesWriter newWriter() {
    return new FixedLenByteArrayPlainValuesWriter(FIXED_LEN, 1024, 64 * 1024, allocator);
  }

  private ByteBufferInputStream wrapForReading(FixedLenByteArrayPlainValuesWriter writer) throws IOException {
    byte[] bytes = writer.getBytes().toByteArray();
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));
  }

  private Binary fixedBinary(int seed) {
    byte[] data = new byte[FIXED_LEN];
    for (int i = 0; i < FIXED_LEN; i++) {
      data[i] = (byte) ((seed + i) & 0xFF);
    }
    return Binary.fromConstantByteArray(data);
  }

  // ---- Encoding metadata ----

  @Test
  public void testEncoding() {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      assertThat(writer.getEncoding()).isEqualTo(Encoding.PLAIN);
    }
  }

  // ---- Scalar round-trip ----

  @Test
  public void testScalarRoundTrip() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary[] expected = {fixedBinary(0), fixedBinary(100), fixedBinary(200)};
      for (Binary v : expected) {
        writer.writeBytes(v);
      }

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertThat(reader.readBytes().getBytes())
            .as("value at index " + i)
            .isEqualTo(expected[i].getBytes());
      }
    }
  }

  // ---- Skip ----

  @Test
  public void testSkip() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      writer.writeBytes(fixedBinary(1));
      writer.writeBytes(fixedBinary(2));
      writer.writeBytes(fixedBinary(3));
      writer.writeBytes(fixedBinary(4));

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(4, wrapForReading(writer));

      reader.skip(); // skip 1
      assertThat(reader.readBytes().getBytes()).isEqualTo(fixedBinary(2).getBytes());
      reader.skip(1); // skip 3
      assertThat(reader.readBytes().getBytes()).isEqualTo(fixedBinary(4).getBytes());
    }
  }

  // ---- Wrong length rejection ----

  @Test
  public void testRejectWrongLengthScalar() {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      Binary wrongLen = Binary.fromConstantByteArray(new byte[FIXED_LEN + 1]);
      assertThatThrownBy(() -> writer.writeBytes(wrongLen))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Fixed Binary size 13 does not match field type length 12");
    }
  }

  // ---- Reset ----

  @Test
  public void testWriterReset() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      writer.writeBytes(fixedBinary(99));
      writer.reset();
      assertThat(writer.getBufferedSize()).isZero();

      writer.writeBytes(fixedBinary(42));

      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(1, wrapForReading(writer));

      assertThat(reader.readBytes().getBytes()).isEqualTo(fixedBinary(42).getBytes());
    }
  }

  // ---- Empty page ----

  @Test
  public void testEmptyPage() throws IOException {
    try (FixedLenByteArrayPlainValuesWriter writer = newWriter()) {
      FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(FIXED_LEN);
      reader.initFromPage(0, wrapForReading(writer));
      // Should not throw
    }
  }
}
