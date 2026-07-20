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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PlainValuesWriter} and {@link PlainValuesReader} covering
 * scalar read/write round-trips for int, long, float, and double.
 */
public class TestPlainValuesWriterReader {

  private TrackingByteBufferAllocator allocator;

  @BeforeEach
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @AfterEach
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

  // ---- Encoding metadata ----

  @Test
  public void testEncoding() {
    try (PlainValuesWriter writer = newWriter()) {
      assertThat(writer.getEncoding()).isEqualTo(Encoding.PLAIN);
    }
  }

  // ---- Integer scalar ----

  @Test
  public void testIntegerScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      int[] expected = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 42, -42};
      for (int v : expected) {
        writer.writeInteger(v);
      }

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertThat(reader.readInteger()).as("value at index " + i).isEqualTo(expected[i]);
      }
    }
  }

  // ---- Long scalar ----

  @Test
  public void testLongScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      long[] expected = {0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 123456789L};
      for (long v : expected) {
        writer.writeLong(v);
      }

      PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertThat(reader.readLong()).as("value at index " + i).isEqualTo(expected[i]);
      }
    }
  }

  // ---- Float scalar ----

  @Test
  public void testFloatScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      float[] expected = {
        0.0f,
        1.5f,
        -1.5f,
        Float.MIN_VALUE,
        Float.MAX_VALUE,
        Float.NaN,
        Float.POSITIVE_INFINITY,
        Float.NEGATIVE_INFINITY
      };
      for (float v : expected) {
        writer.writeFloat(v);
      }

      PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertThat(Float.floatToIntBits(reader.readFloat()))
            .as("value at index " + i)
            .isEqualTo(Float.floatToIntBits(expected[i]));
      }
    }
  }

  // ---- Double scalar ----

  @Test
  public void testDoubleScalarRoundTrip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      double[] expected = {
        0.0,
        1.5,
        -1.5,
        Double.MIN_VALUE,
        Double.MAX_VALUE,
        Double.NaN,
        Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY
      };
      for (double v : expected) {
        writer.writeDouble(v);
      }

      PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertThat(Double.doubleToLongBits(reader.readDouble()))
            .as("value at index " + i)
            .isEqualTo(Double.doubleToLongBits(expected[i]));
      }
    }
  }

  // ---- Skip ----

  @Test
  public void testIntegerSkipThenRead() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeInteger(1);
      writer.writeInteger(2);
      writer.writeInteger(3);
      writer.writeInteger(4);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(4, wrapForReading(writer));

      reader.skip(); // skip 1
      assertThat(reader.readInteger()).isEqualTo(2);
      reader.skip(1); // skip 3
      assertThat(reader.readInteger()).isEqualTo(4);
    }
  }

  @Test
  public void testLongSkip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeLong(100L);
      writer.writeLong(200L);
      writer.writeLong(300L);

      PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      reader.skip(2);
      assertThat(reader.readLong()).isEqualTo(300L);
    }
  }

  @Test
  public void testFloatSkip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);
      writer.writeFloat(3.0f);

      PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      reader.skip();
      assertThat(Float.floatToIntBits(reader.readFloat())).isEqualTo(Float.floatToIntBits(2.0f));
    }
  }

  @Test
  public void testDoubleSkip() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeDouble(1.0);
      writer.writeDouble(2.0);
      writer.writeDouble(3.0);

      PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
      reader.initFromPage(3, wrapForReading(writer));

      reader.skip();
      assertThat(Double.doubleToLongBits(reader.readDouble())).isEqualTo(Double.doubleToLongBits(2.0));
    }
  }

  // ---- Reset ----

  @Test
  public void testWriterReset() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      writer.writeInteger(999);
      writer.reset();
      assertThat(writer.getBufferedSize()).isZero();

      writer.writeInteger(42);

      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(1, wrapForReading(writer));

      assertThat(reader.readInteger()).isEqualTo(42);
    }
  }

  // ---- Empty page ----

  @Test
  public void testEmptyPage() throws IOException {
    try (PlainValuesWriter writer = newWriter()) {
      PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
      reader.initFromPage(0, wrapForReading(writer));
      // Should not throw — no values to read
    }
  }
}
