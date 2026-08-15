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
import org.apache.parquet.column.Encoding;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BooleanPlainValuesWriter} and {@link BooleanPlainValuesReader}
 * covering scalar round-trips, edge cases with partial bytes, and skip.
 */
public class TestBooleanPlainValuesWriterReader {

  private ByteBufferInputStream wrapForReading(BooleanPlainValuesWriter writer) throws IOException {
    byte[] bytes = writer.getBytes().toByteArray();
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes));
  }

  // ---- Encoding metadata ----

  @Test
  public void testEncoding() {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      assertThat(writer.getEncoding()).isEqualTo(Encoding.PLAIN);
    }
  }

  // ---- Scalar round-trip ----

  @Test
  public void testScalarRoundTrip() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] expected = {true, false, true, true, false, false, true, false};
      for (boolean v : expected) {
        writer.writeBoolean(v);
      }

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertThat(reader.readBoolean()).as("value at index " + i).isEqualTo(expected[i]);
      }
    }
  }

  // ---- Partial byte (< 8 booleans) ----

  @Test
  public void testPartialByte() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] expected = {true, false, true};
      for (boolean v : expected) {
        writer.writeBoolean(v);
      }

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(expected.length, wrapForReading(writer));

      for (int i = 0; i < expected.length; i++) {
        assertThat(reader.readBoolean()).as("value at index " + i).isEqualTo(expected[i]);
      }
    }
  }

  // ---- Skip ----

  @Test
  public void testSkip() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      boolean[] values = {true, false, true, false, true};
      for (boolean v : values) {
        writer.writeBoolean(v);
      }

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(values.length, wrapForReading(writer));

      reader.skip(); // skip true
      assertThat(reader.readBoolean()).isFalse();
      reader.skip(2); // skip true, false
      assertThat(reader.readBoolean()).isTrue();
    }
  }

  // ---- Single value ----

  @Test
  public void testSingleValue() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      writer.writeBoolean(true);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(1, wrapForReading(writer));

      assertThat(reader.readBoolean()).isTrue();
    }
  }

  // ---- Reset ----

  @Test
  public void testWriterReset() throws IOException {
    try (BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter()) {
      writer.writeBoolean(true);
      writer.writeBoolean(false);
      writer.reset();
      assertThat(writer.getBufferedSize()).isZero();

      writer.writeBoolean(false);
      writer.writeBoolean(true);

      BooleanPlainValuesReader reader = new BooleanPlainValuesReader();
      reader.initFromPage(2, wrapForReading(writer));

      assertThat(reader.readBoolean()).isFalse();
      assertThat(reader.readBoolean()).isTrue();
    }
  }
}
