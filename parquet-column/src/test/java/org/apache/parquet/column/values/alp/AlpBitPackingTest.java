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
package org.apache.parquet.column.values.alp;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.junit.Test;

/**
 * Tests for bit-packing behavior in the ALP encoding pipeline.
 */
public class AlpBitPackingTest {

  @Test
  public void testBytePackerIntRoundTrip() {
    // Verify BytePacker pack/unpack round-trip for various bit widths
    for (int bitWidth = 1; bitWidth <= 31; bitWidth++) {
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
      int maxVal = (int) Math.min((1L << bitWidth) - 1, Integer.MAX_VALUE);

      int[] input = new int[8];
      for (int i = 0; i < 8; i++) {
        input[i] = (maxVal / 8) * i;
      }

      byte[] packed = new byte[bitWidth];
      packer.pack8Values(input, 0, packed, 0);

      int[] unpacked = new int[8];
      ByteBuffer buf = ByteBuffer.wrap(packed);
      packer.unpack8Values(buf, 0, unpacked, 0);

      for (int i = 0; i < 8; i++) {
        assertEquals("BitWidth=" + bitWidth + " index=" + i, input[i], unpacked[i]);
      }
    }
  }

  @Test
  public void testBytePackerForLongRoundTrip() {
    // Verify BytePackerForLong pack/unpack round-trip for various bit widths
    for (int bitWidth = 1; bitWidth <= 63; bitWidth++) {
      BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
      long maxVal = (bitWidth == 63) ? Long.MAX_VALUE : (1L << bitWidth) - 1;

      long[] input = new long[8];
      for (int i = 0; i < 8; i++) {
        input[i] = (maxVal / 8) * i;
      }

      byte[] packed = new byte[bitWidth];
      packer.pack8Values(input, 0, packed, 0);

      long[] unpacked = new long[8];
      ByteBuffer buf = ByteBuffer.wrap(packed);
      packer.unpack8Values(buf, 0, unpacked, 0);

      for (int i = 0; i < 8; i++) {
        assertEquals("BitWidth=" + bitWidth + " index=" + i, input[i], unpacked[i]);
      }
    }
  }

  @Test
  public void testSimpleTwoFloats() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(2, ByteBufferInputStream.wrap(input.toByteBuffer()));

      assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(reader.readFloat()));
      assertEquals(Float.floatToRawIntBits(2.0f), Float.floatToRawIntBits(reader.readFloat()));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testThreeFloatsWithNegative() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      writer.writeFloat(1.0f);
      writer.writeFloat(-1.0f);
      writer.writeFloat(2.0f);

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(3, ByteBufferInputStream.wrap(input.toByteBuffer()));

      assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(reader.readFloat()));
      assertEquals(Float.floatToRawIntBits(-1.0f), Float.floatToRawIntBits(reader.readFloat()));
      assertEquals(Float.floatToRawIntBits(2.0f), Float.floatToRawIntBits(reader.readFloat()));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testEncoderDecoderDirectly() {
    float value = 1.0f;
    int exponent = 2;
    int factor = 0;

    assertFalse(AlpEncoderDecoder.isFloatException(value, exponent, factor));
    int encoded = AlpEncoderDecoder.encodeFloat(value, exponent, factor);
    float decoded = AlpEncoderDecoder.decodeFloat(encoded, exponent, factor);
    assertEquals(Float.floatToRawIntBits(value), Float.floatToRawIntBits(decoded));
  }

  @Test
  public void testHeaderFormatValidation() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);

      BytesInput input = writer.getBytes();
      byte[] bytes = input.toByteArray();

      // Parse and validate header
      ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      int version = buf.get() & 0xFF;
      int compressionMode = buf.get() & 0xFF;
      int integerEncoding = buf.get() & 0xFF;
      int logVectorSize = buf.get() & 0xFF;
      int numElements = buf.getInt();

      assertEquals(AlpConstants.ALP_VERSION, version);
      assertEquals(AlpConstants.ALP_COMPRESSION_MODE, compressionMode);
      assertEquals(AlpConstants.ALP_INTEGER_ENCODING_FOR, integerEncoding);
      assertEquals(AlpConstants.DEFAULT_VECTOR_SIZE_LOG, logVectorSize);
      assertEquals(2, numElements);

      // Verify offset array follows header (1 vector = 1 offset entry)
      int offset0 = buf.getInt();
      // First vector starts right after the offset array (1 vector * 4 bytes = 4)
      assertEquals(Integer.BYTES, offset0);
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testPartialGroupPacking() throws Exception {
    // Test with fewer than 8 values to exercise partial group packing/unpacking
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      float[] values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (float expected : values) {
        assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(reader.readFloat()));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testBitWidthZeroPacking() throws Exception {
    // All identical values should result in bitWidth=0 (no packed data)
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      for (int i = 0; i < 10; i++) {
        writer.writeFloat(5.0f);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(10, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < 10; i++) {
        assertEquals(Float.floatToRawIntBits(5.0f), Float.floatToRawIntBits(reader.readFloat()));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoublePackingRoundTrip() throws Exception {
    // Verify double packing round-trip with varying values
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());
      double[] values = {1.0, -1.0, 2.0, 100.5, 0.0, 3.14, 42.0, 99.99, 0.001, 7.77};
      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (double expected : values) {
        assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(reader.readDouble()));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testExactEightValues() throws Exception {
    // Exactly 8 values = one full packing group, no partial group
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      float[] values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (float expected : values) {
        assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(reader.readFloat()));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }
}
