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
import org.junit.Test;

/**
 * Tests to debug bit packing issues.
 */
public class AlpBitPackingTest {

  @Test
  public void testSimpleTwoFloats() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      // Write just two simple values
      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);

      BytesInput input = writer.getBytes();
      byte[] bytes = input.toByteArray();

      System.out.println("Encoded bytes length: " + bytes.length);
      System.out.println("Encoded bytes (hex):");
      for (int i = 0; i < bytes.length; i++) {
        System.out.printf("%02X ", bytes[i] & 0xFF);
        if ((i + 1) % 16 == 0) System.out.println();
      }
      System.out.println();

      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(2, ByteBufferInputStream.wrap(input.toByteBuffer()));

      float v1 = reader.readFloat();
      float v2 = reader.readFloat();

      System.out.println("Read v1: " + v1 + " (bits: " + Float.floatToRawIntBits(v1) + ")");
      System.out.println("Read v2: " + v2 + " (bits: " + Float.floatToRawIntBits(v2) + ")");

      assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(v1));
      assertEquals(Float.floatToRawIntBits(2.0f), Float.floatToRawIntBits(v2));
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

      // Write values including negative
      writer.writeFloat(1.0f);
      writer.writeFloat(-1.0f);
      writer.writeFloat(2.0f);

      BytesInput input = writer.getBytes();
      byte[] bytes = input.toByteArray();

      System.out.println("testThreeFloatsWithNegative - Encoded bytes (hex):");
      for (int i = 0; i < bytes.length; i++) {
        System.out.printf("%02X ", bytes[i] & 0xFF);
        if ((i + 1) % 16 == 0) System.out.println();
      }
      System.out.println();

      // Parse header manually to debug
      ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      int version = buf.get() & 0xFF;
      int compressionMode = buf.get() & 0xFF;
      int integerEncoding = buf.get() & 0xFF;
      int logVectorSize = buf.get() & 0xFF;
      int numElements = buf.getInt();

      System.out.println("  Header: version=" + version + ", numElements=" + numElements);

      // Parse AlpInfo
      int exponent = buf.get() & 0xFF;
      int factor = buf.get() & 0xFF;
      int numExceptions = buf.getShort() & 0xFFFF;

      System.out.println(
          "  AlpInfo: exponent=" + exponent + ", factor=" + factor + ", numExceptions=" + numExceptions);

      // Parse ForInfo
      int frameOfReference = buf.getInt();
      int bitWidth = buf.get() & 0xFF;

      System.out.println("  ForInfo: FOR=" + frameOfReference + ", bitWidth=" + bitWidth);

      // Show expected encoding
      float multiplier = 1.0f; // For exp=0, fac=0
      System.out.println("  Expected encoding (exp=" + exponent + ", fac=" + factor + "):");
      System.out.println("    1.0f -> " + Math.round(1.0f * multiplier));
      System.out.println("    -1.0f -> " + Math.round(-1.0f * multiplier));
      System.out.println("    2.0f -> " + Math.round(2.0f * multiplier));

      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(3, ByteBufferInputStream.wrap(input.toByteBuffer()));

      float v1 = reader.readFloat();
      float v2 = reader.readFloat();
      float v3 = reader.readFloat();

      System.out.println("testThreeFloatsWithNegative:");
      System.out.println("Read v1: " + v1 + " expected: 1.0");
      System.out.println("Read v2: " + v2 + " expected: -1.0");
      System.out.println("Read v3: " + v3 + " expected: 2.0");

      assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(v1));
      assertEquals(Float.floatToRawIntBits(-1.0f), Float.floatToRawIntBits(v2));
      assertEquals(Float.floatToRawIntBits(2.0f), Float.floatToRawIntBits(v3));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testEncoderDecoderDirectly() {
    // Test the encoder/decoder directly without bit packing
    float value = 1.0f;
    int exponent = 2;
    int factor = 0;

    // Check if it's an exception
    boolean isException = AlpEncoderDecoder.isFloatException(value, exponent, factor);
    System.out.println("Is 1.0f an exception with exp=2, fac=0? " + isException);

    if (!isException) {
      int encoded = AlpEncoderDecoder.encodeFloat(value, exponent, factor);
      float decoded = AlpEncoderDecoder.decodeFloat(encoded, exponent, factor);
      System.out.println("Encoded: " + encoded);
      System.out.println("Decoded: " + decoded);
      assertEquals(Float.floatToRawIntBits(value), Float.floatToRawIntBits(decoded));
    }
  }

  @Test
  public void testManualUnpack() throws Exception {
    // Manually test unpacking
    byte[] packedData = {0x02}; // Binary: 00000010
    ByteBuffer buf = ByteBuffer.wrap(packedData);

    int[] values = new int[2];
    int bitPos = 0;
    int bitWidth = 1;

    for (int i = 0; i < 2; i++) {
      int value = 0;
      int bitsToRead = bitWidth;
      int destBit = 0;

      while (bitsToRead > 0) {
        int byteIdx = bitPos / 8;
        int bitIdx = bitPos % 8;
        int bitsAvailable = 8 - bitIdx;
        int bitsThisRound = Math.min(bitsAvailable, bitsToRead);
        int mask = (1 << bitsThisRound) - 1;
        int bits = ((buf.get(byteIdx) & 0xFF) >>> bitIdx) & mask;
        value |= (bits << destBit);
        bitPos += bitsThisRound;
        destBit += bitsThisRound;
        bitsToRead -= bitsThisRound;
      }

      values[i] = value;
      System.out.println("Unpacked value[" + i + "] = " + value);
    }

    assertEquals(0, values[0]); // First bit of 0x02 = 0
    assertEquals(1, values[1]); // Second bit of 0x02 = 1
  }

  @Test
  public void testPackUnpackSymmetry() throws Exception {
    // Test that packing and unpacking are symmetric
    // Pack [2, 0, 3] with bitWidth=2, then unpack

    // Pack manually using the same algorithm as the writer
    int[] values = {2, 0, 3};
    int bitWidth = 2;
    int count = 3;

    // Pack (same as writer's packInts)
    int totalBits = count * bitWidth;
    int totalBytes = (totalBits + 7) / 8;
    byte[] packed = new byte[totalBytes];

    long bitBuffer = 0;
    int bitCount = 0;
    int bytePos = 0;

    for (int i = 0; i < count; i++) {
      bitBuffer |= ((long) values[i] << bitCount);
      bitCount += bitWidth;

      while (bitCount >= 8) {
        packed[bytePos++] = (byte) bitBuffer;
        bitBuffer >>>= 8;
        bitCount -= 8;
      }
    }

    // Write remaining bits
    if (bitCount > 0) {
      packed[bytePos] = (byte) bitBuffer;
    }

    System.out.println("testPackUnpackSymmetry:");
    System.out.println("  Input values: [" + values[0] + ", " + values[1] + ", " + values[2] + "]");
    System.out.println("  BitWidth: " + bitWidth);
    System.out.println("  Packed bytes: ");
    for (int i = 0; i < packed.length; i++) {
      System.out.printf(
          "    packed[%d] = 0x%02X = %s\n", i, packed[i] & 0xFF, Integer.toBinaryString(packed[i] & 0xFF));
    }

    // Unpack (same as reader's unpackInts)
    ByteBuffer buf = ByteBuffer.wrap(packed);
    int[] unpacked = new int[count];
    int bitPos = 0;

    for (int i = 0; i < count; i++) {
      int value = 0;
      int bitsToRead = bitWidth;
      int destBit = 0;

      while (bitsToRead > 0) {
        int byteIdx = bitPos / 8;
        int bitIdx = bitPos % 8;
        int bitsAvailable = 8 - bitIdx;
        int bitsThisRound = Math.min(bitsAvailable, bitsToRead);
        int mask = (1 << bitsThisRound) - 1;
        int bits = ((buf.get(byteIdx) & 0xFF) >>> bitIdx) & mask;
        value |= (bits << destBit);
        bitPos += bitsThisRound;
        destBit += bitsThisRound;
        bitsToRead -= bitsThisRound;
      }

      unpacked[i] = value;
    }

    System.out.println("  Unpacked values: [" + unpacked[0] + ", " + unpacked[1] + ", " + unpacked[2] + "]");

    for (int i = 0; i < count; i++) {
      assertEquals("Value at index " + i, values[i], unpacked[i]);
    }
  }

  @Test
  public void testDoubleRandomDebug() throws Exception {
    // Test with random doubles to debug the issue
    java.util.Random rand = new java.util.Random(42);
    final int numElements = 1024;
    double[] values = new double[numElements];
    for (int i = 0; i < numElements; ++i) {
      values[i] = rand.nextDouble() * 1000000.0 - 500000.0;
    }

    // Find which values will be exceptions
    AlpEncoderDecoder.EncodingParams params = AlpEncoderDecoder.findBestDoubleParams(values, 0, numElements);
    System.out.println("testDoubleRandomDebug:");
    System.out.println("  Best params: exponent=" + params.exponent + ", factor=" + params.factor);
    System.out.println("  Num exceptions: " + params.numExceptions + " out of " + numElements);

    // Encode value 28 manually to see what it should be
    double val28 = values[28];
    System.out.println("  Value at index 28: " + val28);
    if (!AlpEncoderDecoder.isDoubleException(val28, params.exponent, params.factor)) {
      long encoded28 = AlpEncoderDecoder.encodeDouble(val28, params.exponent, params.factor);
      System.out.println("    Encoded (before FOR): " + encoded28);
      double decoded28 = AlpEncoderDecoder.decodeDouble(encoded28, params.exponent, params.factor);
      System.out.println("    Decoded back: " + decoded28);
      System.out.println("    Round-trip OK? "
          + (Double.doubleToRawLongBits(val28) == Double.doubleToRawLongBits(decoded28)));
    }

    // Now test encode/decode
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(
          numElements * 16, numElements * 16, new DirectByteBufferAllocator());

      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      byte[] bytes = input.toByteArray();

      System.out.println("  Total encoded bytes: " + bytes.length);

      // Parse header
      ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      int version = buf.get() & 0xFF;
      int compressionMode = buf.get() & 0xFF;
      int integerEncoding = buf.get() & 0xFF;
      int logVectorSize = buf.get() & 0xFF;
      int numElem = buf.getInt();
      System.out.println("  Header: numElements=" + numElem);

      // Parse AlpInfo
      int exponent = buf.get() & 0xFF;
      int factor = buf.get() & 0xFF;
      int numExceptions = buf.getShort() & 0xFFFF;
      System.out.println(
          "  AlpInfo: exponent=" + exponent + ", factor=" + factor + ", numExceptions=" + numExceptions);

      // Parse ForInfo (9 bytes for double)
      long frameOfReference = buf.getLong();
      int bitWidth = buf.get() & 0xFF;
      System.out.println("  ForInfo: FOR=" + frameOfReference + ", bitWidth=" + bitWidth);

      // Calculate expected delta for value 28
      if (!AlpEncoderDecoder.isDoubleException(val28, params.exponent, params.factor)) {
        long encoded28 = AlpEncoderDecoder.encodeDouble(val28, params.exponent, params.factor);
        long delta28 = encoded28 - frameOfReference;
        System.out.println("    Expected delta for value 28: " + delta28);
        System.out.println("    Delta in hex: " + Long.toHexString(delta28));
      }

      // Now manually read the packed data for index 28
      int packedDataStart = 8 + 4 + 9; // Header + AlpInfo + ForInfo
      int bitStart = 28 * bitWidth;
      int byteStart = packedDataStart + (bitStart / 8);
      int bitOffset = bitStart % 8;
      System.out.println(
          "    Packed data for index 28 starts at byte " + byteStart + ", bit offset " + bitOffset);

      // Print the bytes around this position
      System.out.print("    Bytes around position: ");
      for (int i = byteStart; i < Math.min(byteStart + 10, bytes.length); i++) {
        System.out.printf("%02X ", bytes[i] & 0xFF);
      }
      System.out.println();

      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

      // Check first few values and find first mismatch
      int firstMismatch = -1;
      for (int i = 0; i < numElements; i++) {
        double expected = values[i];
        double actual = reader.readDouble();
        if (Double.doubleToRawLongBits(expected) != Double.doubleToRawLongBits(actual)) {
          System.out.println("  First mismatch at index " + i + ":");
          System.out.println(
              "    Expected: " + expected + " (bits: " + Double.doubleToRawLongBits(expected) + ")");
          System.out.println("    Actual: " + actual + " (bits: " + Double.doubleToRawLongBits(actual) + ")");
          System.out.println(
              "    Is exception? " + AlpEncoderDecoder.isDoubleException(expected, exponent, factor));
          firstMismatch = i;
          break;
        }
      }

      if (firstMismatch == -1) {
        System.out.println("  All values matched!");
      }

    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testHeaderParsing() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);

      BytesInput input = writer.getBytes();
      byte[] bytes = input.toByteArray();

      // Parse header manually
      ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      int version = buf.get() & 0xFF;
      int compressionMode = buf.get() & 0xFF;
      int integerEncoding = buf.get() & 0xFF;
      int logVectorSize = buf.get() & 0xFF;
      int numElements = buf.getInt();

      System.out.println("Header parsing:");
      System.out.println("  version: " + version);
      System.out.println("  compressionMode: " + compressionMode);
      System.out.println("  integerEncoding: " + integerEncoding);
      System.out.println("  logVectorSize: " + logVectorSize);
      System.out.println("  numElements: " + numElements);

      assertEquals(1, version);
      assertEquals(0, compressionMode);
      assertEquals(0, integerEncoding);
      assertEquals(10, logVectorSize);
      assertEquals(2, numElements);

      // Parse AlpInfo
      int exponent = buf.get() & 0xFF;
      int factor = buf.get() & 0xFF;
      int numExceptions = buf.getShort() & 0xFFFF;

      System.out.println("AlpInfo:");
      System.out.println("  exponent: " + exponent);
      System.out.println("  factor: " + factor);
      System.out.println("  numExceptions: " + numExceptions);

      // Parse ForInfo
      int frameOfReference = buf.getInt();
      int bitWidth = buf.get() & 0xFF;

      System.out.println("ForInfo:");
      System.out.println("  frameOfReference: " + frameOfReference);
      System.out.println("  bitWidth: " + bitWidth);

      // Calculate expected packed data size
      int vectorLen = 2;
      int packedBytes = (vectorLen * bitWidth + 7) / 8;
      System.out.println("Expected packed bytes: " + packedBytes);

    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }
}
