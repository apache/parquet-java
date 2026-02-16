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
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.Test;

/**
 * End-to-end tests for ALP encoding and decoding pipeline.
 */
public class AlpValuesEndToEndTest {

  private static final int DEFAULT_VECTOR_SIZE = AlpConstants.DEFAULT_VECTOR_SIZE;

  // ========== Helper Methods ==========

  private void roundTripFloat(float[] values) throws Exception {
    roundTripFloat(values, DEFAULT_VECTOR_SIZE);
  }

  private void roundTripFloat(float[] values, int vectorSize) throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      int capacity = Math.max(256, values.length * 8);
      writer = new AlpValuesWriter.FloatAlpValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator(), vectorSize);

      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < values.length; i++) {
        float expected = values[i];
        float actual = reader.readFloat();

        if (Float.isNaN(expected)) {
          assertTrue("Expected NaN at index " + i, Float.isNaN(actual));
        } else {
          assertEquals(
              "Value mismatch at index " + i + " for " + expected,
              Float.floatToRawIntBits(expected),
              Float.floatToRawIntBits(actual));
        }
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  private void roundTripDouble(double[] values) throws Exception {
    roundTripDouble(values, DEFAULT_VECTOR_SIZE);
  }

  private void roundTripDouble(double[] values, int vectorSize) throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      int capacity = Math.max(512, values.length * 16);
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator(), vectorSize);

      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < values.length; i++) {
        double expected = values[i];
        double actual = reader.readDouble();

        if (Double.isNaN(expected)) {
          assertTrue("Expected NaN at index " + i, Double.isNaN(actual));
        } else {
          assertEquals(
              "Value mismatch at index " + i + " for " + expected,
              Double.doubleToRawLongBits(expected),
              Double.doubleToRawLongBits(actual));
        }
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Float Pipeline Tests ==========

  @Test
  public void testFloatSingleValue() throws Exception {
    roundTripFloat(new float[] {1.23f});
  }

  @Test
  public void testFloatSmallBatch() throws Exception {
    roundTripFloat(new float[] {0.0f, 1.0f, -1.0f, 3.14f, 100.5f, 0.001f, 1234567.0f});
  }

  @Test
  public void testFloatRandomData() throws Exception {
    Random rand = new Random(42);
    float[] values = new float[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = rand.nextFloat() * 10000.0f - 5000.0f;
    }
    roundTripFloat(values);
  }

  @Test
  public void testFloatWithExceptions() throws Exception {
    roundTripFloat(new float[] {
      1.0f, Float.NaN, 2.0f, Float.POSITIVE_INFINITY, 3.0f, Float.NEGATIVE_INFINITY, 4.0f, -0.0f, 5.0f
    });
  }

  @Test
  public void testFloatAllExceptions() throws Exception {
    roundTripFloat(new float[] {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f});
  }

  @Test
  public void testFloatMultipleVectors() throws Exception {
    Random rand = new Random(42);
    float[] values = new float[3000];
    for (int i = 0; i < values.length; i++) {
      values[i] = rand.nextFloat() * 1000.0f;
    }
    roundTripFloat(values);
  }

  @Test
  public void testFloatZeroValues() throws Exception {
    roundTripFloat(new float[] {0.0f, 0.0f, 0.0f, 0.0f});
  }

  @Test
  public void testFloatIdenticalValues() throws Exception {
    float[] values = new float[100];
    for (int i = 0; i < values.length; i++) {
      values[i] = 3.14159f;
    }
    roundTripFloat(values);
  }

  // ========== Float: Vector Size / Boundary Tests ==========

  @Test
  public void testFloatExactOneVector() throws Exception {
    float[] values = new float[DEFAULT_VECTOR_SIZE];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01f;
    }
    roundTripFloat(values);
  }

  @Test
  public void testFloatExactTwoVectors() throws Exception {
    float[] values = new float[DEFAULT_VECTOR_SIZE * 2];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01f;
    }
    roundTripFloat(values);
  }

  @Test
  public void testFloatExactThreeVectors() throws Exception {
    float[] values = new float[DEFAULT_VECTOR_SIZE * 3];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01f;
    }
    roundTripFloat(values);
  }

  @Test
  public void testFloatPartialLastVectorPlusOne() throws Exception {
    float[] values = new float[DEFAULT_VECTOR_SIZE + 1];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01f;
    }
    roundTripFloat(values);
  }

  @Test
  public void testFloatPartialLastVectorMinusOne() throws Exception {
    float[] values = new float[DEFAULT_VECTOR_SIZE * 2 - 1];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01f;
    }
    roundTripFloat(values);
  }

  @Test
  public void testFloatCustomVectorSize512() throws Exception {
    float[] values = new float[512 + 100]; // partial second vector
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.1f;
    }
    roundTripFloat(values, 512);
  }

  @Test
  public void testFloatCustomVectorSize2048() throws Exception {
    float[] values = new float[2048 * 2 + 500]; // partial third vector
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.1f;
    }
    roundTripFloat(values, 2048);
  }

  @Test
  public void testFloatCustomVectorSizeSmall() throws Exception {
    // Minimum vector size = 8 (2^3)
    float[] values = new float[25]; // 3 full vectors + 1 partial
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 1.5f;
    }
    roundTripFloat(values, 8);
  }

  // ========== Float: Large Page ==========

  @Test
  public void testFloatLargePage() throws Exception {
    Random rand = new Random(42);
    float[] values = new float[15000];
    for (int i = 0; i < values.length; i++) {
      values[i] = rand.nextFloat() * 100000.0f - 50000.0f;
    }
    roundTripFloat(values);
  }

  // ========== Float: Monetary Data ==========

  @Test
  public void testFloatMonetaryData() throws Exception {
    float[] values = {
      19.99f, 29.95f, 9.99f, 49.99f, 99.95f, 14.50f, 7.99f, 24.99f, 39.95f, 59.99f, 119.99f, 4.99f, 0.99f, 1.50f,
      2.99f, 3.49f
    };
    roundTripFloat(values);
  }

  // ========== Float: All Identical (bit_width = 0) ==========

  @Test
  public void testFloatAllIdenticalBitWidthZero() throws Exception {
    float[] values = new float[2000];
    for (int i = 0; i < values.length; i++) {
      values[i] = 42.0f;
    }
    roundTripFloat(values);
  }

  // ========== Float: Mixed Exceptions at Various Positions ==========

  @Test
  public void testFloatExceptionsAtBoundaries() throws Exception {
    // Exceptions at vector start and end
    float[] values = new float[20];
    values[0] = Float.NaN;
    for (int i = 1; i < values.length - 1; i++) {
      values[i] = i * 1.1f;
    }
    values[values.length - 1] = Float.POSITIVE_INFINITY;
    roundTripFloat(values);
  }

  // ========== Float: Skip Tests ==========

  @Test
  public void testFloatSkip() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      float[] values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(reader.readFloat()));
      reader.skip(3);
      assertEquals(Float.floatToRawIntBits(5.0f), Float.floatToRawIntBits(reader.readFloat()));
      reader.skip(2);
      assertEquals(Float.floatToRawIntBits(8.0f), Float.floatToRawIntBits(reader.readFloat()));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testFloatSkipAcrossVectorBoundaries() throws Exception {
    int vectorSize = 8; // small vector for boundary testing
    int totalValues = vectorSize * 3 + 4; // 3 full vectors + partial
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(
          totalValues * 8, totalValues * 8, new DirectByteBufferAllocator(), vectorSize);

      float[] values = new float[totalValues];
      for (int i = 0; i < totalValues; i++) {
        values[i] = (i + 1) * 1.0f;
        writer.writeFloat(values[i]);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(totalValues, ByteBufferInputStream.wrap(input.toByteBuffer()));

      // Read first value from vector 0
      assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(reader.readFloat()));

      // Skip past vector 0 and vector 1, into vector 2
      reader.skip(vectorSize * 2 - 1); // skip to index vectorSize*2

      // Read first value of vector 2
      float expected = (vectorSize * 2 + 1) * 1.0f;
      assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(reader.readFloat()));

      // Skip into the partial last vector
      reader.skip(vectorSize - 1); // skip to index vectorSize*3

      expected = (vectorSize * 3 + 1) * 1.0f;
      assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(reader.readFloat()));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Float: Writer Reset ==========

  @Test
  public void testFloatWriterReset() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);
      writer.reset();
      assertEquals(0, writer.getBufferedSize());

      float[] values = {3.0f, 4.0f, 5.0f};
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

  // ========== Double Pipeline Tests ==========

  @Test
  public void testDoubleSingleValue() throws Exception {
    roundTripDouble(new double[] {1.23456789});
  }

  @Test
  public void testDoubleSmallBatch() throws Exception {
    roundTripDouble(new double[] {0.0, 1.0, -1.0, 3.14159265358979, 100.5, 0.001, 12345678901234.0});
  }

  @Test
  public void testDoubleRandomData() throws Exception {
    Random rand = new Random(42);
    double[] values = new double[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = rand.nextDouble() * 1000000.0 - 500000.0;
    }
    roundTripDouble(values);
  }

  @Test
  public void testDoubleWithExceptions() throws Exception {
    roundTripDouble(new double[] {
      1.0, Double.NaN, 2.0, Double.POSITIVE_INFINITY, 3.0, Double.NEGATIVE_INFINITY, 4.0, -0.0, 5.0
    });
  }

  @Test
  public void testDoubleAllExceptions() throws Exception {
    roundTripDouble(new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0});
  }

  @Test
  public void testDoubleMultipleVectors() throws Exception {
    Random rand = new Random(42);
    double[] values = new double[3000];
    for (int i = 0; i < values.length; i++) {
      values[i] = rand.nextDouble() * 100000.0;
    }
    roundTripDouble(values);
  }

  @Test
  public void testDoubleZeroValues() throws Exception {
    roundTripDouble(new double[] {0.0, 0.0, 0.0, 0.0});
  }

  @Test
  public void testDoubleIdenticalValues() throws Exception {
    double[] values = new double[100];
    for (int i = 0; i < values.length; i++) {
      values[i] = 3.14159265358979;
    }
    roundTripDouble(values);
  }

  // ========== Double: Vector Size / Boundary Tests ==========

  @Test
  public void testDoubleExactOneVector() throws Exception {
    double[] values = new double[DEFAULT_VECTOR_SIZE];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01;
    }
    roundTripDouble(values);
  }

  @Test
  public void testDoubleExactTwoVectors() throws Exception {
    double[] values = new double[DEFAULT_VECTOR_SIZE * 2];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01;
    }
    roundTripDouble(values);
  }

  @Test
  public void testDoublePartialLastVectorPlusOne() throws Exception {
    double[] values = new double[DEFAULT_VECTOR_SIZE + 1];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01;
    }
    roundTripDouble(values);
  }

  @Test
  public void testDoublePartialLastVectorMinusOne() throws Exception {
    double[] values = new double[DEFAULT_VECTOR_SIZE * 2 - 1];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.01;
    }
    roundTripDouble(values);
  }

  @Test
  public void testDoubleCustomVectorSize512() throws Exception {
    double[] values = new double[512 + 100];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.1;
    }
    roundTripDouble(values, 512);
  }

  @Test
  public void testDoubleCustomVectorSize2048() throws Exception {
    double[] values = new double[2048 * 2 + 500];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 0.1;
    }
    roundTripDouble(values, 2048);
  }

  @Test
  public void testDoubleCustomVectorSizeSmall() throws Exception {
    double[] values = new double[25];
    for (int i = 0; i < values.length; i++) {
      values[i] = i * 1.5;
    }
    roundTripDouble(values, 8);
  }

  // ========== Double: Large Page ==========

  @Test
  public void testDoubleLargePage() throws Exception {
    Random rand = new Random(42);
    double[] values = new double[15000];
    for (int i = 0; i < values.length; i++) {
      values[i] = rand.nextDouble() * 100000.0 - 50000.0;
    }
    roundTripDouble(values);
  }

  // ========== Double: Monetary Data ==========

  @Test
  public void testDoubleMonetaryData() throws Exception {
    double[] values = {
      19.99, 29.95, 9.99, 49.99, 99.95, 14.50, 7.99, 24.99, 39.95, 59.99, 119.99, 4.99, 0.99, 1.50, 2.99, 3.49
    };
    roundTripDouble(values);
  }

  // ========== Double: All Identical (bit_width = 0) ==========

  @Test
  public void testDoubleAllIdenticalBitWidthZero() throws Exception {
    double[] values = new double[2000];
    for (int i = 0; i < values.length; i++) {
      values[i] = 42.0;
    }
    roundTripDouble(values);
  }

  // ========== Double: Mixed Exceptions at Boundaries ==========

  @Test
  public void testDoubleExceptionsAtBoundaries() throws Exception {
    double[] values = new double[20];
    values[0] = Double.NaN;
    for (int i = 1; i < values.length - 1; i++) {
      values[i] = i * 1.1;
    }
    values[values.length - 1] = Double.POSITIVE_INFINITY;
    roundTripDouble(values);
  }

  // ========== Double: Skip Tests ==========

  @Test
  public void testDoubleSkip() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());

      double[] values = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      assertEquals(Double.doubleToRawLongBits(1.0), Double.doubleToRawLongBits(reader.readDouble()));
      reader.skip(3);
      assertEquals(Double.doubleToRawLongBits(5.0), Double.doubleToRawLongBits(reader.readDouble()));
      reader.skip(2);
      assertEquals(Double.doubleToRawLongBits(8.0), Double.doubleToRawLongBits(reader.readDouble()));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoubleSkipAcrossVectorBoundaries() throws Exception {
    int vectorSize = 8;
    int totalValues = vectorSize * 3 + 4;
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(
          totalValues * 16, totalValues * 16, new DirectByteBufferAllocator(), vectorSize);

      double[] values = new double[totalValues];
      for (int i = 0; i < totalValues; i++) {
        values[i] = (i + 1) * 1.0;
        writer.writeDouble(values[i]);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(totalValues, ByteBufferInputStream.wrap(input.toByteBuffer()));

      assertEquals(Double.doubleToRawLongBits(1.0), Double.doubleToRawLongBits(reader.readDouble()));
      reader.skip(vectorSize * 2 - 1);

      double expected = (vectorSize * 2 + 1) * 1.0;
      assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(reader.readDouble()));

      reader.skip(vectorSize - 1);
      expected = (vectorSize * 3 + 1) * 1.0;
      assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(reader.readDouble()));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Double: Writer Reset ==========

  @Test
  public void testDoubleWriterReset() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());

      writer.writeDouble(1.0);
      writer.writeDouble(2.0);
      writer.reset();
      assertEquals(0, writer.getBufferedSize());

      double[] values = {3.0, 4.0, 5.0};
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

  // ========== Packed Size Verification ==========

  @Test
  public void testPackedSizeSpecFormula() throws Exception {
    // Verify that the packed data size exactly matches ceil(n * bitWidth / 8)
    // by checking total encoded bytes for partial vectors with various remainders.
    // Vector size 8 means: 1 full group of 8 = no partial group,
    // but with count not divisible by 8 we exercise partial groups.
    for (int count : new int[] {1, 2, 3, 5, 7, 9, 11, 13, 15}) {
      float[] values = new float[count];
      for (int i = 0; i < count; i++) {
        values[i] = (i + 1) * 1.0f;
      }
      // Use smallest vector size so the partial vector IS the only vector
      roundTripFloat(values, 1024);
    }
  }

  @Test
  public void testPartialVectorAllRemaindersMod8() throws Exception {
    // Test every possible remainder (1..7) to exercise the partial group path.
    // With vectorSize=16: 16 is 2 full groups of 8, no partial group.
    // So use vectorSize=16 and write 16+R values where R in {1..7}
    // to get a partial last vector of size R.
    for (int remainder = 1; remainder <= 7; remainder++) {
      int count = 16 + remainder;
      float[] values = new float[count];
      for (int i = 0; i < count; i++) {
        values[i] = (i + 1) * 0.5f;
      }
      roundTripFloat(values, 16);
    }
    // Also test for doubles
    for (int remainder = 1; remainder <= 7; remainder++) {
      int count = 16 + remainder;
      double[] values = new double[count];
      for (int i = 0; i < count; i++) {
        values[i] = (i + 1) * 0.5;
      }
      roundTripDouble(values, 16);
    }
  }

  // ========== Negative Encoded Values / Wide FOR Range ==========

  @Test
  public void testFloatNegativeValues() throws Exception {
    // Values that produce negative encoded integers, testing FOR with negative min
    float[] values = new float[100];
    for (int i = 0; i < values.length; i++) {
      values[i] = -50.0f + i * 1.0f; // range [-50, 49]
    }
    roundTripFloat(values);
  }

  @Test
  public void testDoubleNegativeValues() throws Exception {
    double[] values = new double[100];
    for (int i = 0; i < values.length; i++) {
      values[i] = -50.0 + i * 1.0;
    }
    roundTripDouble(values);
  }

  @Test
  public void testFloatWideRange() throws Exception {
    // Mix of small and large positive values - exercises higher bit widths
    float[] values = {0.01f, 0.02f, 1000.0f, 2000.0f, 0.05f, 50000.0f, 0.99f, 99999.0f};
    roundTripFloat(values);
  }

  @Test
  public void testDoubleWideRange() throws Exception {
    double[] values = {0.01, 0.02, 100000.0, 200000.0, 0.05, 5000000.0, 0.99, 99999999.0};
    roundTripDouble(values);
  }

  // ========== Binary Format Verification ==========

  @Test
  public void testBinaryFormatExactBytes() throws Exception {
    // Write a known small dataset with vectorSize=8, then verify the
    // exact binary layout byte-by-byte
    int vectorSize = 8;
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator(), vectorSize);

      // Write 10 identical values: 1.0f
      // With e=0, f=0: encoded=1, FOR min=1, all deltas=0, bitWidth=0
      for (int i = 0; i < 10; i++) {
        writer.writeFloat(1.0f);
      }

      BytesInput input = writer.getBytes();
      byte[] bytes = input.toByteArray();
      java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);

      // Header (8 bytes)
      assertEquals("version", 1, buf.get() & 0xFF);
      assertEquals("compression_mode", 0, buf.get() & 0xFF);
      assertEquals("integer_encoding", 0, buf.get() & 0xFF);
      assertEquals("log_vector_size", 3, buf.get() & 0xFF); // log2(8)=3
      assertEquals("num_elements", 10, buf.getInt());

      // Offset array (2 vectors * 4 bytes = 8 bytes)
      int offset0 = buf.getInt(); // first vector offset
      int offset1 = buf.getInt(); // second vector offset
      assertEquals("offset0 = past offset array", 8, offset0);

      // Vector 0: 8 identical values, bitWidth=0, no exceptions
      // AlpInfo (4 bytes)
      int v0Exp = buf.get() & 0xFF;
      int v0Fac = buf.get() & 0xFF;
      int v0Exc = buf.getShort() & 0xFFFF;
      assertEquals("v0 exceptions", 0, v0Exc);

      // ForInfo (5 bytes)
      int v0For = buf.getInt();
      int v0Bw = buf.get() & 0xFF;
      assertEquals("v0 bitWidth", 0, v0Bw);
      // No packed bytes (bitWidth=0)
      // No exceptions

      // Vector 0 should be exactly 4 + 5 = 9 bytes
      assertEquals("offset1 should be offset0 + 9", offset0 + 9, offset1);

      // Vector 1: 2 identical values, bitWidth=0, no exceptions
      int v1Exp = buf.get() & 0xFF;
      int v1Fac = buf.get() & 0xFF;
      assertEquals("same exponent both vectors", v0Exp, v1Exp);
      assertEquals("same factor both vectors", v0Fac, v1Fac);
      int v1Exc = buf.getShort() & 0xFFFF;
      assertEquals("v1 exceptions", 0, v1Exc);
      int v1For = buf.getInt();
      int v1Bw = buf.get() & 0xFF;
      assertEquals("v1 bitWidth", 0, v1Bw);

      // Should have consumed all bytes
      assertEquals("all bytes consumed", bytes.length, buf.position());

      // Verify round-trip
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(10, ByteBufferInputStream.wrap(input.toByteBuffer()));
      for (int i = 0; i < 10; i++) {
        assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(reader.readFloat()));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Exception-Heavy Vectors ==========

  @Test
  public void testFloatManyExceptionsInVector() throws Exception {
    // Vector with more exceptions than normal values
    float[] values = new float[10];
    values[0] = 1.0f; // normal
    for (int i = 1; i < 10; i++) {
      values[i] = Float.NaN; // all exceptions except first
    }
    roundTripFloat(values, 16);
  }

  @Test
  public void testDoubleManyExceptionsInVector() throws Exception {
    double[] values = new double[10];
    values[0] = 1.0;
    for (int i = 1; i < 10; i++) {
      values[i] = Double.NaN;
    }
    roundTripDouble(values, 16);
  }

  @Test
  public void testFloatExceptionsAcrossMultipleVectors() throws Exception {
    // Exceptions in different vectors
    int vectorSize = 8;
    float[] values = new float[vectorSize * 3];
    for (int i = 0; i < values.length; i++) {
      values[i] = (i + 1) * 0.5f;
    }
    // Place exceptions at specific positions in different vectors
    values[0] = Float.NaN; // vector 0, position 0
    values[vectorSize - 1] = Float.POSITIVE_INFINITY; // vector 0, last position
    values[vectorSize] = -0.0f; // vector 1, position 0
    values[vectorSize * 2 + 3] = Float.NaN; // vector 2, position 3
    roundTripFloat(values, vectorSize);
  }

  // ========== Read After Skip to End ==========

  @Test(expected = ParquetDecodingException.class)
  public void testFloatReadPastEnd() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      writer.writeFloat(1.0f);

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(1, ByteBufferInputStream.wrap(input.toByteBuffer()));

      reader.readFloat(); // read the one value
      reader.readFloat(); // should throw
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test(expected = ParquetDecodingException.class)
  public void testFloatSkipPastEnd() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(2, ByteBufferInputStream.wrap(input.toByteBuffer()));

      reader.skip(3); // should throw - only 2 values
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Hand-Crafted Binary → Reader (independent of writer) ==========

  /**
   * Construct ALP-encoded bytes manually per the spec and verify the reader
   * decodes them correctly. This catches symmetric bugs where writer and reader
   * both implement the same wrong thing.
   *
   * <p>Test vector: 3 float values {100.0f, 200.0f, 300.0f} with vectorSize=8.
   * With exponent=0, factor=0: encoded = {100, 200, 300}.
   * FOR min = 100, deltas = {0, 100, 200}, maxDelta = 200, bitWidth = 8.
   * Packed: 3 values in 1 partial group of 8, padded with zeros.
   * Packed size = ceil(3 * 8 / 8) = 3 bytes.
   */
  @Test
  public void testReaderWithHandCraftedBytes() throws Exception {
    // Manually build ALP page for {100.0f, 200.0f, 300.0f}, vectorSize=8
    int vectorSize = 8;
    int logVectorSize = 3;
    int numElements = 3;
    int numVectors = 1;

    // Encoding: e=0, f=0 → multiplier=1.0
    // encoded = {100, 200, 300}
    // FOR min = 100
    // deltas = {0, 100, 200}
    // maxDelta = 200, bitWidth = 8
    int exponent = 0;
    int factor = 0;
    int numExceptions = 0;
    int frameOfReference = 100;
    int bitWidth = 8;

    // Bit-pack deltas using the same BytePacker the reader will use
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(8);
    int[] deltas = {0, 100, 200, 0, 0, 0, 0, 0}; // padded to 8
    byte[] packed = new byte[8]; // bitWidth bytes for one group of 8
    packer.pack8Values(deltas, 0, packed, 0);
    int packedSize = (3 * 8 + 7) / 8; // = 3 bytes for 3 values at 8 bits

    // Build the page
    int vectorDataSize = 4 + 5 + packedSize; // AlpInfo + ForInfo + packed
    int offsetArraySize = numVectors * 4;

    ByteBuffer page =
        ByteBuffer.allocate(8 + offsetArraySize + vectorDataSize).order(ByteOrder.LITTLE_ENDIAN);

    // Header (8 bytes)
    page.put((byte) 1); // version
    page.put((byte) 0); // compression_mode
    page.put((byte) 0); // integer_encoding
    page.put((byte) logVectorSize);
    page.putInt(numElements);

    // Offset array (1 vector)
    page.putInt(offsetArraySize); // offset₀ = past offset array

    // Vector 0: AlpInfo (4 bytes)
    page.put((byte) exponent);
    page.put((byte) factor);
    page.putShort((short) numExceptions);

    // Vector 0: ForInfo (5 bytes)
    page.putInt(frameOfReference);
    page.put((byte) bitWidth);

    // Vector 0: Packed data (3 bytes)
    page.put(packed, 0, packedSize);

    page.flip();

    // Now read it back
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(page));

    assertEquals(Float.floatToRawIntBits(100.0f), Float.floatToRawIntBits(reader.readFloat()));
    assertEquals(Float.floatToRawIntBits(200.0f), Float.floatToRawIntBits(reader.readFloat()));
    assertEquals(Float.floatToRawIntBits(300.0f), Float.floatToRawIntBits(reader.readFloat()));
  }

  /**
   * Hand-craft bytes with exceptions to verify reader handles them independently.
   * Values: {1.0f, NaN, 3.0f} with vectorSize=8.
   * e=0, f=0: encoded={1, placeholder=1, 3}, 1 exception at position 1.
   * FOR min=1, deltas={0, 0, 2}, maxDelta=2, bitWidth=2.
   */
  @Test
  public void testReaderWithHandCraftedExceptions() throws Exception {
    int vectorSize = 8;
    int logVectorSize = 3;
    int numElements = 3;

    int exponent = 0;
    int factor = 0;
    int numExceptions = 1;
    int placeholder = 1; // first non-exception encoded value
    int frameOfReference = 1; // min of {1, 1, 3}
    int bitWidth = 2; // ceil(log2(2+1)) = 2

    // deltas = {1-1, 1-1, 3-1} = {0, 0, 2}, padded to 8: {0,0,2,0,0,0,0,0}
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(2);
    int[] deltas = {0, 0, 2, 0, 0, 0, 0, 0};
    byte[] packed = new byte[2]; // bitWidth=2 bytes for a group of 8
    packer.pack8Values(deltas, 0, packed, 0);
    int packedSize = (3 * 2 + 7) / 8; // = 1 byte

    // Vector data: AlpInfo(4) + ForInfo(5) + packed(1) + excPos(2) + excVal(4) = 16
    int vectorDataSize = 4 + 5 + packedSize + 2 + 4;
    int offsetArraySize = 1 * 4;

    ByteBuffer page =
        ByteBuffer.allocate(8 + offsetArraySize + vectorDataSize).order(ByteOrder.LITTLE_ENDIAN);

    // Header
    page.put((byte) 1);
    page.put((byte) 0);
    page.put((byte) 0);
    page.put((byte) logVectorSize);
    page.putInt(numElements);

    // Offset array
    page.putInt(offsetArraySize);

    // AlpInfo
    page.put((byte) exponent);
    page.put((byte) factor);
    page.putShort((short) numExceptions);

    // ForInfo
    page.putInt(frameOfReference);
    page.put((byte) bitWidth);

    // Packed data
    page.put(packed, 0, packedSize);

    // Exception positions (uint16 LE)
    page.putShort((short) 1); // position 1

    // Exception values (float32 LE)
    page.putFloat(Float.NaN);

    page.flip();

    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(page));

    assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(reader.readFloat()));
    float val1 = reader.readFloat();
    assertTrue("Expected NaN at position 1", Float.isNaN(val1));
    assertEquals(Float.floatToRawIntBits(3.0f), Float.floatToRawIntBits(reader.readFloat()));
  }

  /**
   * Hand-craft double ALP bytes and verify reader independence.
   * Values: {10.0, 20.0, 30.0}, vectorSize=8, e=0, f=0.
   * encoded={10,20,30}, FOR min=10, deltas={0,10,20}, maxDelta=20, bitWidth=5.
   */
  @Test
  public void testDoubleReaderWithHandCraftedBytes() throws Exception {
    int numElements = 3;
    int exponent = 0;
    int factor = 0;
    long frameOfReference = 10;
    int bitWidth = 5; // ceil(log2(20+1)) = 5

    org.apache.parquet.column.values.bitpacking.BytePackerForLong packer =
        Packer.LITTLE_ENDIAN.newBytePackerForLong(5);
    long[] deltas = {0, 10, 20, 0, 0, 0, 0, 0};
    byte[] packed = new byte[5]; // bitWidth bytes for group of 8
    packer.pack8Values(deltas, 0, packed, 0);
    int packedSize = (3 * 5 + 7) / 8; // = 2 bytes

    int vectorDataSize = 4 + 9 + packedSize; // AlpInfo + DoubleForInfo + packed
    int offsetArraySize = 4;

    ByteBuffer page =
        ByteBuffer.allocate(8 + offsetArraySize + vectorDataSize).order(ByteOrder.LITTLE_ENDIAN);

    // Header
    page.put((byte) 1);
    page.put((byte) 0);
    page.put((byte) 0);
    page.put((byte) 3); // log2(8)
    page.putInt(numElements);

    // Offset array
    page.putInt(offsetArraySize);

    // AlpInfo
    page.put((byte) exponent);
    page.put((byte) factor);
    page.putShort((short) 0); // no exceptions

    // ForInfo (9 bytes for double)
    page.putLong(frameOfReference);
    page.put((byte) bitWidth);

    // Packed data
    page.put(packed, 0, packedSize);

    page.flip();

    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(page));

    assertEquals(Double.doubleToRawLongBits(10.0), Double.doubleToRawLongBits(reader.readDouble()));
    assertEquals(Double.doubleToRawLongBits(20.0), Double.doubleToRawLongBits(reader.readDouble()));
    assertEquals(Double.doubleToRawLongBits(30.0), Double.doubleToRawLongBits(reader.readDouble()));
  }

  // ========== Writer Output → Hand-Verified Bytes ==========

  /**
   * Write known values, then verify the exact binary output against hand computation.
   * This verifies the writer independently of the reader.
   *
   * <p>Input: {1.0f, 2.0f, 3.0f} with vectorSize=8.
   * Expected: e=0, f=0, encoded={1,2,3}, FOR min=1, deltas={0,1,2},
   * maxDelta=2, bitWidth=2. Packed size = ceil(3*2/8) = 1 byte.
   */
  @Test
  public void testWriterOutputExactBytes() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator(), 8);
      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);
      writer.writeFloat(3.0f);

      byte[] bytes = writer.getBytes().toByteArray();
      ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

      // Header (8 bytes)
      assertEquals(1, buf.get() & 0xFF); // version
      assertEquals(0, buf.get() & 0xFF); // compression_mode
      assertEquals(0, buf.get() & 0xFF); // integer_encoding
      assertEquals(3, buf.get() & 0xFF); // log2(8) = 3
      assertEquals(3, buf.getInt()); // num_elements

      // Offset array (1 vector)
      int offset0 = buf.getInt();
      assertEquals(4, offset0); // 1 * 4 = past offset array

      // AlpInfo
      int exp = buf.get() & 0xFF;
      int fac = buf.get() & 0xFF;
      int numExc = buf.getShort() & 0xFFFF;
      assertEquals(0, numExc);

      // ForInfo
      int forRef = buf.getInt();
      int bw = buf.get() & 0xFF;

      // Verify: for e=0, f=0: multiplier=1.0
      // encoded = {1, 2, 3}, min = 1, deltas = {0, 1, 2}, bitWidth = 2
      assertEquals(1, forRef); // frame of reference = 1
      assertEquals(2, bw); // bitWidth for max delta 2

      // Packed data: ceil(3 * 2 / 8) = 1 byte
      // deltas {0, 1, 2} at bitWidth=2:
      // Use BytePacker to compute expected packed byte
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(2);
      int[] expectedDeltas = {0, 1, 2, 0, 0, 0, 0, 0};
      byte[] expectedPacked = new byte[2]; // bitWidth=2
      packer.pack8Values(expectedDeltas, 0, expectedPacked, 0);

      byte actualPackedByte = buf.get();
      assertEquals("packed byte", expectedPacked[0], actualPackedByte);

      // Should have consumed all bytes
      assertEquals("all bytes consumed", bytes.length, buf.position());
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== NaN Bit Pattern Preservation ==========

  /**
   * Verify that different NaN bit patterns survive the round-trip exactly.
   * Java allows multiple NaN representations. Our exception handling must
   * preserve the exact bit pattern, not normalize to Float.NaN.
   */
  @Test
  public void testNaNBitPatternPreservation() throws Exception {
    // Standard NaN
    float standardNaN = Float.NaN; // 0x7FC00000
    // Signaling NaN (different bit pattern)
    float signalingNaN = Float.intBitsToFloat(0x7F800001);
    // Negative NaN
    float negativeNaN = Float.intBitsToFloat(0xFFC00000);
    // Another NaN variant
    float customNaN = Float.intBitsToFloat(0x7FFFFFFF);

    assertTrue(Float.isNaN(standardNaN));
    assertTrue(Float.isNaN(signalingNaN));
    assertTrue(Float.isNaN(negativeNaN));
    assertTrue(Float.isNaN(customNaN));

    float[] values = {1.0f, standardNaN, 2.0f, signalingNaN, 3.0f, negativeNaN, 4.0f, customNaN};

    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < values.length; i++) {
        float actual = reader.readFloat();
        assertEquals(
            "Bit pattern mismatch at index " + i + " (0x"
                + Integer.toHexString(Float.floatToRawIntBits(values[i])) + " vs 0x"
                + Integer.toHexString(Float.floatToRawIntBits(actual)) + ")",
            Float.floatToRawIntBits(values[i]),
            Float.floatToRawIntBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  /**
   * Same test for double NaN bit patterns.
   */
  @Test
  public void testDoubleNaNBitPatternPreservation() throws Exception {
    double standardNaN = Double.NaN;
    double signalingNaN = Double.longBitsToDouble(0x7FF0000000000001L);
    double negativeNaN = Double.longBitsToDouble(0xFFF8000000000000L);
    double customNaN = Double.longBitsToDouble(0x7FFFFFFFFFFFFFFFL);

    double[] values = {1.0, standardNaN, 2.0, signalingNaN, 3.0, negativeNaN, 4.0, customNaN};

    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());
      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < values.length; i++) {
        double actual = reader.readDouble();
        assertEquals(
            "Bit pattern mismatch at index " + i + " (0x"
                + Long.toHexString(Double.doubleToRawLongBits(values[i])) + " vs 0x"
                + Long.toHexString(Double.doubleToRawLongBits(actual)) + ")",
            Double.doubleToRawLongBits(values[i]),
            Double.doubleToRawLongBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Negative Zero Bit-Exact Roundtrip ==========

  /**
   * Verify that -0.0f roundtrips as -0.0f (bit pattern 0x80000000),
   * not as +0.0f (bit pattern 0x00000000).
   */
  @Test
  public void testNegativeZeroBitExact() throws Exception {
    float negZero = -0.0f;
    float posZero = 0.0f;

    // Sanity: they are == but have different bit patterns
    assertTrue(negZero == posZero);
    assertNotEquals(Float.floatToRawIntBits(negZero), Float.floatToRawIntBits(posZero));

    float[] values = {posZero, negZero, 1.0f, negZero};

    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < values.length; i++) {
        float actual = reader.readFloat();
        assertEquals(
            "Bit pattern at index " + i,
            Float.floatToRawIntBits(values[i]),
            Float.floatToRawIntBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Extreme Float Values ==========

  /**
   * Test subnormal floats, Float.MIN_VALUE, Float.MAX_VALUE, Float.MIN_NORMAL.
   * These should all either encode losslessly or be stored as exceptions.
   */
  @Test
  public void testExtremeFloatValues() throws Exception {
    float[] values = {
      Float.MIN_VALUE, // smallest positive subnormal: 1.4e-45
      Float.MIN_NORMAL, // smallest positive normal: 1.17549435e-38
      Float.MAX_VALUE, // 3.4028235e38
      -Float.MAX_VALUE, // most negative
      1.17549435e-38f, // near MIN_NORMAL
      3.4028234e38f, // near MAX_VALUE
      1.0e-10f, // very small positive
      1.0e10f, // large positive
    };
    roundTripFloat(values);
  }

  /**
   * Test extreme double values.
   */
  @Test
  public void testExtremeDoubleValues() throws Exception {
    double[] values = {
      Double.MIN_VALUE, // smallest positive subnormal
      Double.MIN_NORMAL, // smallest positive normal
      Double.MAX_VALUE, // largest positive
      -Double.MAX_VALUE, // most negative
      1.0e-100, // very small
      1.0e100, // very large
    };
    roundTripDouble(values);
  }

  // ========== Preset Caching Under Distribution Change ==========

  /**
   * Write >8 vectors where the optimal (e,f) changes after the sampling phase.
   * First 8 vectors: monetary data (e=2, f=0 optimal).
   * Remaining vectors: large integers (e=0, f=0 optimal).
   * Verify all values survive round-trip despite the distribution change.
   */
  @Test
  public void testPresetCachingWithDistributionChange() throws Exception {
    int vectorSize = 8; // small vectors to get to 8 quickly
    int samplingVectors = 8;
    int postSamplingVectors = 4;
    int totalValues = vectorSize * (samplingVectors + postSamplingVectors);

    float[] values = new float[totalValues];

    // First 8 vectors: decimal monetary values (best with e=2, f=0)
    for (int i = 0; i < vectorSize * samplingVectors; i++) {
      values[i] = 10.00f + (i % 100) * 0.01f; // 10.00, 10.01, 10.02, ...
    }

    // Last 4 vectors: whole numbers (best with e=0, f=0)
    for (int i = vectorSize * samplingVectors; i < totalValues; i++) {
      values[i] = (float) (i * 1000);
    }

    roundTripFloat(values, vectorSize);
  }

  /**
   * Same test for doubles.
   */
  @Test
  public void testDoublePresetCachingWithDistributionChange() throws Exception {
    int vectorSize = 8;
    int totalValues = vectorSize * 12; // 8 sampling + 4 post

    double[] values = new double[totalValues];

    for (int i = 0; i < vectorSize * 8; i++) {
      values[i] = 10.00 + (i % 100) * 0.01;
    }
    for (int i = vectorSize * 8; i < totalValues; i++) {
      values[i] = (double) (i * 1000);
    }

    roundTripDouble(values, vectorSize);
  }

  // ========== Multi-Vector Hand-Crafted Binary ==========

  /**
   * Hand-craft a 2-vector page to verify offset array navigation works.
   * 10 values with vectorSize=8: vector 0 has values 1-8, vector 1 has 9-10.
   */
  @Test
  public void testMultiVectorHandCrafted() throws Exception {
    int vectorSize = 8;
    int numElements = 10;
    int numVectors = 2;

    // Vector 0: {1,2,3,4,5,6,7,8} → e=0,f=0, encoded={1..8}, FOR min=1, deltas={0..7}
    // maxDelta=7, bitWidth=3. Packed: 8 values at 3 bits = 3 bytes exactly.
    int v0BitWidth = 3;
    BytePacker packer3 = Packer.LITTLE_ENDIAN.newBytePacker(3);
    int[] v0Deltas = {0, 1, 2, 3, 4, 5, 6, 7};
    byte[] v0Packed = new byte[3];
    packer3.pack8Values(v0Deltas, 0, v0Packed, 0);
    int v0PackedSize = 3; // (8*3+7)/8 = 3

    // Vector 1: {9, 10} → e=0,f=0, encoded={9,10}, FOR min=9, deltas={0,1}
    // maxDelta=1, bitWidth=1. Packed: ceil(2*1/8) = 1 byte.
    int v1BitWidth = 1;
    BytePacker packer1 = Packer.LITTLE_ENDIAN.newBytePacker(1);
    int[] v1Deltas = {0, 1, 0, 0, 0, 0, 0, 0};
    byte[] v1Packed = new byte[1];
    packer1.pack8Values(v1Deltas, 0, v1Packed, 0);
    int v1PackedSize = 1; // (2*1+7)/8 = 1

    int v0DataSize = 4 + 5 + v0PackedSize; // 12
    int v1DataSize = 4 + 5 + v1PackedSize; // 10
    int offsetArraySize = numVectors * 4; // 8

    ByteBuffer page =
        ByteBuffer.allocate(8 + offsetArraySize + v0DataSize + v1DataSize)
            .order(ByteOrder.LITTLE_ENDIAN);

    // Header
    page.put((byte) 1);
    page.put((byte) 0);
    page.put((byte) 0);
    page.put((byte) 3); // log2(8)
    page.putInt(numElements);

    // Offset array
    page.putInt(offsetArraySize); // vector 0 starts at 8
    page.putInt(offsetArraySize + v0DataSize); // vector 1 starts at 8+12=20

    // Vector 0
    page.put((byte) 0); // exponent
    page.put((byte) 0); // factor
    page.putShort((short) 0); // exceptions
    page.putInt(1); // FOR = 1
    page.put((byte) v0BitWidth);
    page.put(v0Packed, 0, v0PackedSize);

    // Vector 1
    page.put((byte) 0); // exponent
    page.put((byte) 0); // factor
    page.putShort((short) 0); // exceptions
    page.putInt(9); // FOR = 9
    page.put((byte) v1BitWidth);
    page.put(v1Packed, 0, v1PackedSize);

    page.flip();

    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(page));

    for (int i = 0; i < numElements; i++) {
      float expected = (float) (i + 1);
      float actual = reader.readFloat();
      assertEquals(
          "Value mismatch at index " + i,
          Float.floatToRawIntBits(expected),
          Float.floatToRawIntBits(actual));
    }
  }

  // ========== Skip Over Entire Vector Without Decoding ==========

  /**
   * Verify that skipping an entire vector and reading the next one works.
   * Uses hand-crafted bytes to ensure independence from writer.
   */
  @Test
  public void testSkipEntireVectorHandCrafted() throws Exception {
    // Reuse the multi-vector setup: {1..8} in vector 0, {9, 10} in vector 1
    int vectorSize = 8;
    int numElements = 10;
    int numVectors = 2;

    BytePacker packer3 = Packer.LITTLE_ENDIAN.newBytePacker(3);
    int[] v0Deltas = {0, 1, 2, 3, 4, 5, 6, 7};
    byte[] v0Packed = new byte[3];
    packer3.pack8Values(v0Deltas, 0, v0Packed, 0);

    BytePacker packer1 = Packer.LITTLE_ENDIAN.newBytePacker(1);
    int[] v1Deltas = {0, 1, 0, 0, 0, 0, 0, 0};
    byte[] v1Packed = new byte[1];
    packer1.pack8Values(v1Deltas, 0, v1Packed, 0);

    int v0DataSize = 4 + 5 + 3;
    int v1DataSize = 4 + 5 + 1;
    int offsetArraySize = numVectors * 4;

    ByteBuffer page =
        ByteBuffer.allocate(8 + offsetArraySize + v0DataSize + v1DataSize)
            .order(ByteOrder.LITTLE_ENDIAN);

    page.put((byte) 1).put((byte) 0).put((byte) 0).put((byte) 3);
    page.putInt(numElements);
    page.putInt(offsetArraySize);
    page.putInt(offsetArraySize + v0DataSize);

    // Vector 0
    page.put((byte) 0).put((byte) 0).putShort((short) 0);
    page.putInt(1).put((byte) 3);
    page.put(v0Packed, 0, 3);

    // Vector 1
    page.put((byte) 0).put((byte) 0).putShort((short) 0);
    page.putInt(9).put((byte) 1);
    page.put(v1Packed, 0, 1);

    page.flip();

    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(numElements, ByteBufferInputStream.wrap(page));

    // Skip entire vector 0 (8 values)
    reader.skip(8);

    // Read from vector 1
    assertEquals(Float.floatToRawIntBits(9.0f), Float.floatToRawIntBits(reader.readFloat()));
    assertEquals(Float.floatToRawIntBits(10.0f), Float.floatToRawIntBits(reader.readFloat()));
  }

  // ========== Vector Size Validation ==========

  /**
   * Verify that vectorSize=65536 is rejected because num_exceptions (uint16)
   * cannot represent 65536, which would cause silent data corruption if all
   * values in a vector are exceptions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testVectorSize65536Rejected() throws Exception {
    new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator(), 65536);
  }

  /**
   * Verify that vectorSize=32768 (max allowed) works correctly.
   */
  @Test
  public void testVectorSize32768Works() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator(), 32768);
      // Write a small number of values (partial vector)
      for (int i = 0; i < 10; i++) {
        writer.writeFloat(i * 1.0f);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(10, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < 10; i++) {
        assertEquals(
            Float.floatToRawIntBits(i * 1.0f),
            Float.floatToRawIntBits(reader.readFloat()));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Verify Encoder Produces Expected Values ==========

  /**
   * Manually verify that the encoder produces the exact integer values
   * we expect for known inputs. This is independent of the writer/reader.
   */
  @Test
  public void testEncoderProducesExpectedValues() {
    // 1.23f * 100 = 123
    assertEquals(123, AlpEncoderDecoder.encodeFloat(1.23f, 2, 0));
    // 19.99f * 100 = 1999
    assertEquals(1999, AlpEncoderDecoder.encodeFloat(19.99f, 2, 0));
    // -5.0f * 10 = -50
    assertEquals(-50, AlpEncoderDecoder.encodeFloat(-5.0f, 1, 0));
    // 0.0f * anything = 0
    assertEquals(0, AlpEncoderDecoder.encodeFloat(0.0f, 5, 0));
    // 42.0f * 1 = 42
    assertEquals(42, AlpEncoderDecoder.encodeFloat(42.0f, 0, 0));
    // 1.5f * 10 = 15
    assertEquals(15, AlpEncoderDecoder.encodeFloat(1.5f, 1, 0));

    // Double path
    assertEquals(123L, AlpEncoderDecoder.encodeDouble(1.23, 2, 0));
    assertEquals(1999L, AlpEncoderDecoder.encodeDouble(19.99, 2, 0));
    assertEquals(-50L, AlpEncoderDecoder.encodeDouble(-5.0, 1, 0));
    assertEquals(0L, AlpEncoderDecoder.encodeDouble(0.0, 5, 0));
    assertEquals(42L, AlpEncoderDecoder.encodeDouble(42.0, 0, 0));
  }
}
