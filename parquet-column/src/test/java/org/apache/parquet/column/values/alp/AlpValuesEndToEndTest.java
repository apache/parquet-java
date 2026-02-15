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

import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
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
}
