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

import static org.junit.Assert.assertEquals;

import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.junit.Test;

/**
 * End-to-end tests for ALP encoding and decoding pipeline.
 */
public class AlpValuesEndToEndTest {

  // ========== Float Pipeline Tests ==========

  @Test
  public void testFloatPipelineSingleValue() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(64, 64, new DirectByteBufferAllocator());

      float value = 1.23f;
      writer.writeFloat(value);

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(1, ByteBufferInputStream.wrap(input.toByteBuffer()));

      float decoded = reader.readFloat();
      assertEquals(Float.floatToRawIntBits(value), Float.floatToRawIntBits(decoded));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testFloatPipelineSmallBatch() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      float[] values = {0.0f, 1.0f, -1.0f, 3.14f, 100.5f, 0.001f, 1234567.0f};
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (float expected : values) {
        float actual = reader.readFloat();
        assertEquals(
            "Value mismatch for " + expected,
            Float.floatToRawIntBits(expected),
            Float.floatToRawIntBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testFloatPipelineRandomData() throws Exception {
    Random rand = new Random(42);
    final int numElements = 1024;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; ++i) {
      values[i] = rand.nextFloat() * 10000.0f - 5000.0f;
    }

    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(
          numElements * 8, numElements * 8, new DirectByteBufferAllocator());

      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (float expected : values) {
        float actual = reader.readFloat();
        assertEquals(
            "Value mismatch for " + expected,
            Float.floatToRawIntBits(expected),
            Float.floatToRawIntBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testFloatPipelineWithExceptions() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      // Mix of regular values and exception values
      float[] values = {
        1.0f, Float.NaN, 2.0f, Float.POSITIVE_INFINITY, 3.0f, Float.NEGATIVE_INFINITY, 4.0f, -0.0f, 5.0f
      };

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
          assertEquals("NaN check at index " + i, true, Float.isNaN(actual));
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

  @Test
  public void testFloatPipelineAllExceptions() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      // All exception values
      float[] values = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f};

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
          assertEquals("NaN check at index " + i, true, Float.isNaN(actual));
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

  @Test
  public void testFloatPipelineMultipleVectors() throws Exception {
    Random rand = new Random(42);
    // More than one vector (vector size is 1024)
    final int numElements = 3000;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; ++i) {
      values[i] = rand.nextFloat() * 1000.0f;
    }

    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(
          numElements * 8, numElements * 8, new DirectByteBufferAllocator());

      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < numElements; i++) {
        float expected = values[i];
        float actual = reader.readFloat();
        assertEquals(
            "Value mismatch at index " + i,
            Float.floatToRawIntBits(expected),
            Float.floatToRawIntBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testFloatReaderSkip() throws Exception {
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

      // Read first value
      assertEquals(Float.floatToRawIntBits(1.0f), Float.floatToRawIntBits(reader.readFloat()));

      // Skip 3 values
      reader.skip(3);

      // Should now read 5.0
      assertEquals(Float.floatToRawIntBits(5.0f), Float.floatToRawIntBits(reader.readFloat()));

      // Skip 2 values
      reader.skip(2);

      // Should now read 8.0
      assertEquals(Float.floatToRawIntBits(8.0f), Float.floatToRawIntBits(reader.readFloat()));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Double Pipeline Tests ==========

  @Test
  public void testDoublePipelineSingleValue() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(128, 128, new DirectByteBufferAllocator());

      double value = 1.23456789;
      writer.writeDouble(value);

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(1, ByteBufferInputStream.wrap(input.toByteBuffer()));

      double decoded = reader.readDouble();
      assertEquals(Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(decoded));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoublePipelineSmallBatch() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());

      double[] values = {0.0, 1.0, -1.0, 3.14159265358979, 100.5, 0.001, 12345678901234.0};
      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (double expected : values) {
        double actual = reader.readDouble();
        assertEquals(
            "Value mismatch for " + expected,
            Double.doubleToRawLongBits(expected),
            Double.doubleToRawLongBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoublePipelineRandomData() throws Exception {
    Random rand = new Random(42);
    final int numElements = 1024;
    double[] values = new double[numElements];
    for (int i = 0; i < numElements; ++i) {
      values[i] = rand.nextDouble() * 1000000.0 - 500000.0;
    }

    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(
          numElements * 16, numElements * 16, new DirectByteBufferAllocator());

      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (double expected : values) {
        double actual = reader.readDouble();
        assertEquals(
            "Value mismatch for " + expected,
            Double.doubleToRawLongBits(expected),
            Double.doubleToRawLongBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoublePipelineWithExceptions() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());

      // Mix of regular values and exception values
      double[] values = {
        1.0, Double.NaN, 2.0, Double.POSITIVE_INFINITY, 3.0, Double.NEGATIVE_INFINITY, 4.0, -0.0, 5.0
      };

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
          assertEquals("NaN check at index " + i, true, Double.isNaN(actual));
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

  @Test
  public void testDoublePipelineAllExceptions() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());

      // All exception values
      double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0};

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
          assertEquals("NaN check at index " + i, true, Double.isNaN(actual));
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

  @Test
  public void testDoublePipelineMultipleVectors() throws Exception {
    Random rand = new Random(42);
    // More than one vector (vector size is 1024)
    final int numElements = 3000;
    double[] values = new double[numElements];
    for (int i = 0; i < numElements; ++i) {
      values[i] = rand.nextDouble() * 100000.0;
    }

    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(
          numElements * 16, numElements * 16, new DirectByteBufferAllocator());

      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < numElements; i++) {
        double expected = values[i];
        double actual = reader.readDouble();
        assertEquals(
            "Value mismatch at index " + i,
            Double.doubleToRawLongBits(expected),
            Double.doubleToRawLongBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoubleReaderSkip() throws Exception {
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

      // Read first value
      assertEquals(Double.doubleToRawLongBits(1.0), Double.doubleToRawLongBits(reader.readDouble()));

      // Skip 3 values
      reader.skip(3);

      // Should now read 5.0
      assertEquals(Double.doubleToRawLongBits(5.0), Double.doubleToRawLongBits(reader.readDouble()));

      // Skip 2 values
      reader.skip(2);

      // Should now read 8.0
      assertEquals(Double.doubleToRawLongBits(8.0), Double.doubleToRawLongBits(reader.readDouble()));
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Writer Reset Tests ==========

  @Test
  public void testFloatWriterReset() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      // Write some values
      writer.writeFloat(1.0f);
      writer.writeFloat(2.0f);

      // Reset
      writer.reset();

      // Should be empty after reset
      assertEquals(0, writer.getBufferedSize());

      // Write new values
      float[] values = {3.0f, 4.0f, 5.0f};
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (float expected : values) {
        float actual = reader.readFloat();
        assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoubleWriterReset() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());

      // Write some values
      writer.writeDouble(1.0);
      writer.writeDouble(2.0);

      // Reset
      writer.reset();

      // Should be empty after reset
      assertEquals(0, writer.getBufferedSize());

      // Write new values
      double[] values = {3.0, 4.0, 5.0};
      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (double expected : values) {
        double actual = reader.readDouble();
        assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== Edge Case Tests ==========

  @Test
  public void testFloatZeroValues() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      float[] values = {0.0f, 0.0f, 0.0f, 0.0f};
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (float expected : values) {
        float actual = reader.readFloat();
        assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoubleZeroValues() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());

      double[] values = {0.0, 0.0, 0.0, 0.0};
      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (double expected : values) {
        double actual = reader.readDouble();
        assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testFloatIdenticalValues() throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator());

      float[] values = new float[100];
      for (int i = 0; i < values.length; i++) {
        values[i] = 3.14159f;
      }
      for (float v : values) {
        writer.writeFloat(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (float expected : values) {
        float actual = reader.readFloat();
        assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoubleIdenticalValues() throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(512, 512, new DirectByteBufferAllocator());

      double[] values = new double[100];
      for (int i = 0; i < values.length; i++) {
        values[i] = 3.14159265358979;
      }
      for (double v : values) {
        writer.writeDouble(v);
      }

      BytesInput input = writer.getBytes();
      AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (double expected : values) {
        double actual = reader.readDouble();
        assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(actual));
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }
}
