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
package org.apache.parquet.column.values.bytestreamsplit;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.junit.Test;

public class ByteStreamSplitValuesEndToEndTest {

  @Test
  public void testFloatPipeline() throws Exception {
    // Generate random data.
    Random rand = new Random(1337);
    final int numElements = 1024;
    float[] values = new float[numElements];
    for (int i = 0; i < numElements; ++i) {
      float f = rand.nextFloat() * 4096.0f;
      values[i] = f;
    }

    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer = null;
    try {
      // Encode data.
      writer = new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
          numElements * 4, numElements * 4, new DirectByteBufferAllocator());
      for (float v : values) {
        writer.writeFloat(v);
      }

      assertEquals(numElements * 4, writer.getBufferedSize());
      BytesInput input = writer.getBytes();
      assertEquals(numElements * 4, input.size());

      ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();

      reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (float expectedValue : values) {
        float newValue = reader.readFloat();
        assertEquals(expectedValue, newValue, 0.0f);
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  @Test
  public void testDoublePipeline() throws Exception {
    // Generate random data.
    Random rand = new Random(18990);
    final int numElements = 1024;
    double[] values = new double[numElements];
    for (int i = 0; i < numElements; ++i) {
      double f = rand.nextDouble() * 16384.0;
      values[i] = f;
    }

    // Encode data.
    ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(
            numElements * 8, numElements * 8, new DirectByteBufferAllocator());
    for (double v : values) {
      writer.writeDouble(v);
    }

    assertEquals(numElements * 8, writer.getBufferedSize());
    BytesInput input = writer.getBytes();
    assertEquals(numElements * 8, input.size());

    ByteStreamSplitValuesReaderForDouble reader = new ByteStreamSplitValuesReaderForDouble();

    reader.initFromPage(numElements, ByteBufferInputStream.wrap(input.toByteBuffer()));

    for (double expectedValue : values) {
      double newValue = reader.readDouble();
      assertEquals(expectedValue, newValue, 0.0);
    }

    writer.reset();
    writer.close();
  }
}
