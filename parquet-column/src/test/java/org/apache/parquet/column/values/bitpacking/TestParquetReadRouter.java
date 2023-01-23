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
package org.apache.parquet.column.values.bitpacking;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;

public class TestParquetReadRouter {

  /**
   * The range of bitWidth is 1 ~ 32, change it directly if test other bitWidth.
   */
  private static final int bitWidth = 7;
  private static final int outputValues = 1024;
  private final byte[] input = new byte[outputValues * bitWidth / 8];
  private final int[] output = new int[outputValues];
  private final int[] outputBatch = new int[outputValues];
  private final int[] outputBatchVector = new int[outputValues];

  @Test
  public void testRead() throws IOException {
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) i;
    }
    ByteBufferInputStream inputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(input));

    ParquetReadRouter.read(bitWidth, inputStream, 0, output);
    ParquetReadRouter.readBatch(bitWidth, inputStream, 0, outputBatch);
    ParquetReadRouter.readBatchVector(bitWidth, inputStream, 0, outputBatchVector);
    assertArrayEquals(output, outputBatch);
    assertArrayEquals(output, outputBatchVector);
  }
}
