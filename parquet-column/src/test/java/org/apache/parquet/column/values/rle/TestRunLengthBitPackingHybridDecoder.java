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
package org.apache.parquet.column.values.rle;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.Test;

public class TestRunLengthBitPackingHybridDecoder {

  @Test
  public void testRleRunWithZeroRepeatCount() {
    // An RLE run of 0 x 7 followed by a valid RLE run of 3 x 5
    RunLengthBitPackingHybridDecoder decoder = decoderFor(3, 0b0000_0000, 7, 0b0000_0110, 5);

    assertThatThrownBy(decoder::readInt)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid RLE run with a repeat count of 0");
  }

  @Test
  public void testBitPackedRunWithZeroGroups() {
    // A bit-packed run of 0 groups followed by a valid RLE run of 3 x 5
    RunLengthBitPackingHybridDecoder decoder = decoderFor(3, 0b0000_0001, 0b0000_0110, 5);

    assertThatThrownBy(decoder::readInt)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid bit-packed run with 0 groups of 8 values");
  }

  @Test
  public void testSkipRleRunWithZeroRepeatCount() {
    // Skipping must reject the empty run as well instead of silently moving on to the next one
    RunLengthBitPackingHybridDecoder decoder = decoderFor(3, 0b0000_0000, 7, 0b0000_0110, 5);

    assertThatThrownBy(() -> decoder.skipInts(1))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid RLE run with a repeat count of 0");
  }

  private static RunLengthBitPackingHybridDecoder decoderFor(int bitWidth, int... bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    for (int b : bytes) {
      buffer.put((byte) b);
    }
    buffer.flip();
    return new RunLengthBitPackingHybridDecoder(bitWidth, ByteBufferInputStream.wrap(buffer));
  }
}
