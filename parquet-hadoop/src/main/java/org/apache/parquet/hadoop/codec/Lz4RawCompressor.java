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
package org.apache.parquet.hadoop.codec;

import io.airlift.compress.v3.lz4.Lz4Compressor;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Lz4RawCompressor extends NonBlockedCompressor {

  private final Lz4Compressor compressor = Lz4Compressor.create();

  /** Reused for direct buffers; lazily allocated and grown when needed. */
  private byte[] inputBuf;
  /** Reused for direct buffers; lazily allocated and grown when needed. */
  private byte[] outputBuf;

  @Override
  protected int maxCompressedLength(int byteSize) {
    return compressor.maxCompressedLength(byteSize);
  }

  @Override
  protected int compress(ByteBuffer uncompressed, ByteBuffer compressed) throws IOException {
    int startPos = compressed.position();
    int inputLen = uncompressed.remaining();
    int maxOut = compressor.maxCompressedLength(inputLen);

    final int compressedSize;
    if (uncompressed.hasArray() && compressed.hasArray()) {
      int inputOffset = uncompressed.arrayOffset() + uncompressed.position();
      int outputOffset = compressed.arrayOffset() + compressed.position();
      compressedSize = compressor.compress(
          uncompressed.array(), inputOffset, inputLen,
          compressed.array(), outputOffset, maxOut);
      // Advance positions to match the direct-buffer path (where get/put do this)
      uncompressed.position(uncompressed.position() + inputLen);
    } else {
      if (inputBuf == null || inputBuf.length < inputLen) {
        inputBuf = new byte[inputLen];
      }
      if (outputBuf == null || outputBuf.length < maxOut) {
        outputBuf = new byte[maxOut];
      }
      uncompressed.get(inputBuf, 0, inputLen);
      compressedSize = compressor.compress(inputBuf, 0, inputLen, outputBuf, 0, maxOut);
      compressed.put(outputBuf, 0, compressedSize);
    }

    compressed.limit(startPos + compressedSize);
    compressed.position(startPos);
    return compressedSize;
  }
}
