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

import io.airlift.compress.v3.lz4.Lz4Decompressor;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.compress.DirectDecompressor;

public class Lz4RawDecompressor extends NonBlockedDecompressor implements DirectDecompressor {

  private final Lz4Decompressor decompressor = Lz4Decompressor.create();

  /** Reused for direct buffers; lazily allocated and grown when needed. */
  private byte[] inputBuf;
  /** Reused for direct buffers; lazily allocated and grown when needed. */
  private byte[] outputBuf;

  @Override
  protected int maxUncompressedLength(ByteBuffer compressed, int maxUncompressedLength) throws IOException {
    // We cannot obtain the precise uncompressed length from the input data.
    // Simply return the maxUncompressedLength.
    return maxUncompressedLength;
  }

  @Override
  protected int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
    int startPos = uncompressed.position();
    int compressedLen = compressed.remaining();
    int maxOut = uncompressed.remaining();

    final int uncompressedSize;
    if (compressed.hasArray() && uncompressed.hasArray()) {
      int inputOffset = compressed.arrayOffset() + compressed.position();
      int outputOffset = uncompressed.arrayOffset() + uncompressed.position();
      uncompressedSize = decompressor.decompress(
          compressed.array(), inputOffset, compressedLen,
          uncompressed.array(), outputOffset, maxOut);
      // Advance positions to match the direct-buffer path (where get/put do this)
      compressed.position(compressed.position() + compressedLen);
    } else {
      if (inputBuf == null || inputBuf.length < compressedLen) {
        inputBuf = new byte[compressedLen];
      }
      if (outputBuf == null || outputBuf.length < maxOut) {
        outputBuf = new byte[maxOut];
      }
      compressed.get(inputBuf, 0, compressedLen);
      uncompressedSize = decompressor.decompress(
          inputBuf, 0, compressedLen, outputBuf, 0, maxOut);
      uncompressed.put(outputBuf, 0, uncompressedSize);
    }

    uncompressed.limit(startPos + uncompressedSize);
    uncompressed.position(startPos);
    return uncompressedSize;
  }

  @Override
  public void decompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
    uncompress(compressed, uncompressed);
  }
}
