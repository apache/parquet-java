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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.xerial.snappy.Snappy;

public class SnappyDecompressor extends NonBlockedDecompressor implements DirectDecompressor {

  /**
   * One-shot decompression straight between two buffers (both must be direct; xerial Snappy
   * requires it), bypassing the setInput()/decompress() accumulation this class otherwise uses
   * to adapt to the streaming {@link org.apache.hadoop.io.compress.Decompressor} API.
   *
   * <p>Validates the block's own declared uncompressed length against the output buffer's
   * capacity before decompressing, rather than relying solely on the resolved snappy-java
   * version's internal bounds checking (older snappy-java releases have shipped without it).
   */
  @Override
  public void decompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
    int declaredLength = Snappy.uncompressedLength(compressed);
    if (declaredLength > uncompressed.remaining()) {
      throw new IOException("Snappy block declares an uncompressed length of " + declaredLength
          + " bytes, which does not fit in the " + uncompressed.remaining() + "-byte output buffer");
    }
    uncompress(compressed, uncompressed);
  }

  @Override
  protected int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
    return Snappy.uncompress(compressed, uncompressed);
  }

  @Override
  protected int maxUncompressedLength(ByteBuffer compressed, int maxUncompressedLength) throws IOException {
    return Snappy.uncompressedLength(compressed);
  }
} // class SnappyDecompressor
