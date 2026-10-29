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

import com.aayushatharva.brotli4j.decoder.Decoder;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.io.compress.CompressionInputStream;

class BrotliCompressionInputStream extends CompressionInputStream {

  private final byte[] decompressed;
  private int position;

  BrotliCompressionInputStream(InputStream in, int maxOutputSize) throws IOException {
    super(in);
    if (maxOutputSize < 0) {
      throw new IOException(
          "Invalid Brotli maximum output size: " + maxOutputSize + ". Expected a non-negative size.");
    }
    byte[] compressed = in.readAllBytes();
    if (compressed.length == 0) {
      this.decompressed = new byte[0];
      return;
    }
    // The native decoder requires a positive limit, including for an empty decoded stream.
    int outputLimit = Math.max(maxOutputSize, 1);
    try {
      // brotli4j's streaming BrotliInputStream fails on valid streams whose output crosses the
      // encoder window (hyperxpro/Brotli4j#267). Parquet feeds one page at a time, so use the
      // bounded one-shot decoder, which handles those streams without allowing unbounded output.
      byte[] decoded = Decoder.decompress(compressed, 0, compressed.length, outputLimit);
      if (decoded.length > maxOutputSize) {
        throw new IOException(
            "Brotli decompressed size " + decoded.length + " exceeds maximum size " + maxOutputSize);
      }
      this.decompressed = decoded;
    } catch (IOException | RuntimeException e) {
      throw new IOException("Failed to decompress Brotli stream", e);
    }
  }

  @Override
  public int read() {
    if (position >= decompressed.length) {
      return -1;
    }
    return decompressed[position++] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) {
    if (len == 0) {
      return 0;
    }
    if (position >= decompressed.length) {
      return -1;
    }
    int count = Math.min(len, decompressed.length - position);
    System.arraycopy(decompressed, position, b, off, count);
    position += count;
    return count;
  }

  @Override
  public int available() {
    return decompressed.length - position;
  }

  @Override
  public void resetState() throws IOException {
    throw new IOException("BrotliCompressionInputStream does not support resetState");
  }

  @Override
  public void seek(long pos) {
    if (pos < 0 || pos > decompressed.length) {
      throw new IllegalArgumentException("Invalid position: " + pos);
    }
    position = (int) pos;
  }

  @Override
  public long getPos() {
    return position;
  }
}
