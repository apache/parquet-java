/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Factory for creating (and potentially caching) {@link BytesInputCompressor} and {@link BytesInputDecompressor}
 * instances to compress/decompress page data.
 * <p>
 * The factory instance shall be released after use. The compressor/decompressor instances shall not be used after
 * release.
 *
 * @see #release()
 */
public interface CompressionCodecFactory {

  /**
   * Returns a {@link BytesInputCompressor} instance for the specified codec name to be used for compressing page data.
   * <p>
   * The compressor is not thread-safe, so one instance for each working thread is required.
   *
   * @param codecName the codec name which the compressor instance is to be returned
   * @return the compressor instance for the specified codec name
   * @see BytesInputCompressor#release()
   */
  BytesInputCompressor getCompressor(CompressionCodecName codecName);

  /**
   * Returns a {@link BytesInputDecompressor} instance for the specified codec name to be used for decompressing page
   * data.
   * <p>
   * The decompressor is not thread-safe, so one instance for each working thread is required.
   *
   * @param codecName the codec name which the decompressor instance is to be returned
   * @return the decompressor instance for the specified codec name
   * @see BytesInputDecompressor#release()
   */
  BytesInputDecompressor getDecompressor(CompressionCodecName codecName);

  /**
   * Releasing this factory instance.
   * <p>
   * Each compressor/decompressor instance shall be released before invoking this. Nor the compressor/decompressor
   * instances retrieved from this factory nor this factory instance itself shall be used after release.
   *
   * @see BytesInputCompressor#release()
   * @see BytesInputDecompressor#release()
   */
  void release();

  /**
   * Compressor instance of a specific codec to be used for compressing page data.
   * <p>
   * This compressor shall be released after use. This compressor shall not be used after release.
   *
   * @see #release()
   */
  interface BytesInputCompressor {

    /**
     * Compresses the specified {@link BytesInput} data and returns it as {@link BytesInput}.
     * <p>
     * Depending on the implementation {@code bytes} might be completely consumed. The returned {@link BytesInput}
     * instance needs to be consumed before using this compressor again. This is because the implementation might use
     * its internal buffer to directly provide the returned {@link BytesInput} instance.
     *
     * @param bytes the page data to be compressed
     * @return a {@link BytesInput} containing the compressed data. Needs to be consumed before using this compressor
     * again.
     * @throws IOException if any I/O error occurs during the compression
     */
    BytesInput compress(BytesInput bytes) throws IOException;

    /**
     * Returns the codec name of this compressor.
     *
     * @return the codec name
     */
    CompressionCodecName getCodecName();

    /**
     * Releases this compressor instance.
     * <p>
     * No subsequent calls on this instance nor the returned {@link BytesInput} instance returned by
     * {@link #compress(BytesInput)} shall be used after release.
     */
    void release();
  }

  /**
   * Decompressor instance of a specific codec to be used for decompressing page data.
   * <p>
   * This decompressor shall be released after use. This decompressor shall not be used after release.
   *
   * @see #release()
   */
  interface BytesInputDecompressor {

    /**
     * Decompresses the specified {@link BytesInput} data and returns it as {@link BytesInput}.
     * <p>
     * The decompressed data must have the size specified. Depending on the implementation {@code bytes} might be
     * completely consumed. The returned {@link BytesInput} instance needs to be consumed before using this decompressor
     * again. This is because the implementation might use its internal buffer to directly provide the returned
     * {@link BytesInput} instance.
     *
     * @param bytes            the page data to be decompressed
     * @param decompressedSize the exact size of the decompressed data
     * @return a {@link BytesInput} containing the decompressed data. Needs to be consumed before using this
     * decompressor again.
     * @throws IOException if any I/O error occurs during the decompression
     */
    BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException;

    /**
     * Decompresses {@code compressedSize} bytes from {@code input} from the current position. The decompressed bytes is
     * to be written int {@code output} from its current position. The decompressed data must have the size specified.
     * <p>
     * {@code output} must have the available bytes of {@code decompressedSize}. According to the {@link ByteBuffer}
     * contract the position of {@code input} will be increased by {@code compressedSize}, and the position of
     * {@code output} will be increased by {@code decompressedSize}. (It means, one would have to flip the output buffer
     * before reading the decompressed data from it.)
     *
     * @param input            the input buffer where the data is to be decompressed from
     * @param compressedSize   the exact size of the compressed (input) data
     * @param output           the output buffer where the data is to be decompressed into
     * @param decompressedSize the exact size of the decompressed (output) data
     * @throws IOException if any I/O error occurs during the decompression
     */
    void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException;

    /**
     * Releases this decompressor instance. No subsequent calls on this instance nor the returned {@link BytesInput}
     * instance returned by {@link #decompress(BytesInput, int)} shall be used after release.
     */
    void release();
  }
}
