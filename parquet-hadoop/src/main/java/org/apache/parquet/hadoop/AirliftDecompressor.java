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
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

public class AirliftDecompressor extends CodecFactory.BytesDecompressor {
  private CompressionCodec codec;
  private final Decompressor decompressor;

  public AirliftDecompressor(CompressionCodec codec) {
    this.codec = codec;
    decompressor = codec.createDecompressor();
  }

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    decompressor.reset();
    InputStream is = codec.createInputStream(bytes.toInputStream(), decompressor);
    return BytesInput.from(is, uncompressedSize);
  }

  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
    throws IOException {
    ByteBuffer decompressed = decompress(BytesInput.from(input), uncompressedSize).toByteBuffer();
    output.put(decompressed);
  }

  @Override
  public void release() {}
}

