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

import io.airlift.compress.gzip.JdkGzipCodec;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.lzo.LzoCodec;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class AirliftCompressorCodecFactory implements CompressionCodecFactory {

  private final int pageSize;

  AirliftCompressorCodecFactory(int pageSize) {
    this.pageSize = pageSize;
  }

  @Override
  public CodecFactory.BytesCompressor getCompressor(CompressionCodecName codecName) {
    switch (codecName.getParquetCompressionCodec()) {
      case GZIP:
        return new AirliftCompressor(codecName, new JdkGzipCodec(), pageSize);
      case LZO:
        return new AirliftCompressor(codecName, new LzoCodec(), pageSize);
      case LZ4:
        return new AirliftCompressor(codecName, new Lz4Codec(), pageSize);
      default:
        throw new IllegalArgumentException("Codec not supported in AirliftCompressorCodecFactory: " + codecName);
    }
  }

  @Override
  public CodecFactory.BytesDecompressor getDecompressor(CompressionCodecName codecName) {
    switch (codecName.getParquetCompressionCodec()) {
      case GZIP:
        return new AirliftDecompressor(new JdkGzipCodec());
      case LZO:
        return new AirliftDecompressor(new LzoCodec());
      case LZ4:
        return new AirliftDecompressor(new Lz4Codec());
      default:
        throw new IllegalArgumentException("Codec not supported in AirliftCompressorCodecFactory: " + codecName);
    }
  }

  @Override
  public void release() {}

  @Override
  public CompressionCodecFactory withAirliftCompressors(boolean useAirliftCompressors) {
    return new AirliftCompressorCodecFactory(pageSize);
  }

  static boolean isSupported(CompressionCodecName codecName) {
    switch (codecName.getParquetCompressionCodec()) {
      case GZIP:
      case LZO:
      case LZ4:
        return true;
      default:
        return false;
    }
  }
}
