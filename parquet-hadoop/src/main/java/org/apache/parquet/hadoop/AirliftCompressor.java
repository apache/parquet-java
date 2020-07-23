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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class AirliftCompressor extends CodecFactory.BytesCompressor {
  private final Compressor compressor;
  private final ByteArrayOutputStream compressedOutBuffer;
  private final CompressionCodec hadoopCodec;
  private final CompressionCodecName parquetCodecName;

  AirliftCompressor(CompressionCodecName parquetCodecName, CompressionCodec hadoopCodec, int pageSize) {
    this.parquetCodecName = parquetCodecName;
    this.hadoopCodec = hadoopCodec;
    this.compressor = hadoopCodec.createCompressor();
    this.compressedOutBuffer = new ByteArrayOutputStream(pageSize);
  }

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    compressedOutBuffer.reset();
    try (CompressionOutputStream cos = hadoopCodec.createOutputStream(compressedOutBuffer, compressor)) {
      bytes.writeAllTo(cos);
      return BytesInput.from(compressedOutBuffer);
    }
   }

  @Override
  public CompressionCodecName getCodecName() {
    return parquetCodecName;
  }

  @Override
  public void release() {}
}
