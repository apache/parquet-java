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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

public class TestBrotliCodec {

  @Test
  public void testRoundTrip() throws Exception {
    byte[] input = new byte[64 * 1024];
    new Random(42).nextBytes(input);

    Configuration conf = new Configuration(false);
    conf.setInt(BrotliCodec.BROTLI_QUALITY, 5);

    CodecFactory factory = new CodecFactory(conf, 64 * 1024);
    try {
      CodecFactory.BytesCompressor compressor = factory.getCompressor(CompressionCodecName.BROTLI);
      BytesInput compressed = compressor.compress(BytesInput.from(input));

      CodecFactory.BytesDecompressor decompressor =
          factory.getDecompressor(CompressionCodecName.BROTLI);
      BytesInput decompressed = decompressor.decompress(compressed, input.length);

      assertArrayEquals(input, decompressed.toByteArray());
    } finally {
      factory.release();
    }
  }
}
