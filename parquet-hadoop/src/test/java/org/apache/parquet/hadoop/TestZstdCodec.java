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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.codec.ZstdCodec;
import org.junit.Assert;
import org.junit.Test;  

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

public class TestZstdCodec {

  @Test
  public void testZstdCodec() throws IOException {
    ZstdCodec codec = new ZstdCodec();
    Configuration conf = new Configuration();
    int[] levels = {1, 4, 7, 10, 13, 16, 19, 22};
    int[] dataSizes = {0, 1, 10, 1024, 1024 * 1024};

    for (int i = 0; i < levels.length; i++) {
      conf.setInt(ZstdCodec.PARQUET_COMPRESS_ZSTD_LEVEL, levels[i]);
      codec.setConf(conf);
      for (int j = 0; j < dataSizes.length; j++) {
        testZstd(codec, dataSizes[j]);
      }
    }
  }

  private void testZstd(ZstdCodec codec, int dataSize) throws IOException {
    byte[] data = new byte[dataSize];
    (new Random()).nextBytes(data);
    BytesInput compressedData = compress(codec,  BytesInput.from(data));
    BytesInput decompressedData = decompress(codec, compressedData, data.length);
    Assert.assertArrayEquals(data, decompressedData.toByteArray());
  }

  private BytesInput compress(ZstdCodec codec, BytesInput bytes) throws IOException {
    ByteArrayOutputStream compressedOutBuffer = new ByteArrayOutputStream((int)bytes.size());
    CompressionOutputStream cos = codec.createOutputStream(compressedOutBuffer, null);
    bytes.writeAllTo(cos);
    cos.close();
    return BytesInput.from(compressedOutBuffer);
  }

  private BytesInput decompress(ZstdCodec codec, BytesInput bytes, int uncompressedSize) throws IOException {
    BytesInput decompressed;
    InputStream is = codec.createInputStream(bytes.toInputStream(), null);
    decompressed = BytesInput.from(is, uncompressedSize);
    is.close();
    return decompressed;
  }
}
