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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.codec.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestLz4RawCodec {
  @Test
  public void testBlock() throws IOException {
    // Reuse the lz4 objects between test cases
    Lz4RawCompressor compressor = new Lz4RawCompressor();
    Lz4RawDecompressor decompressor = new Lz4RawDecompressor();

    testBlockCompression(compressor, decompressor, "");
    testBlockCompression(compressor, decompressor, "FooBar");
    testBlockCompression(compressor, decompressor, "FooBar1FooBar2");
    testBlockCompression(compressor, decompressor, "FooBar");
    testBlockCompression(compressor, decompressor, "ablahblahblahabcdef");
    testBlockCompression(compressor, decompressor, "");
    testBlockCompression(compressor, decompressor, "FooBar");
  }

  // Test lz4 raw compression in the block fashion
  private void testBlockCompression(Lz4RawCompressor compressor, Lz4RawDecompressor decompressor,
                                    String data) throws IOException {
    compressor.reset();
    decompressor.reset();

    int uncompressedSize = data.length();
    byte[] uncompressedData = data.getBytes();

    assert (compressor.needsInput());
    compressor.setInput(uncompressedData, 0, uncompressedSize);
    assert (compressor.needsInput());
    compressor.finish();
    assert (!compressor.needsInput());
    assert (!compressor.finished() || uncompressedSize == 0);
    byte[] compressedData = new byte[1000];

    int compressedSize = compressor.compress(compressedData, 0, 1000);
    assert (compressor.finished());

    assert (!decompressor.finished());
    assert (decompressor.needsInput());
    decompressor.setInput(compressedData, 0, compressedSize);
    assert (!decompressor.finished());
    byte[] decompressedData = new byte[uncompressedSize];
    int decompressedSize = decompressor.decompress(decompressedData, 0, uncompressedSize);
    assert (decompressor.finished());

    assertEquals(uncompressedSize, decompressedSize);
    assertArrayEquals(uncompressedData, decompressedData);
  }

  // Test lz4 raw compression in the streaming fashion
  @Test
  public void testCodec() throws IOException {
    Lz4RawCodec codec = new Lz4RawCodec();
    Configuration conf = new Configuration();
    int[] bufferSizes = {128, 1024, 4 * 1024, 16 * 1024, 128 * 1024, 1024 * 1024};
    int[] dataSizes = {0, 1, 10, 128, 1024, 2048, 1024 * 1024};

    for (int i = 0; i < bufferSizes.length; i++) {
      conf.setInt(Lz4RawCodec.BUFFER_SIZE_CONFIG, bufferSizes[i]);
      codec.setConf(conf);
      for (int j = 0; j < dataSizes.length; j++) {
        // do not repeat
        testLz4RawCodec(codec, dataSizes[j], dataSizes[j]);
        // repeat by every 128 bytes
        testLz4RawCodec(codec, dataSizes[j], 128);
      }
    }
  }

  private void testLz4RawCodec(Lz4RawCodec codec, int dataSize, int repeatSize) throws IOException {
    byte[] data = new byte[dataSize];
    if (repeatSize >= dataSize) {
      (new Random()).nextBytes(data);
    } else {
      byte[] repeat = new byte[repeatSize];
      (new Random()).nextBytes(repeat);
      for (int offset = 0; offset < dataSize; offset += repeatSize) {
        System.arraycopy(repeat, 0, data, offset, Math.min(repeatSize, dataSize - offset));
      }
    }
    BytesInput compressedData = compress(codec, BytesInput.from(data));
    byte[] decompressedData = decompress(codec, compressedData, data.length);
    Assert.assertArrayEquals(data, decompressedData);
  }

  private BytesInput compress(Lz4RawCodec codec, BytesInput bytes) throws IOException {
    ByteArrayOutputStream compressedOutBuffer = new ByteArrayOutputStream((int) bytes.size());
    CompressionOutputStream cos = codec.createOutputStream(compressedOutBuffer);
    bytes.writeAllTo(cos);
    cos.close();
    return BytesInput.from(compressedOutBuffer);
  }

  private byte[] decompress(Lz4RawCodec codec, BytesInput bytes, int uncompressedSize) throws IOException {
    InputStream is = codec.createInputStream(bytes.toInputStream());
    byte[] decompressed = BytesInput.from(is, uncompressedSize).toByteArray();
    is.close();
    return decompressed;
  }
}
