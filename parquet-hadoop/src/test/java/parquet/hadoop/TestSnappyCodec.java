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
package parquet.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.xerial.snappy.Snappy;

import parquet.hadoop.codec.SnappyCodec;
import parquet.hadoop.codec.SnappyCompressor;
import parquet.hadoop.codec.SnappyDecompressor;

public class TestSnappyCodec {
  @Test
  public void TestSnappy() throws IOException {
    // Reuse the snappy objects between test cases
    SnappyCompressor compressor = new SnappyCompressor();
    SnappyDecompressor decompressor = new SnappyDecompressor();

    TestSnappy(compressor, decompressor, "");    
    TestSnappy(compressor, decompressor, "FooBar");    
    TestSnappy(compressor, decompressor, "FooBar1", "FooBar2");    
    TestSnappy(compressor, decompressor, "FooBar");
    TestSnappy(compressor, decompressor, "a", "blahblahblah", "abcdef");    
    TestSnappy(compressor, decompressor, "");
    TestSnappy(compressor, decompressor, "FooBar");
  }
  
  @Test
  public void TestSnappyStream() throws IOException {
    SnappyCodec codec = new SnappyCodec();
    codec.setConf(new Configuration());
    
    int blockSize = 1024;
    int inputSize = blockSize * 1024;
 
    byte[] input = new byte[inputSize];
    for (int i = 0; i < inputSize; ++i) {
      input[i] = (byte)i;
    }

    ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
    
    CompressionOutputStream compressor = codec.createOutputStream(compressedStream);
    int bytesCompressed = 0;
    while (bytesCompressed < inputSize) {
      int len = Math.min(inputSize - bytesCompressed, blockSize);
      compressor.write(input, bytesCompressed, len);
      bytesCompressed += len;
    }
    compressor.finish();
    
    byte[] rawCompressed = Snappy.compress(input);
    byte[] codecCompressed = compressedStream.toByteArray();
    
    // Validate that the result from the codec is the same as if we compressed the 
    // buffer directly.
    assertArrayEquals(rawCompressed, codecCompressed);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(codecCompressed);    
    CompressionInputStream decompressor = codec.createInputStream(inputStream);
    byte[] codecDecompressed = new byte[inputSize];
    int bytesDecompressed = 0;
    int numBytes;
    while ((numBytes = decompressor.read(codecDecompressed, bytesDecompressed, blockSize)) != 0) {
      bytesDecompressed += numBytes;
      if (bytesDecompressed == inputSize) break;
    }
    
    byte[] rawDecompressed = Snappy.uncompress(rawCompressed);
    
    assertArrayEquals(input, rawDecompressed);
    assertArrayEquals(input, codecDecompressed);
  }

  private void TestSnappy(SnappyCompressor compressor, SnappyDecompressor decompressor, 
      String... strings) throws IOException {
    compressor.reset();
    decompressor.reset();

    int uncompressedSize = 0;
    for (String s: strings) {
      uncompressedSize += s.length();
    }
    byte[] uncompressedData = new byte[uncompressedSize];
    int len = 0;
    for (String s: strings) {
      byte[] tmp = s.getBytes();
      System.arraycopy(tmp, 0, uncompressedData, len, s.length());
      len += s.length();
    }

    assert(compressor.needsInput());
    compressor.setInput(uncompressedData, 0, len);
    assert(compressor.needsInput());
    compressor.finish();
    assert(!compressor.needsInput());
    assert(!compressor.finished() || uncompressedSize == 0);
    byte[] compressedData = new byte[1000];

    int compressedSize = compressor.compress(compressedData, 0, 1000);
    assert(compressor.finished());

    assert(!decompressor.finished());
    assert(decompressor.needsInput());
    decompressor.setInput(compressedData, 0, compressedSize);
    assert(!decompressor.finished());
    byte[] decompressedData = new byte[uncompressedSize];
    int decompressedSize = decompressor.decompress(decompressedData, 0, uncompressedSize);
    assert(decompressor.finished());

    assertEquals(uncompressedSize, decompressedSize);
    assertArrayEquals(uncompressedData, decompressedData);
  }
}
