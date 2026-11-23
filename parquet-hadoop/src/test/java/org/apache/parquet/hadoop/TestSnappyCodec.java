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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.parquet.hadoop.codec.SnappyCodec;
import org.apache.parquet.hadoop.codec.SnappyCompressor;
import org.apache.parquet.hadoop.codec.SnappyDecompressor;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

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
  public void testSnappyLargeValueUsesDirectDecompressFastPath() throws IOException {
    // Large enough that the whole page is decompressed in a single call, exercising
    // NonBlockedDecompressor's direct byte[] fast path (and its scratch buffer growth),
    // rather than the small multi-call draining path covered by TestSnappyStream.
    byte[] uncompressedData = new byte[2_000_000];
    new Random(42).nextBytes(uncompressedData);

    SnappyCompressor compressor = new SnappyCompressor();
    compressor.setInput(uncompressedData, 0, uncompressedData.length);
    compressor.finish();
    byte[] compressedData = new byte[Snappy.maxCompressedLength(uncompressedData.length)];
    int compressedSize = compressor.compress(compressedData, 0, compressedData.length);

    SnappyDecompressor decompressor = new SnappyDecompressor();
    decompressor.setInput(compressedData, 0, compressedSize);
    byte[] decompressedData = new byte[uncompressedData.length];
    int decompressedSize = decompressor.decompress(decompressedData, 0, decompressedData.length);

    assertThat(decompressor.finished()).isTrue();
    assertThat(decompressedSize).isEqualTo(uncompressedData.length);
    assertThat(decompressedData).isEqualTo(uncompressedData);

    // Reuse across multiple pages of different sizes to exercise the scratch buffer
    // shrinking/growing (it never shrinks, but must grow correctly for a bigger page).
    byte[] bigger = new byte[3_000_000];
    new Random(7).nextBytes(bigger);
    SnappyCompressor compressor2 = new SnappyCompressor();
    compressor2.setInput(bigger, 0, bigger.length);
    compressor2.finish();
    byte[] compressed2 = new byte[Snappy.maxCompressedLength(bigger.length)];
    int compressedSize2 = compressor2.compress(compressed2, 0, compressed2.length);

    decompressor.reset();
    decompressor.setInput(compressed2, 0, compressedSize2);
    byte[] decompressed2 = new byte[bigger.length];
    int decompressedSize2 = decompressor.decompress(decompressed2, 0, decompressed2.length);

    assertThat(decompressor.finished()).isTrue();
    assertThat(decompressedSize2).isEqualTo(bigger.length);
    assertThat(decompressed2).isEqualTo(bigger);
  }

  @Test
  public void TestSnappyStream() throws IOException {
    SnappyCodec codec = new SnappyCodec();
    codec.setConf(new Configuration());

    int blockSize = 1024;
    int inputSize = blockSize * 1024;

    byte[] input = new byte[inputSize];
    for (int i = 0; i < inputSize; ++i) {
      input[i] = (byte) i;
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
    assertThat(codecCompressed).isEqualTo(rawCompressed);

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

    assertThat(rawDecompressed).isEqualTo(input);
    assertThat(codecDecompressed).isEqualTo(input);
  }

  private void TestSnappy(SnappyCompressor compressor, SnappyDecompressor decompressor, String... strings)
      throws IOException {
    compressor.reset();
    decompressor.reset();

    int uncompressedSize = 0;
    for (String s : strings) {
      uncompressedSize += s.length();
    }
    byte[] uncompressedData = new byte[uncompressedSize];
    int len = 0;
    for (String s : strings) {
      byte[] tmp = s.getBytes();
      System.arraycopy(tmp, 0, uncompressedData, len, s.length());
      len += s.length();
    }

    assert (compressor.needsInput());
    compressor.setInput(uncompressedData, 0, len);
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

    assertThat(decompressedSize).isEqualTo(uncompressedSize);
    assertThat(decompressedData).isEqualTo(uncompressedData);
  }
}
