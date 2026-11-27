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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.codec.SnappyCodec;
import org.apache.parquet.hadoop.codec.SnappyDecompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

/**
 * HeapBytesDecompressor is package-private, so this test lives in org.apache.parquet.hadoop
 * (unlike TestCompressionCodec, which lives in org.apache.parquet.hadoop.codec) to be able to
 * instantiate it directly against a stub codec and prove which internal code path actually ran.
 */
public class TestCodecFactoryDirectPath {

  /**
   * Regression guard: a test that only checks the final decompressed bytes would pass
   * identically whether CodecFactory routes SNAPPY through the one-shot DirectDecompressor path
   * or falls back to the old streaming Decompressor API, since both are functionally correct.
   * This proves the one-shot path is actually what ran.
   */
  @Test
  public void testHeapDecompressorUsesOneShotDirectPathNotStreaming() throws IOException {
    byte[] raw = new byte[1024];
    new Random(7).nextBytes(raw);

    CodecFactory realFactory = new CodecFactory(new Configuration(), 64 * 1024);
    BytesInputCompressor compressor = realFactory.getCompressor(CompressionCodecName.SNAPPY);
    BytesInput compressed = compressor.compress(BytesInput.from(raw));
    compressor.release();
    realFactory.release();

    CodecFactory factory = new CodecFactory(new Configuration(), 64 * 1024);
    CodecFactory.HeapBytesDecompressor decompressor =
        factory.new HeapBytesDecompressor(new CountingStubSnappyCodec());

    BytesInput decompressed = decompressor.decompress(compressed, raw.length);

    assertThat(decompressed.toByteArray()).isEqualTo(raw);
    assertThat(CountingSnappyDecompressor.directPathCalls.get()).isEqualTo(1);
    assertThat(CountingSnappyDecompressor.streamingPathCalls.get()).isEqualTo(0);

    decompressor.release();
    factory.release();
  }

  /** Counts calls to the one-shot ByteBuffer path vs. the old streaming byte[] path. */
  private static final class CountingSnappyDecompressor extends SnappyDecompressor {
    static final AtomicInteger directPathCalls = new AtomicInteger();
    static final AtomicInteger streamingPathCalls = new AtomicInteger();

    @Override
    public void decompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
      directPathCalls.incrementAndGet();
      super.decompress(compressed, uncompressed);
    }

    @Override
    public synchronized int decompress(byte[] b, int off, int len) throws IOException {
      streamingPathCalls.incrementAndGet();
      return super.decompress(b, off, len);
    }
  }

  /**
   * Only createDecompressor()/getDecompressorType() need to differ from the real SnappyCodec:
   * the one-shot DirectDecompressor branch under test returns before ever reaching
   * createInputStream()/createOutputStream(), so there's no need to stub those out too.
   */
  private static final class CountingStubSnappyCodec extends SnappyCodec {
    @Override
    public Decompressor createDecompressor() {
      return new CountingSnappyDecompressor();
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
      return CountingSnappyDecompressor.class;
    }
  }
}
