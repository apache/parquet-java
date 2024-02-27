/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.BROTLI;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZO;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDirectCodecFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TestDirectCodecFactory.class);

  private enum Decompression {
    ON_HEAP,
    OFF_HEAP,
    OFF_HEAP_BYTES_INPUT
  }

  private final int pageSize = 64 * 1024;

  private void test(int size, CompressionCodecName codec, boolean useOnHeapCompression, Decompression decomp) {
    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator());
        ByteBufferReleaser releaser = new ByteBufferReleaser(allocator)) {
      final CodecFactory codecFactory =
          CodecFactory.createDirectCodecFactory(new Configuration(), allocator, pageSize);
      ByteBuffer rawBuf = allocator.allocate(size);
      releaser.releaseLater(rawBuf);
      final byte[] rawArr = new byte[size];
      ByteBuffer outBuf = allocator.allocate(size * 2);
      releaser.releaseLater(outBuf);
      final Random r = new Random();
      final byte[] random = new byte[1024];
      int pos = 0;
      while (pos < size) {
        r.nextBytes(random);
        rawBuf.put(random);
        System.arraycopy(random, 0, rawArr, pos, random.length);
        pos += random.length;
      }
      rawBuf.flip();

      final BytesInputCompressor c = codecFactory.getCompressor(codec);
      final BytesInputDecompressor d = codecFactory.getDecompressor(codec);

      final BytesInput compressed;
      if (useOnHeapCompression) {
        compressed = c.compress(BytesInput.from(rawArr));
      } else {
        compressed = c.compress(BytesInput.from(rawBuf));
      }

      switch (decomp) {
        case OFF_HEAP: {
          final ByteBuffer buf = compressed.toByteBuffer();
          final ByteBuffer b = allocator.allocate(buf.capacity());
          try {
            b.put(buf);
            b.flip();
            d.decompress(b, (int) compressed.size(), outBuf, size);
            for (int i = 0; i < size; i++) {
              Assert.assertTrue("Data didn't match at " + i, outBuf.get(i) == rawBuf.get(i));
            }
          } finally {
            allocator.release(b);
          }
          break;
        }

        case OFF_HEAP_BYTES_INPUT: {
          final ByteBuffer buf = compressed.toByteBuffer();
          final ByteBuffer b = allocator.allocate(buf.limit());
          try {
            b.put(buf);
            b.flip();
            final BytesInput input = d.decompress(BytesInput.from(b), size);
            Assert.assertArrayEquals(
                String.format("While testing codec %s", codec), input.toByteArray(), rawArr);
          } finally {
            allocator.release(b);
          }
          break;
        }
        case ON_HEAP: {
          final byte[] buf = compressed.toByteArray();
          final BytesInput input = d.decompress(BytesInput.from(buf), size);
          Assert.assertArrayEquals(input.toByteArray(), rawArr);
          break;
        }
      }
      c.release();
      d.release();
    } catch (Exception e) {
      final String msg = String.format(
          "Failure while testing Codec: %s, OnHeapCompressionInput: %s, Decompression Mode: %s, Data Size: %d",
          codec.name(), useOnHeapCompression, decomp.name(), size);
      LOG.error(msg);
      throw new RuntimeException(msg, e);
    }
  }

  @Test
  public void createDirectFactoryWithHeapAllocatorFails() {
    String errorMsg =
        "Test failed, creation of a direct codec factory should have failed when passed a non-direct allocator.";
    try {
      CodecFactory.createDirectCodecFactory(new Configuration(), new HeapByteBufferAllocator(), 0);
      throw new RuntimeException(errorMsg);
    } catch (IllegalStateException ex) {
      // indicates successful completion of the test
      Assert.assertTrue(
          "Missing expected error message.",
          ex.getMessage().contains("A DirectCodecFactory requires a direct buffer allocator be provided."));
    } catch (Exception ex) {
      throw new RuntimeException(errorMsg + " Failed with the wrong error.");
    }
  }

  @Test
  public void compressionCodecs() {
    final int[] sizes = {4 * 1024, 1 * 1024 * 1024};
    final boolean[] comp = {true, false};
    Set<CompressionCodecName> codecsToSkip = new HashSet<>();
    codecsToSkip.add(LZO); // not distributed because it is GPL
    codecsToSkip.add(LZ4); // not distributed in the default version of Hadoop
    final String arch = System.getProperty("os.arch");
    if ("aarch64".equals(arch)) {
      // PARQUET-1975 brotli-codec does not have natives for ARM64 architectures
      codecsToSkip.add(BROTLI);
    }

    for (final int size : sizes) {
      for (final boolean useOnHeapComp : comp) {
        for (final Decompression decomp : Decompression.values()) {
          for (final CompressionCodecName codec : CompressionCodecName.values()) {
            if (codecsToSkip.contains(codec)) {
              continue;
            }
            test(size, codec, useOnHeapComp, decomp);
          }
        }
      }
    }
  }

  static class PublicCodecFactory extends CodecFactory {
    // To make getCodec public

    public PublicCodecFactory(Configuration configuration, int pageSize) {
      super(configuration, pageSize);
    }

    public org.apache.hadoop.io.compress.CompressionCodec getCodec(CompressionCodecName name) {
      return super.getCodec(name);
    }
  }

  @Test
  public void cachingKeysGzip() {
    Configuration config_zlib_2 = new Configuration();
    config_zlib_2.set("zlib.compress.level", "2");

    Configuration config_zlib_5 = new Configuration();
    config_zlib_5.set("zlib.compress.level", "5");

    final CodecFactory codecFactory_2 = new PublicCodecFactory(config_zlib_2, pageSize);
    final CodecFactory codecFactory_5 = new PublicCodecFactory(config_zlib_5, pageSize);

    CompressionCodec codec_2_1 = codecFactory_2.getCodec(CompressionCodecName.GZIP);
    CompressionCodec codec_2_2 = codecFactory_2.getCodec(CompressionCodecName.GZIP);
    CompressionCodec codec_5_1 = codecFactory_5.getCodec(CompressionCodecName.GZIP);

    Assert.assertEquals(codec_2_1, codec_2_2);
    Assert.assertNotEquals(codec_2_1, codec_5_1);
  }

  @Test
  public void cachingKeysZstd() {
    Configuration config_zstd_2 = new Configuration();
    config_zstd_2.set("parquet.compression.codec.zstd.level", "2");

    Configuration config_zstd_5 = new Configuration();
    config_zstd_5.set("parquet.compression.codec.zstd.level", "5");

    final CodecFactory codecFactory_2 = new PublicCodecFactory(config_zstd_2, pageSize);
    final CodecFactory codecFactory_5 = new PublicCodecFactory(config_zstd_5, pageSize);

    CompressionCodec codec_2_1 = codecFactory_2.getCodec(CompressionCodecName.ZSTD);
    CompressionCodec codec_2_2 = codecFactory_2.getCodec(CompressionCodecName.ZSTD);
    CompressionCodec codec_5_1 = codecFactory_5.getCodec(CompressionCodecName.ZSTD);

    Assert.assertEquals(codec_2_1, codec_2_2);
    Assert.assertNotEquals(codec_2_1, codec_5_1);
  }
}
