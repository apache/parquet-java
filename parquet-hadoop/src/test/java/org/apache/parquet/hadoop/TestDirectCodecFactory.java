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
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4_RAW;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZO;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
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
      final CodecFactory directCodecFactory =
          CodecFactory.createDirectCodecFactory(new Configuration(), allocator, pageSize);
      final CodecFactory heapCodecFactory = new CodecFactory(new Configuration(), pageSize);
      ByteBuffer rawBuf = allocator.allocate(size);
      releaser.releaseLater(rawBuf);
      final byte[] rawArr = new byte[size];
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

      final BytesInputCompressor directCompressor = directCodecFactory.getCompressor(codec);
      final BytesInputDecompressor directDecompressor = directCodecFactory.getDecompressor(codec);
      final BytesInputCompressor heapCompressor = heapCodecFactory.getCompressor(codec);
      final BytesInputDecompressor heapDecompressor = heapCodecFactory.getDecompressor(codec);

      if (codec == LZ4_RAW) {
        // Hadoop codecs support direct decompressors only if the related native libraries are available.
        // This is not the case for our CI so let's rely on LZ4_RAW where the implementation is our own.
        assertThat(directDecompressor)
            .as(String.format("The hadoop codec %s should support direct decompression", codec))
            .isInstanceOf(DirectCodecFactory.FullDirectDecompressor.class);
      }

      final BytesInput directCompressed;
      if (useOnHeapCompression) {
        directCompressed = directCompressor.compress(BytesInput.from(rawArr));
      } else {
        directCompressed = directCompressor.compress(BytesInput.from(rawBuf));
      }

      BytesInput heapCompressed = heapCompressor.compress(BytesInput.from(rawArr));

      // Validate direct => direct
      validateDecompress(
          size,
          codec,
          decomp,
          directCompressed.copy(releaser),
          allocator,
          directDecompressor,
          rawBuf,
          rawArr);

      // Validate heap => direct
      validateDecompress(size, codec, decomp, heapCompressed, allocator, directDecompressor, rawBuf, rawArr);

      // Validate direct => heap
      validateDecompress(size, codec, decomp, directCompressed, allocator, heapDecompressor, rawBuf, rawArr);

      directCompressor.release();
      directDecompressor.release();
      directCodecFactory.release();
      heapCompressor.release();
      heapDecompressor.release();
      heapCodecFactory.release();
    } catch (Exception e) {
      final String msg = String.format(
          "Failure while testing Codec: %s, OnHeapCompressionInput: %s, Decompression Mode: %s, Data Size: %d",
          codec.name(), useOnHeapCompression, decomp.name(), size);
      LOG.error(msg);
      throw new RuntimeException(msg, e);
    }
  }

  private static void validateDecompress(
      int size,
      CompressionCodecName codec,
      Decompression decomp,
      BytesInput compressed,
      ByteBufferAllocator allocator,
      BytesInputDecompressor d,
      ByteBuffer rawBuf,
      byte[] rawArr)
      throws IOException {
    switch (decomp) {
      case OFF_HEAP: {
        final ByteBuffer buf = compressed.toByteBuffer();
        final ByteBuffer b = allocator.allocate(buf.capacity() + 20);
        final ByteBuffer outBuf = allocator.allocate(size + 20);
        final int shift = 10;
        try {
          b.position(shift);
          b.put(buf);
          b.position(shift);
          outBuf.position(shift);
          d.decompress(b, (int) compressed.size(), outBuf, size);
          assertThat(b.position())
              .as("Input buffer position mismatch for codec " + codec)
              .isEqualTo(compressed.size() + shift);
          assertThat(outBuf.position())
              .as("Output buffer position mismatch for codec " + codec)
              .isEqualTo(size + shift);
          for (int i = 0; i < size; i++) {
            assertThat(outBuf.get(shift + i))
                .as(String.format("Data didn't match at %d, while testing codec %s", i, codec))
                .isEqualTo(rawBuf.get(i));
          }
        } finally {
          allocator.release(b);
          allocator.release(outBuf);
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
          assertThat(rawArr)
              .as(String.format("While testing codec %s", codec))
              .isEqualTo(input.toByteArray());
        } finally {
          allocator.release(b);
        }
        break;
      }
      case ON_HEAP: {
        final byte[] buf = compressed.toByteArray();
        final BytesInput input = d.decompress(BytesInput.from(buf), size);
        assertThat(rawArr)
            .as(String.format("While testing codec %s", codec))
            .isEqualTo(input.toByteArray());
        break;
      }
    }
  }

  @Test
  public void createDirectFactoryWithHeapAllocatorFails() {
    assertThatThrownBy(() ->
            CodecFactory.createDirectCodecFactory(new Configuration(), new HeapByteBufferAllocator(), 0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("A DirectCodecFactory requires a direct buffer allocator be provided.");
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

    assertThat(codec_2_2).isEqualTo(codec_2_1);
    assertThat(codec_2_1).isNotEqualTo(codec_5_1);
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

    assertThat(codec_2_2).isEqualTo(codec_2_1);
    assertThat(codec_2_1).isNotEqualTo(codec_5_1);
  }

  @Test
  public void leveledCompressorCachedForSameLevel() {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor c1 = factory.getCompressor(ZSTD, 3);
    BytesInputCompressor c2 = factory.getCompressor(ZSTD, 3);
    assertThat(c2).as("Same codec+level should return the cached instance").isSameAs(c1);
    factory.release();
  }

  @Test
  public void leveledCompressorDiffersByLevel() {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor c1 = factory.getCompressor(ZSTD, 1);
    BytesInputCompressor c3 = factory.getCompressor(ZSTD, 3);
    assertThat(c3)
        .as("Different levels should return different compressor instances")
        .isNotSameAs(c1);
    factory.release();
  }

  @Test
  public void leveledCacheIsolatedFromNoLevelCache() {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor noLevel = factory.getCompressor(ZSTD);
    BytesInputCompressor withLevel = factory.getCompressor(ZSTD, 3);
    assertThat(withLevel)
        .as("Level-aware and no-level compressors should use separate cache entries")
        .isNotSameAs(noLevel);
    factory.release();
  }

  @Test
  public void leveledUncompressedReturnsNoOp() {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor comp = factory.getCompressor(UNCOMPRESSED, 5);
    assertThat(comp).isSameAs(CodecFactory.NO_OP_COMPRESSOR);
    factory.release();
  }

  @Test
  public void leveledSnappyIgnoresLevel() {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor comp = factory.getCompressor(SNAPPY, 99);
    assertThat(comp).isNotNull();
    assertThat(comp.getCodecName()).isEqualTo(SNAPPY);
    factory.release();
  }

  @Test
  public void leveledGzipInvalidLevelThrows() {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    try {
      assertThatThrownBy(() -> factory.getCompressor(GZIP, 99))
          .isInstanceOf(BadConfigurationException.class)
          .hasMessageContaining("99");
    } finally {
      factory.release();
    }
  }

  @Test
  public void leveledGzipBoundaryLevelsValid() {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    for (int level : new int[] {-1, 0, 1, 9}) {
      BytesInputCompressor comp = factory.getCompressor(GZIP, level);
      assertThat(comp)
          .as("Compressor should not be null for GZIP level " + level)
          .isNotNull();
      assertThat(comp.getCodecName())
          .as("Codec name should be GZIP for level " + level)
          .isEqualTo(GZIP);
    }
    factory.release();
  }

  @Test
  public void leveledZstdRoundTrip() throws IOException {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    byte[] original = "hello parquet per-column compression".getBytes(StandardCharsets.UTF_8);
    BytesInputDecompressor decompressor = factory.getDecompressor(ZSTD);
    for (int level : new int[] {-5, 0, 1, 3, 10, 22}) {
      BytesInput compressed = factory.getCompressor(ZSTD, level).compress(BytesInput.from(original));
      byte[] result = decompressor.decompress(compressed, original.length).toByteArray();
      assertThat(result).as("Round-trip failed at ZSTD level " + level).isEqualTo(original);
    }
    factory.release();
  }

  @Test
  public void leveledGzipRoundTrip() throws IOException {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    byte[] original = "hello parquet per-column compression".getBytes(StandardCharsets.UTF_8);
    BytesInputDecompressor decompressor = factory.getDecompressor(GZIP);
    for (int level : new int[] {1, 5, 9}) {
      BytesInput compressed = factory.getCompressor(GZIP, level).compress(BytesInput.from(original));
      byte[] result = decompressor.decompress(compressed, original.length).toByteArray();
      assertThat(result).as("Round-trip failed at GZIP level " + level).isEqualTo(original);
    }
    factory.release();
  }

  @Test
  public void directFactoryLeveledZstdRoundTrip() throws IOException {
    CodecFactory heap = new CodecFactory(new Configuration(), pageSize);
    CodecFactory direct =
        CodecFactory.createDirectCodecFactory(new Configuration(), new DirectByteBufferAllocator(), pageSize);
    try {
      assertThat(heap.getCompressor(ZSTD, 3))
          .as("heap factory should produce a HeapBytesCompressor")
          .isInstanceOf(CodecFactory.HeapBytesCompressor.class);

      // The direct factory must not fall back to the heap/Hadoop path for leveled ZSTD/SNAPPY.
      BytesInputCompressor directZstd = direct.getCompressor(ZSTD, 3);
      assertThat(directZstd)
          .as("direct factory ZSTD(level) should not fall back to HeapBytesCompressor")
          .isNotInstanceOf(CodecFactory.HeapBytesCompressor.class);
      assertThat(directZstd.getCodecName()).isEqualTo(ZSTD);
      assertThat(directZstd.getClass())
          .as("leveled ZSTD should use the same direct compressor type as the no-level path")
          .isEqualTo(direct.getCompressor(ZSTD).getClass());

      BytesInputCompressor directSnappy = direct.getCompressor(SNAPPY, 5);
      assertThat(directSnappy)
          .as("direct factory SNAPPY(level) should not fall back to HeapBytesCompressor")
          .isNotInstanceOf(CodecFactory.HeapBytesCompressor.class);
      assertThat(directSnappy.getClass())
          .as("leveled SNAPPY should use the same direct compressor type as the no-level path")
          .isEqualTo(direct.getCompressor(SNAPPY).getClass());

      // The direct ZSTD level path must compress/decompress correctly at each level.
      byte[] original = "hello parquet per-column zstd direct compression".getBytes(StandardCharsets.UTF_8);
      BytesInputDecompressor decompressor = heap.getDecompressor(ZSTD);
      for (int level : new int[] {1, 3, 22}) {
        byte[] compressed = direct.getCompressor(ZSTD, level)
            .compress(BytesInput.from(original))
            .toByteArray();
        byte[] result = decompressor
            .decompress(BytesInput.from(compressed), original.length)
            .toByteArray();
        assertThat(result)
            .as("Direct ZSTD round-trip failed at level " + level)
            .isEqualTo(original);
      }
    } finally {
      heap.release();
      direct.release();
    }
  }

  @Test
  public void directFactoryInvalidZstdLevelThrows() {
    CodecFactory direct =
        CodecFactory.createDirectCodecFactory(new Configuration(), new DirectByteBufferAllocator(), pageSize);
    try {
      assertThatThrownBy(() -> direct.getCompressor(ZSTD, 23))
          .isInstanceOf(BadConfigurationException.class)
          .hasMessageContaining("23");
    } finally {
      direct.release();
    }
  }
}
