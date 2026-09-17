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
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.codec.ZstandardCodec;
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
    codecsToSkip.add(LZ4); // not distributed in the default version of Hadoop

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

  @Test
  public void compressionLevelGzip() throws IOException {
    Configuration config_zlib_1 = new Configuration();
    config_zlib_1.set("zlib.compress.level", "1");

    Configuration config_zlib_9 = new Configuration();
    config_zlib_9.set("zlib.compress.level", "9");

    // Generate compressible data so different levels produce different sizes
    byte[] data = new byte[64 * 1024];
    new Random(42).nextBytes(data);

    final CodecFactory codecFactory_1 = new CodecFactory(config_zlib_1, pageSize);
    final CodecFactory codecFactory_9 = new CodecFactory(config_zlib_9, pageSize);

    BytesInputCompressor compressor_1 = codecFactory_1.getCompressor(CompressionCodecName.GZIP);
    BytesInputCompressor compressor_9 = codecFactory_9.getCompressor(CompressionCodecName.GZIP);

    long size_1 = compressor_1.compress(BytesInput.from(data)).size();
    long size_9 = compressor_9.compress(BytesInput.from(data)).size();

    // Level 9 should produce smaller (or equal) output than level 1
    assertThat(size_9)
        .as("Expected level 9 (" + size_9 + ") <= level 1 (" + size_1 + ")")
        .isLessThanOrEqualTo(size_1);

    codecFactory_1.release();
    codecFactory_9.release();
  }

  @Test
  public void compressionLevelGzipSupportsHadoopNames() throws IOException {
    for (String level : new String[] {
      "NO_COMPRESSION",
      "BEST_SPEED",
      "TWO",
      "THREE",
      "FOUR",
      "FIVE",
      "SIX",
      "SEVEN",
      "EIGHT",
      "BEST_COMPRESSION",
      "DEFAULT_COMPRESSION"
    }) {
      Configuration conf = new Configuration();
      conf.set("zlib.compress.level", level);
      CodecFactory factory = new CodecFactory(conf, pageSize);
      try {
        BytesInputCompressor compressor = factory.getCompressor(GZIP);
        BytesInput compressed = compressor.compress(BytesInput.from(new byte[] {1, 2, 3}));
        assertThat(factory.getDecompressor(GZIP)
                .decompress(compressed, 3)
                .toByteArray())
            .as("GZIP round-trip failed for Hadoop level " + level)
            .isEqualTo(new byte[] {1, 2, 3});
      } finally {
        factory.release();
      }
    }
  }

  @Test
  public void compressionLevelZstd() throws IOException {
    Configuration config_zstd_1 = new Configuration();
    config_zstd_1.set("parquet.compression.codec.zstd.level", "1");

    Configuration config_zstd_19 = new Configuration();
    config_zstd_19.set("parquet.compression.codec.zstd.level", "19");

    // Generate compressible data so different levels produce different sizes
    byte[] data = new byte[64 * 1024];
    new Random(42).nextBytes(data);

    final CodecFactory codecFactory_1 = new CodecFactory(config_zstd_1, pageSize);
    final CodecFactory codecFactory_19 = new CodecFactory(config_zstd_19, pageSize);

    BytesInputCompressor compressor_1 = codecFactory_1.getCompressor(CompressionCodecName.ZSTD);
    BytesInputCompressor compressor_19 = codecFactory_19.getCompressor(CompressionCodecName.ZSTD);

    long size_1 = compressor_1.compress(BytesInput.from(data)).size();
    long size_19 = compressor_19.compress(BytesInput.from(data)).size();

    // Level 19 should produce smaller (or equal) output than level 1
    assertThat(size_19)
        .as("Expected level 19 (" + size_19 + ") <= level 1 (" + size_1 + ")")
        .isLessThanOrEqualTo(size_1);

    codecFactory_1.release();
    codecFactory_19.release();
  }

  // ---- Tests for empty input (0 bytes) through direct compressor/decompressor path ----

  @Test
  public void emptyInputRoundTrip() throws IOException {
    // Codecs that have direct bypass implementations in CodecFactory
    CompressionCodecName[] directCodecs = {SNAPPY, ZSTD, LZ4_RAW, GZIP, LZO, BROTLI};
    for (CompressionCodecName codec : directCodecs) {
      CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
      BytesInputCompressor compressor = factory.getCompressor(codec);
      BytesInputDecompressor decompressor = factory.getDecompressor(codec);

      BytesInput compressed = compressor.compress(BytesInput.from(new byte[0]));
      BytesInput decompressed = decompressor.decompress(compressed, 0);
      assertThat(decompressed.toByteArray().length)
          .as("Empty input round-trip failed for " + codec)
          .isEqualTo(0);

      compressor.release();
      decompressor.release();
      factory.release();
    }
  }

  // ---- Tests for GZIP consecutive compressions with a single compressor instance ----

  @Test
  public void gzipConsecutiveCompressionsProduceCorrectResults() throws IOException {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = factory.getCompressor(GZIP);
    BytesInputDecompressor decompressor = factory.getDecompressor(GZIP);

    Random r = new Random(99);
    for (int i = 0; i < 10; i++) {
      byte[] data = new byte[4096 + i * 1024];
      r.nextBytes(data);

      BytesInput compressed = compressor.compress(BytesInput.from(data));
      BytesInput decompressed = decompressor.decompress(compressed, data.length);
      assertThat(decompressed.toByteArray())
          .as("GZIP consecutive round-trip failed on iteration " + i)
          .isEqualTo(data);
    }

    compressor.release();
    decompressor.release();
    factory.release();
  }

  // ---- Tests for buffer reuse safety in Snappy/LZ4_RAW compressors ----

  @Test
  public void snappyCompressorBufferReuseSafety() throws IOException {
    verifyCompressorOutputCopiedBeforeReuse(SNAPPY);
  }

  @Test
  public void lz4RawCompressorBufferReuseSafety() throws IOException {
    verifyCompressorOutputCopiedBeforeReuse(LZ4_RAW);
  }

  /**
   * Verifies that the caller can safely copy the compressed output before the next
   * compress() call overwrites the internal buffer. This is the documented contract
   * for compressors that reuse output buffers.
   */
  private void verifyCompressorOutputCopiedBeforeReuse(CompressionCodecName codec) throws IOException {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    BytesInputCompressor compressor = factory.getCompressor(codec);
    BytesInputDecompressor decompressor = factory.getDecompressor(codec);

    byte[] data1 = new byte[4096];
    byte[] data2 = new byte[4096];
    new Random(1).nextBytes(data1);
    new Random(2).nextBytes(data2);

    // Compress first, copy result immediately
    BytesInput compressed1 = compressor.compress(BytesInput.from(data1));
    byte[] compressed1Bytes = compressed1.toByteArray();

    // Compress second (may overwrite internal buffer)
    BytesInput compressed2 = compressor.compress(BytesInput.from(data2));
    byte[] compressed2Bytes = compressed2.toByteArray();

    // Both should decompress correctly from the copied bytes
    BytesInput decompressed1 = decompressor.decompress(BytesInput.from(compressed1Bytes), data1.length);
    assertThat(decompressed1.toByteArray())
        .as(codec + " buffer reuse: first input corrupted")
        .isEqualTo(data1);

    BytesInput decompressed2 = decompressor.decompress(BytesInput.from(compressed2Bytes), data2.length);
    assertThat(decompressed2.toByteArray())
        .as(codec + " buffer reuse: second input corrupted")
        .isEqualTo(data2);

    compressor.release();
    decompressor.release();
    factory.release();
  }

  // ---- Tests for ZSTD bufferPool config propagation through new direct compressor ----

  @Test
  public void zstdBufferPoolEnabledRoundTrip() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ZstandardCodec.PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED, true);
    verifyZstdRoundTrip(conf, "bufferPool=true");
  }

  @Test
  public void zstdBufferPoolDisabledRoundTrip() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ZstandardCodec.PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED, false);
    verifyZstdRoundTrip(conf, "bufferPool=false");
  }

  /**
   * Verifies ZSTD round-trip with different bufferPool configurations through the
   * new direct ZstdBytesCompressor/ZstdBytesDecompressor path in CodecFactory.
   */
  private void verifyZstdRoundTrip(Configuration conf, String label) throws IOException {
    CodecFactory factory = new CodecFactory(conf, pageSize);
    BytesInputCompressor compressor = factory.getCompressor(ZSTD);
    BytesInputDecompressor decompressor = factory.getDecompressor(ZSTD);

    byte[] data = new byte[64 * 1024];
    new Random(42).nextBytes(data);

    BytesInput compressed = compressor.compress(BytesInput.from(data));
    BytesInput decompressed = decompressor.decompress(compressed, data.length);
    assertThat(decompressed.toByteArray())
        .as("ZSTD round-trip failed with " + label)
        .isEqualTo(data);

    compressor.release();
    decompressor.release();
    factory.release();
  }

  // ---- Tests for ZSTD workers config propagation ----

  @Test
  public void zstdWorkersConfigRoundTrip() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(ZstandardCodec.PARQUET_COMPRESS_ZSTD_WORKERS, 2);
    CodecFactory factory = new CodecFactory(conf, pageSize);
    BytesInputCompressor compressor = factory.getCompressor(ZSTD);
    BytesInputDecompressor decompressor = factory.getDecompressor(ZSTD);

    byte[] data = new byte[64 * 1024];
    new Random(42).nextBytes(data);

    BytesInput compressed = compressor.compress(BytesInput.from(data));
    BytesInput decompressed = decompressor.decompress(compressed, data.length);
    assertThat(decompressed.toByteArray())
        .as("ZSTD round-trip failed with workers=2")
        .isEqualTo(data);

    compressor.release();
    decompressor.release();
    factory.release();
  }

  // ---- Tests for ZSTD level through the direct CodecFactory path ----

  @Test
  public void zstdLevelConfigThroughDirectPath() throws IOException {
    Configuration confLow = new Configuration();
    confLow.setInt(ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, 1);

    Configuration confHigh = new Configuration();
    confHigh.setInt(ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, 19);

    byte[] data = new byte[64 * 1024];
    new Random(42).nextBytes(data);

    CodecFactory factoryLow = new CodecFactory(confLow, pageSize);
    CodecFactory factoryHigh = new CodecFactory(confHigh, pageSize);

    long sizeLow =
        factoryLow.getCompressor(ZSTD).compress(BytesInput.from(data)).size();
    long sizeHigh =
        factoryHigh.getCompressor(ZSTD).compress(BytesInput.from(data)).size();

    assertThat(sizeHigh)
        .as("Expected ZSTD level 19 (" + sizeHigh + ") <= level 1 (" + sizeLow + ")")
        .isLessThanOrEqualTo(sizeLow);

    factoryLow.release();
    factoryHigh.release();
  }

  // ---- Tests for GZIP level through the direct CodecFactory path ----

  @Test
  public void gzipLevelConfigThroughDirectPath() throws IOException {
    Configuration confLow = new Configuration();
    confLow.setInt("zlib.compress.level", 1);

    Configuration confHigh = new Configuration();
    confHigh.setInt("zlib.compress.level", 9);

    byte[] data = new byte[64 * 1024];
    new Random(42).nextBytes(data);

    CodecFactory factoryLow = new CodecFactory(confLow, pageSize);
    CodecFactory factoryHigh = new CodecFactory(confHigh, pageSize);

    BytesInputCompressor compLow = factoryLow.getCompressor(GZIP);
    BytesInputCompressor compHigh = factoryHigh.getCompressor(GZIP);

    long sizeLow = compLow.compress(BytesInput.from(data)).size();
    long sizeHigh = compHigh.compress(BytesInput.from(data)).size();

    assertThat(sizeHigh)
        .as("Expected GZIP level 9 (" + sizeHigh + ") <= level 1 (" + sizeLow + ")")
        .isLessThanOrEqualTo(sizeLow);

    // Also verify round-trip for both levels
    BytesInputDecompressor decompLow = factoryLow.getDecompressor(GZIP);
    BytesInputDecompressor decompHigh = factoryHigh.getDecompressor(GZIP);

    assertThat(decompLow
            .decompress(compLow.compress(BytesInput.from(data)), data.length)
            .toByteArray())
        .isEqualTo(data);
    assertThat(decompHigh
            .decompress(compHigh.compress(BytesInput.from(data)), data.length)
            .toByteArray())
        .isEqualTo(data);

    factoryLow.release();
    factoryHigh.release();
  }

  // ---- Tests for BROTLI direct bypass (when native lib available) ----

  @Test
  public void brotliDirectFactoryRoundTrip() throws IOException {
    // Test through the DirectCodecFactory path where BROTLI bypass lives
    try (TrackingByteBufferAllocator alloc = TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator())) {
      CodecFactory directFactory = CodecFactory.createDirectCodecFactory(new Configuration(), alloc, pageSize);
      BytesInputCompressor compressor = directFactory.getCompressor(BROTLI);
      BytesInputDecompressor decompressor = directFactory.getDecompressor(BROTLI);

      // Use compressible data (repeated patterns) so compression is verifiable
      byte[] data = new byte[16 * 1024];
      for (int i = 0; i < data.length; i++) {
        data[i] = (byte) (i % 251);
      }

      BytesInput compressed = compressor.compress(BytesInput.from(data));
      BytesInput decompressed = decompressor.decompress(compressed, data.length);
      assertThat(decompressed.toByteArray())
          .as("BROTLI direct round-trip failed")
          .isEqualTo(data);

      // Test multiple consecutive compressions to verify state management
      for (int i = 0; i < 5; i++) {
        byte[] moreData = new byte[8 * 1024 + i * 1024];
        for (int j = 0; j < moreData.length; j++) {
          moreData[j] = (byte) ((j + i) % 251);
        }
        BytesInput moreCompressed = compressor.compress(BytesInput.from(moreData));
        BytesInput moreDecompressed = decompressor.decompress(moreCompressed, moreData.length);
        assertThat(moreDecompressed.toByteArray())
            .as("BROTLI direct round-trip failed on iteration " + i)
            .isEqualTo(moreData);
      }

      compressor.release();
      decompressor.release();
      directFactory.release();
    }
  }

  // ---- Test for cross-factory interop with new direct codecs ----

  @Test
  public void crossFactoryInteropAllDirectCodecs() throws IOException {
    CompressionCodecName[] codecs = {SNAPPY, ZSTD, LZ4_RAW, GZIP, LZO, BROTLI};

    byte[] data = new byte[32 * 1024];
    new Random(42).nextBytes(data);

    CodecFactory heapFactory = new CodecFactory(new Configuration(), pageSize);
    try (TrackingByteBufferAllocator alloc = TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator())) {
      CodecFactory directFactory = CodecFactory.createDirectCodecFactory(new Configuration(), alloc, pageSize);

      for (CompressionCodecName codec : codecs) {
        // heap compress -> direct decompress
        BytesInput heapCompressed = heapFactory.getCompressor(codec).compress(BytesInput.from(data));
        BytesInput directDecompressed =
            directFactory.getDecompressor(codec).decompress(heapCompressed, data.length);
        assertThat(directDecompressed.toByteArray())
            .as(codec + " heap->direct failed")
            .isEqualTo(data);

        // direct compress -> heap decompress
        BytesInput directCompressed = directFactory.getCompressor(codec).compress(BytesInput.from(data));
        BytesInput heapDecompressed =
            heapFactory.getDecompressor(codec).decompress(directCompressed, data.length);
        assertThat(heapDecompressed.toByteArray())
            .as(codec + " direct->heap failed")
            .isEqualTo(data);
      }

      directFactory.release();
    }
    heapFactory.release();
  }

  @Test
  public void zstdCompressorBufferReuseSafety() throws IOException {
    verifyCompressorOutputCopiedBeforeReuse(ZSTD);
  }

  @Test
  public void brotliQualityConfigProducesValidOutput() throws IOException {
    byte[] data = new byte[16 * 1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 251);
    }

    for (int quality : new int[] {0, 1, 6, 11}) {
      Configuration conf = new Configuration();
      conf.setInt("compression.brotli.quality", quality);
      CodecFactory factory = new CodecFactory(conf, pageSize);
      BytesInputCompressor compressor = factory.getCompressor(BROTLI);
      BytesInputDecompressor decompressor = factory.getDecompressor(BROTLI);

      BytesInput compressed = compressor.compress(BytesInput.from(data));
      BytesInput decompressed = decompressor.decompress(compressed, data.length);
      assertThat(decompressed.toByteArray())
          .as("BROTLI quality=" + quality + " round-trip failed")
          .isEqualTo(data);

      compressor.release();
      decompressor.release();
      factory.release();
    }
  }

  @Test
  public void singleByteInputRoundTrip() throws IOException {
    CompressionCodecName[] codecs = {SNAPPY, ZSTD, LZ4_RAW, GZIP, LZO, BROTLI};
    byte[] data = new byte[] {42};

    for (CompressionCodecName codec : codecs) {
      CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
      BytesInputCompressor compressor = factory.getCompressor(codec);
      BytesInputDecompressor decompressor = factory.getDecompressor(codec);

      BytesInput compressed = compressor.compress(BytesInput.from(data));
      BytesInput decompressed = decompressor.decompress(compressed, 1);
      assertThat(decompressed.toByteArray())
          .as("Single-byte round-trip failed for " + codec)
          .isEqualTo(data);

      compressor.release();
      decompressor.release();
      factory.release();
    }
  }

  @Test
  public void decompressorsRejectIncorrectDecompressedSize() throws IOException {
    byte[] data = new byte[4096];
    new Random(42).nextBytes(data);

    for (CompressionCodecName codec : new CompressionCodecName[] {SNAPPY, ZSTD, LZ4_RAW, LZ4, GZIP, LZO, BROTLI}) {
      verifyIncorrectDecompressedSizeRejected(new CodecFactory(new Configuration(), pageSize), codec, data);
      verifyIncorrectDecompressedSizeRejected(
          CodecFactory.createDirectCodecFactory(
              new Configuration(), new DirectByteBufferAllocator(), pageSize),
          codec,
          data);
    }
  }

  @Test
  public void brotliRejectsNonEmptyOutputForZeroDeclaredSize() throws IOException {
    CodecFactory factory = new CodecFactory(new Configuration(), pageSize);
    try {
      BytesInput compressed = factory.getCompressor(BROTLI).compress(BytesInput.from(new byte[] {1}));
      assertThatThrownBy(() -> factory.getDecompressor(BROTLI).decompress(compressed, 0))
          .isInstanceOf(IOException.class);
    } finally {
      factory.release();
    }
  }

  @Test
  public void uncompressedRejectsIncorrectDecompressedSize() {
    assertThatThrownBy(() -> CodecFactory.NO_OP_DECOMPRESSOR.decompress(BytesInput.from(new byte[] {1}), 2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void directFactoryRejectsInvalidBrotliQuality() {
    Configuration conf = new Configuration();
    conf.setInt("compression.brotli.quality", 12);
    CodecFactory factory = CodecFactory.createDirectCodecFactory(conf, new DirectByteBufferAllocator(), pageSize);
    try {
      assertThatThrownBy(() -> factory.getCompressor(BROTLI))
          .isInstanceOf(BadConfigurationException.class)
          .hasMessageContaining("12");
    } finally {
      factory.release();
    }
  }

  private void verifyIncorrectDecompressedSizeRejected(CodecFactory factory, CompressionCodecName codec, byte[] data)
      throws IOException {
    try {
      BytesInput compressed = factory.getCompressor(codec).compress(BytesInput.from(data));
      byte[] compressedBytes = compressed.toByteArray();
      for (int expectedSize : new int[] {data.length - 1, data.length + 1}) {
        BytesInput copied = BytesInput.from(compressedBytes);
        assertThatThrownBy(() -> factory.getDecompressor(codec).decompress(copied, expectedSize))
            .as("Expected %s to reject decompressed size %s", codec, expectedSize)
            .isInstanceOfAny(IOException.class, RuntimeException.class);
      }
    } finally {
      factory.release();
    }
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
      // Sanity: the heap factory's leveled path uses the heap byte-array ZSTD compressor.
      assertThat(heap.getCompressor(ZSTD, 3))
          .as("heap factory should produce a heap ZstdBytesCompressor")
          .isInstanceOf(CodecFactory.ZstdBytesCompressor.class);

      // The direct factory must stay on its direct compressors for leveled ZSTD/SNAPPY,
      // not fall back to the heap byte-array implementations.
      BytesInputCompressor directZstd = direct.getCompressor(ZSTD, 3);
      assertThat(directZstd)
          .as("direct factory ZSTD(level) should not fall back to the heap compressor")
          .isNotInstanceOf(CodecFactory.ZstdBytesCompressor.class);
      assertThat(directZstd.getCodecName()).isEqualTo(ZSTD);
      assertThat(directZstd.getClass())
          .as("leveled ZSTD should use the same direct compressor type as the no-level path")
          .isEqualTo(direct.getCompressor(ZSTD).getClass());

      BytesInputCompressor directSnappy = direct.getCompressor(SNAPPY, 5);
      assertThat(directSnappy)
          .as("direct factory SNAPPY(level) should not fall back to the heap compressor")
          .isNotInstanceOf(CodecFactory.SnappyBytesCompressor.class);
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
      assertThatThrownBy(() -> direct.getCompressor(ZSTD, 23)).isInstanceOf(BadConfigurationException.class);
    } finally {
      direct.release();
    }
  }
}
