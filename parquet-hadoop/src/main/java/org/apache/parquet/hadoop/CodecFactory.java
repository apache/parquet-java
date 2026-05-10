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

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.NoPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.codec.Lz4RawCodec;
import org.apache.parquet.hadoop.codec.ZstandardCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.xerial.snappy.Snappy;

public class CodecFactory implements CompressionCodecFactory {

  protected static final Map<String, CompressionCodec> CODEC_BY_NAME =
      Collections.synchronizedMap(new HashMap<String, CompressionCodec>());

  private final Map<CompressionCodecName, BytesCompressor> compressors = new HashMap<>();
  private final Map<CompressionCodecName, BytesDecompressor> decompressors = new HashMap<>();

  protected final ParquetConfiguration conf;
  protected final int pageSize;

  // May be null if parquetConfiguration is not an instance of org.apache.parquet.conf.HadoopParquetConfiguration
  @Deprecated
  protected final Configuration configuration;

  static final BytesDecompressor NO_OP_DECOMPRESSOR = new BytesDecompressor() {
    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) {
      Preconditions.checkArgument(
          compressedSize == decompressedSize,
          "Non-compressed data did not have matching compressed and decompressed sizes.");
      Preconditions.checkArgument(
          input.remaining() >= compressedSize, "Not enough bytes available in the input buffer");
      int origLimit = input.limit();
      input.limit(input.position() + compressedSize);
      output.put(input);
      input.limit(origLimit);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) {
      return bytes;
    }

    @Override
    public void release() {}
  };

  static final BytesCompressor NO_OP_COMPRESSOR = new BytesCompressor() {
    @Override
    public BytesInput compress(BytesInput bytes) {
      return bytes;
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.UNCOMPRESSED;
    }

    @Override
    public void release() {}
  };

  /**
   * Create a new codec factory.
   *
   * @param configuration used to pass compression codec configuration information
   * @param pageSize      the expected page size, does not set a hard limit, currently just
   *                      used to set the initial size of the output stream used when
   *                      compressing a buffer. If this factory is only used to construct
   *                      decompressors this parameter has no impact on the function of the factory
   */
  public CodecFactory(Configuration configuration, int pageSize) {
    this(new HadoopParquetConfiguration(configuration), pageSize);
  }

  /**
   * Create a new codec factory.
   *
   * @param configuration used to pass compression codec configuration information
   * @param pageSize      the expected page size, does not set a hard limit, currently just
   *                      used to set the initial size of the output stream used when
   *                      compressing a buffer. If this factory is only used to construct
   *                      decompressors this parameter has no impact on the function of the factory
   */
  public CodecFactory(ParquetConfiguration configuration, int pageSize) {
    if (configuration instanceof HadoopParquetConfiguration) {
      this.configuration = ((HadoopParquetConfiguration) configuration).getConfiguration();
    } else {
      this.configuration = null;
    }
    this.conf = configuration;
    this.pageSize = pageSize;
  }

  /**
   * Create a codec factory that will provide compressors and decompressors
   * that will work natively with ByteBuffers backed by direct memory.
   *
   * @param config    configuration options for different compression codecs
   * @param allocator an allocator for creating result buffers during compression
   *                  and decompression, must provide buffers backed by Direct
   *                  memory and return true for the isDirect() method
   *                  on the ByteBufferAllocator interface
   * @param pageSize  the default page size. This does not set a hard limit on the
   *                  size of buffers that can be compressed, but performance may
   *                  be improved by setting it close to the expected size of buffers
   *                  (in the case of parquet, pages) that will be compressed. This
   *                  setting is unused in the case of decompressing data, as parquet
   *                  always records the uncompressed size of a buffer. If this
   *                  CodecFactory is only going to be used for decompressors, this
   *                  parameter will not impact the function of the factory.
   * @return a configured direct codec factory
   */
  public static CodecFactory createDirectCodecFactory(
      Configuration config, ByteBufferAllocator allocator, int pageSize) {
    return new DirectCodecFactory(config, allocator, pageSize);
  }

  class HeapBytesDecompressor extends BytesDecompressor {

    private final CompressionCodec codec;
    private final Decompressor decompressor;

    HeapBytesDecompressor(CompressionCodec codec) {
      this.codec = Objects.requireNonNull(codec);
      decompressor = CodecPool.getDecompressor(codec);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      final BytesInput decompressed;
      if (decompressor != null) {
        decompressor.reset();
      }
      InputStream is = codec.createInputStream(bytes.toInputStream(), decompressor);

      // Eagerly materialize the decompressed stream for codecs that require all input in a single buffer.
      // ZSTD: releases off-heap resources early to avoid fragmentation (see parquet-format#398).
      // LZ4_RAW: requires one-shot decompression; the lazy StreamBytesInput.writeInto() path reads via
      // Channels.newChannel() in ~8KB chunks, causing the decompressor to be called with an undersized
      // output buffer (see #3478).
      if (codec instanceof ZstandardCodec || codec instanceof Lz4RawCodec) {
        decompressed = BytesInput.copy(BytesInput.from(is, decompressedSize));
        is.close();
      } else {
        decompressed = BytesInput.from(is, decompressedSize);
      }
      return decompressed;
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      Preconditions.checkArgument(
          input.remaining() >= compressedSize, "Not enough bytes available in the input buffer");
      int origLimit = input.limit();
      int origPosition = input.position();
      input.limit(origPosition + compressedSize);
      ByteBuffer decompressed =
          decompress(BytesInput.from(input), decompressedSize).toByteBuffer();
      output.put(decompressed);
      input.limit(origLimit);
      input.position(origPosition + compressedSize);
    }

    public void release() {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }

  /**
   * Encapsulates the logic around hadoop compression
   */
  class HeapBytesCompressor extends BytesCompressor {

    private final CompressionCodec codec;
    private final Compressor compressor;
    private final ByteArrayOutputStream compressedOutBuffer;
    private final CompressionCodecName codecName;

    HeapBytesCompressor(CompressionCodecName codecName, CompressionCodec codec) {
      this.codecName = codecName;
      this.codec = Objects.requireNonNull(codec);
      this.compressor = CodecPool.getCompressor(codec);
      this.compressedOutBuffer = new ByteArrayOutputStream(pageSize);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      compressedOutBuffer.reset();
      if (compressor != null) {
        // null compressor for non-native gzip
        compressor.reset();
      }
      try (CompressionOutputStream cos = codec.createOutputStream(compressedOutBuffer, compressor)) {
        bytes.writeAllTo(cos);
        cos.finish();
      }
      return BytesInput.from(compressedOutBuffer);
    }

    @Override
    public void release() {
      if (compressor != null) {
        CodecPool.returnCompressor(compressor);
      }
    }

    public CompressionCodecName getCodecName() {
      return codecName;
    }
  }

  @Override
  public BytesCompressor getCompressor(CompressionCodecName codecName) {
    BytesCompressor comp = compressors.get(codecName);
    if (comp == null) {
      comp = createCompressor(codecName);
      compressors.put(codecName, comp);
    }
    return comp;
  }

  @Override
  public BytesDecompressor getDecompressor(CompressionCodecName codecName) {
    BytesDecompressor decomp = decompressors.get(codecName);
    if (decomp == null) {
      decomp = createDecompressor(codecName);
      decompressors.put(codecName, decomp);
    }
    return decomp;
  }

  protected BytesCompressor createCompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return NO_OP_COMPRESSOR;
      case SNAPPY:
        return new SnappyBytesCompressor();
      case ZSTD:
        BufferPool zstdCompressPool = conf.getBoolean(
            ZstandardCodec.PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED,
            ZstandardCodec.DEFAULT_PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED)
            ? RecyclingBufferPool.INSTANCE : NoPool.INSTANCE;
        return new ZstdBytesCompressor(
            conf.getInt(
                ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, ZstandardCodec.DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL),
            conf.getInt(
                ZstandardCodec.PARQUET_COMPRESS_ZSTD_WORKERS, ZstandardCodec.DEFAULTPARQUET_COMPRESS_ZSTD_WORKERS),
            pageSize,
            zstdCompressPool);
      case LZ4_RAW:
        return new Lz4RawBytesCompressor();
      case GZIP:
        int gzipLevel = conf.getInt(
            "zlib.compress.level", Deflater.DEFAULT_COMPRESSION);
        return new GzipBytesCompressor(gzipLevel, pageSize);
      default:
        CompressionCodec codec = getCodec(codecName);
        return codec == null ? NO_OP_COMPRESSOR : new HeapBytesCompressor(codecName, codec);
    }
  }

  protected BytesDecompressor createDecompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return NO_OP_DECOMPRESSOR;
      case SNAPPY:
        return new SnappyBytesDecompressor();
      case ZSTD:
        BufferPool zstdDecompressPool = conf.getBoolean(
            ZstandardCodec.PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED,
            ZstandardCodec.DEFAULT_PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED)
            ? RecyclingBufferPool.INSTANCE : NoPool.INSTANCE;
        return new ZstdBytesDecompressor(zstdDecompressPool);
      case LZ4_RAW:
        return new Lz4RawBytesDecompressor();
      case GZIP:
        return new GzipBytesDecompressor();
      default:
        CompressionCodec codec = getCodec(codecName);
        return codec == null ? NO_OP_DECOMPRESSOR : new HeapBytesDecompressor(codec);
    }
  }

  /**
   * @param codecName the requested codec
   * @return the corresponding hadoop codec. null if UNCOMPRESSED
   */
  protected CompressionCodec getCodec(CompressionCodecName codecName) {
    String codecClassName = codecName.getHadoopCompressionCodecClassName();
    if (codecClassName == null) {
      return null;
    }
    String codecCacheKey = this.cacheKey(codecName);
    CompressionCodec codec = CODEC_BY_NAME.get(codecCacheKey);
    if (codec != null) {
      return codec;
    }

    try {
      Class<?> codecClass;
      try {
        codecClass = Class.forName(codecClassName);
      } catch (ClassNotFoundException e) {
        // Try to load the class using the job classloader
        codecClass = new Configuration(false).getClassLoader().loadClass(codecClassName);
      }
      codec = (CompressionCodec)
          ReflectionUtils.newInstance(codecClass, ConfigurationUtil.createHadoopConfiguration(conf));
      CODEC_BY_NAME.put(codecCacheKey, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
    }
  }

  private String cacheKey(CompressionCodecName codecName) {
    String level = null;
    switch (codecName) {
      case BROTLI:
        level = conf.get("compression.brotli.quality");
        break;
      default:
        // compression level is not supported; ignore it
    }
    String codecClass = codecName.getHadoopCompressionCodecClassName();
    return level == null ? codecClass : codecClass + ":" + level;
  }

  @Override
  public void release() {
    for (BytesCompressor compressor : compressors.values()) {
      compressor.release();
    }
    compressors.clear();
    for (BytesDecompressor decompressor : decompressors.values()) {
      decompressor.release();
    }
    decompressors.clear();
  }

  /**
   * @deprecated will be removed in 2.0.0; use CompressionCodecFactory.BytesInputCompressor instead.
   */
  @Deprecated
  public abstract static class BytesCompressor implements CompressionCodecFactory.BytesInputCompressor {
    public abstract BytesInput compress(BytesInput bytes) throws IOException;

    public abstract CompressionCodecName getCodecName();

    public abstract void release();
  }

  /**
   * @deprecated will be removed in 2.0.0; use CompressionCodecFactory.BytesInputDecompressor instead.
   */
  @Deprecated
  public abstract static class BytesDecompressor implements CompressionCodecFactory.BytesInputDecompressor {
    public abstract BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException;

    public abstract void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException;

    public abstract void release();
  }

  // ---- Optimized Snappy compressor/decompressor using direct JNI calls ----

  /**
   * Compresses using Snappy's byte-array JNI API directly, bypassing the Hadoop
   * stream abstraction. This avoids intermediate direct ByteBuffer copies and
   * reduces the compression to a single native call per page.
   */
  static class SnappyBytesCompressor extends BytesCompressor {
    private byte[] outputBuffer;

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      int maxLen = Snappy.maxCompressedLength(input.length);
      if (outputBuffer == null || outputBuffer.length < maxLen) {
        outputBuffer = new byte[maxLen];
      }
      int compressed = Snappy.compress(input, 0, input.length, outputBuffer, 0);
      return BytesInput.from(outputBuffer, 0, compressed);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.SNAPPY;
    }

    @Override
    public void release() {
      outputBuffer = null;
    }
  }

  /**
   * Decompresses using Snappy's byte-array JNI API directly.
   */
  static class SnappyBytesDecompressor extends BytesDecompressor {
    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      byte[] input = bytes.toByteArray();
      byte[] output = new byte[decompressedSize];
      Snappy.uncompress(input, 0, input.length, output, 0);
      return BytesInput.from(output);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      byte[] inputBytes = new byte[compressedSize];
      input.get(inputBytes);
      byte[] outputBytes = new byte[decompressedSize];
      Snappy.uncompress(inputBytes, 0, compressedSize, outputBytes, 0);
      output.put(outputBytes);
    }

    @Override
    public void release() {}
  }

  // ---- Optimized ZSTD compressor/decompressor using zstd-jni streaming API directly ----

  /**
   * Compresses using zstd-jni's {@link ZstdOutputStreamNoFinalizer} directly,
   * bypassing the Hadoop codec framework ({@code ZstandardCodec}, {@code CodecPool},
   * {@code CompressionOutputStream} wrapper). Uses a configurable {@link BufferPool}
   * (defaulting to {@link RecyclingBufferPool}) for the internal 128KB output buffer,
   * matching the streaming API's natural buffer size. The buffer pool strategy is
   * controlled by the {@code parquet.compression.codec.zstd.bufferPool.enabled} config.
   * This avoids the overhead of Hadoop codec instantiation and compressor pool management
   * while using the same underlying ZSTD streaming path, which is well-optimized for all
   * input sizes including large pages (256KB+).
   */
  static class ZstdBytesCompressor extends BytesCompressor {
    private final int level;
    private final int workers;
    private final BufferPool bufferPool;
    private final ByteArrayOutputStream compressedOutBuffer;

    ZstdBytesCompressor(int level, int workers, int pageSize, BufferPool bufferPool) {
      this.level = level;
      this.workers = workers;
      this.bufferPool = bufferPool;
      this.compressedOutBuffer = new ByteArrayOutputStream(pageSize);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      compressedOutBuffer.reset();
      try (ZstdOutputStreamNoFinalizer zos =
          new ZstdOutputStreamNoFinalizer(compressedOutBuffer, bufferPool, level)) {
        if (workers > 0) {
          zos.setWorkers(workers);
        }
        bytes.writeAllTo(zos);
      }
      return BytesInput.from(compressedOutBuffer);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.ZSTD;
    }

    @Override
    public void release() {
      // ByteArrayOutputStream does not hold native resources
    }
  }

  /**
   * Decompresses using zstd-jni's {@link ZstdInputStreamNoFinalizer} directly,
   * bypassing the Hadoop codec framework. Uses a configurable {@link BufferPool}
   * for internal buffers, matching the streaming decompression path. The buffer pool
   * strategy is controlled by the {@code parquet.compression.codec.zstd.bufferPool.enabled}
   * config. Reads the full decompressed output in a single pass via
   * {@link InputStream#readNBytes(int)}.
   */
  static class ZstdBytesDecompressor extends BytesDecompressor {
    private final BufferPool bufferPool;

    ZstdBytesDecompressor(BufferPool bufferPool) {
      this.bufferPool = bufferPool;
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      try (ZstdInputStreamNoFinalizer zis =
          new ZstdInputStreamNoFinalizer(bytes.toInputStream(), bufferPool)) {
        byte[] output = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = zis.read(output, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of ZSTD stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        return BytesInput.from(output);
      }
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      byte[] inputBytes = new byte[compressedSize];
      input.get(inputBytes);
      ByteArrayInputStream bais = new ByteArrayInputStream(inputBytes);
      try (ZstdInputStreamNoFinalizer zis =
          new ZstdInputStreamNoFinalizer(bais, bufferPool)) {
        byte[] outputBytes = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = zis.read(outputBytes, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of ZSTD stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        output.put(outputBytes);
      }
    }

    @Override
    public void release() {
      // No persistent resources - streams are closed per call
    }
  }

  // ---- Optimized LZ4_RAW compressor/decompressor using airlift LZ4 directly ----

  /**
   * Compresses using airlift's LZ4 compressor directly with heap ByteBuffers,
   * bypassing the Hadoop stream abstraction and NonBlockedCompressor's direct
   * buffer copies.
   */
  static class Lz4RawBytesCompressor extends BytesCompressor {
    private final io.airlift.compress.lz4.Lz4Compressor compressor =
        new io.airlift.compress.lz4.Lz4Compressor();
    private byte[] outputBuffer;

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      int maxLen = compressor.maxCompressedLength(input.length);
      if (outputBuffer == null || outputBuffer.length < maxLen) {
        outputBuffer = new byte[maxLen];
      }
      ByteBuffer inputBuf = ByteBuffer.wrap(input);
      ByteBuffer outputBuf = ByteBuffer.wrap(outputBuffer);
      compressor.compress(inputBuf, outputBuf);
      int compressedSize = outputBuf.position();
      return BytesInput.from(outputBuffer, 0, compressedSize);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.LZ4_RAW;
    }

    @Override
    public void release() {
      outputBuffer = null;
    }
  }

  /**
   * Decompresses using airlift's LZ4 decompressor directly with heap ByteBuffers.
   */
  static class Lz4RawBytesDecompressor extends BytesDecompressor {
    private final io.airlift.compress.lz4.Lz4Decompressor decompressor =
        new io.airlift.compress.lz4.Lz4Decompressor();

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      byte[] input = bytes.toByteArray();
      byte[] output = new byte[decompressedSize];
      ByteBuffer inputBuf = ByteBuffer.wrap(input);
      ByteBuffer outputBuf = ByteBuffer.wrap(output);
      decompressor.decompress(inputBuf, outputBuf);
      return BytesInput.from(output);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      byte[] inputBytes = new byte[compressedSize];
      input.get(inputBytes);
      byte[] outputBytes = new byte[decompressedSize];
      ByteBuffer inputBuf = ByteBuffer.wrap(inputBytes);
      ByteBuffer outputBuf = ByteBuffer.wrap(outputBytes);
      decompressor.decompress(inputBuf, outputBuf);
      output.put(outputBytes);
    }

    @Override
    public void release() {}
  }

  // ---- Optimized GZIP compressor/decompressor using Deflater/Inflater directly ----

  /** GZIP magic number: 0x1f 0x8b. */
  private static final int GZIP_MAGIC = 0x8b1f;

  /** Minimal 10-byte GZIP header: magic, method=8 (deflate), flags=0, mtime=0, xfl=0, os=0. */
  private static final byte[] GZIP_HEADER = {
    0x1f, (byte) 0x8b, // magic
    0x08, // method: deflate
    0x00, // flags: none
    0x00, 0x00, 0x00, 0x00, // mtime: not set
    0x00, // extra flags
    0x00 // OS: FAT (matches Java's GZIPOutputStream default)
  };

  /**
   * Compresses using {@link Deflater} directly with a reusable instance,
   * bypassing Hadoop's GzipCodec and the stream overhead of
   * {@link java.util.zip.GZIPOutputStream}. The Deflater is kept across
   * calls and reset via {@link Deflater#reset()}, avoiding native zlib
   * state allocation per page. Writes a minimal GZIP header and trailer
   * (CRC32 + original size) manually.
   *
   * <p>Note: this implementation always uses Java's built-in {@link Deflater}
   * (java.util.zip / JDK zlib). It does <em>not</em> use Hadoop native libraries,
   * so hardware-accelerated compression via Intel ISA-L will not be used even if
   * the native libraries are installed. The overhead reduction from bypassing the
   * Hadoop codec framework typically outweighs the ISA-L advantage for the page
   * sizes used by Parquet.
   */
  static class GzipBytesCompressor extends BytesCompressor {
    private final Deflater deflater;
    private final CRC32 crc = new CRC32();
    private final ByteArrayOutputStream baos;

    GzipBytesCompressor(int level, int pageSize) {
      this.deflater = new Deflater(level, true);
      this.baos = new ByteArrayOutputStream(pageSize);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();

      deflater.reset();
      crc.reset();
      crc.update(input);

      baos.reset();
      // GZIP header
      baos.write(GZIP_HEADER);

      // Deflate
      deflater.setInput(input);
      deflater.finish();
      byte[] buf = new byte[4096];
      while (!deflater.finished()) {
        int n = deflater.deflate(buf);
        baos.write(buf, 0, n);
      }

      // GZIP trailer: CRC32 + original size (little-endian)
      writeInt(baos, (int) crc.getValue());
      writeInt(baos, input.length);

      return BytesInput.from(baos);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.GZIP;
    }

    @Override
    public void release() {
      deflater.end();
    }
  }

  /**
   * Decompresses using {@link Inflater} directly with a reusable instance,
   * bypassing Hadoop's GzipCodec and the stream overhead of
   * {@link java.util.zip.GZIPInputStream}. Skips the GZIP header, inflates
   * into the output buffer, and verifies the CRC32 + size trailer.
   *
   * <p>Note: this implementation always uses Java's built-in {@link Inflater}
   * (java.util.zip / JDK zlib). It does <em>not</em> use Hadoop native libraries,
   * so hardware-accelerated decompression via Intel ISA-L will not be used even if
   * the native libraries are installed.
   */
  static class GzipBytesDecompressor extends BytesDecompressor {
    private final Inflater inflater = new Inflater(true);
    private final CRC32 crc = new CRC32();

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize)
        throws IOException {
      byte[] compressed = bytes.toByteArray();
      int headerLen = readGzipHeaderLength(compressed);

      inflater.reset();
      inflater.setInput(
          compressed, headerLen, compressed.length - headerLen - 8);

      byte[] output = new byte[decompressedSize];
      try {
        int inflated = 0;
        while (inflated < decompressedSize) {
          int n = inflater.inflate(
              output, inflated, decompressedSize - inflated);
          if (n == 0 && inflater.finished()) {
            break;
          }
          if (n == 0 && inflater.needsInput()) {
            throw new IOException(
                "Unexpected end of GZIP stream at offset "
                    + inflated + " of " + decompressedSize);
          }
          inflated += n;
        }
      } catch (java.util.zip.DataFormatException e) {
        throw new IOException("Invalid GZIP data", e);
      }

      // Verify CRC32 and original size from trailer
      int trailerOffset = compressed.length - 8;
      int expectedCrc = readInt(compressed, trailerOffset);
      int expectedSize = readInt(compressed, trailerOffset + 4);

      crc.reset();
      crc.update(output);
      if ((int) crc.getValue() != expectedCrc) {
        throw new IOException("GZIP CRC32 mismatch");
      }
      if (decompressedSize != (expectedSize & 0xFFFFFFFFL)) {
        throw new IOException("GZIP size mismatch");
      }

      return BytesInput.from(output);
    }

    @Override
    public void decompress(
        ByteBuffer input, int compressedSize,
        ByteBuffer output, int decompressedSize) throws IOException {
      byte[] inputBytes = new byte[compressedSize];
      input.get(inputBytes);
      BytesInput result = decompress(
          BytesInput.from(inputBytes), decompressedSize);
      output.put(result.toByteArray());
    }

    @Override
    public void release() {
      inflater.end();
    }
  }

  /**
   * Reads the length of a GZIP header, handling optional extra, name,
   * comment, and header CRC fields per RFC 1952.
   */
  private static int readGzipHeaderLength(byte[] data) throws IOException {
    if (data.length < 10
        || (data[0] & 0xFF) != 0x1f
        || (data[1] & 0xFF) != 0x8b) {
      throw new IOException("Not a GZIP stream");
    }
    int flags = data[3] & 0xFF;
    int offset = 10;

    if ((flags & 0x04) != 0) { // FEXTRA
      if (offset + 2 > data.length) {
        throw new IOException("Truncated GZIP FEXTRA");
      }
      int extraLen = (data[offset] & 0xFF)
          | ((data[offset + 1] & 0xFF) << 8);
      offset += 2 + extraLen;
    }
    if ((flags & 0x08) != 0) { // FNAME
      while (offset < data.length && data[offset] != 0) {
        offset++;
      }
      offset++; // skip null terminator
    }
    if ((flags & 0x10) != 0) { // FCOMMENT
      while (offset < data.length && data[offset] != 0) {
        offset++;
      }
      offset++; // skip null terminator
    }
    if ((flags & 0x02) != 0) { // FHCRC
      offset += 2;
    }
    return offset;
  }

  /** Writes a 32-bit integer in little-endian byte order. */
  private static void writeInt(ByteArrayOutputStream out, int value) {
    out.write(value & 0xFF);
    out.write((value >> 8) & 0xFF);
    out.write((value >> 16) & 0xFF);
    out.write((value >> 24) & 0xFF);
  }

  /** Reads a 32-bit little-endian integer from a byte array. */
  private static int readInt(byte[] data, int offset) {
    return (data[offset] & 0xFF)
        | ((data[offset + 1] & 0xFF) << 8)
        | ((data[offset + 2] & 0xFF) << 16)
        | ((data[offset + 3] & 0xFF) << 24);
  }
}
