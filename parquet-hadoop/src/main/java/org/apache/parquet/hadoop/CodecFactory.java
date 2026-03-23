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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.codec.ZstandardCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

public class CodecFactory implements CompressionCodecFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CodecFactory.class);

  protected static final Map<String, CompressionCodec> CODEC_BY_NAME =
      Collections.synchronizedMap(new HashMap<String, CompressionCodec>());

  static final String GZIP_COMPRESS_LEVEL = "zlib.compress.level";
  static final String BROTLI_COMPRESS_QUALITY = "compression.brotli.quality";

  private final Map<String, BytesCompressor> compressors = new HashMap<>();
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

      // We need to explicitly close the ZstdDecompressorStream here to release the resources it holds to
      // avoid off-heap memory fragmentation issue, see https://github.com/apache/parquet-format/issues/398.
      // This change will load the decompressor stream into heap a little earlier, since the problem it solves
      // only happens in the ZSTD codec, so this modification is only made for ZSTD streams.
      if (codec instanceof ZstandardCodec) {
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
    String key = cacheKey(codecName);
    BytesCompressor comp = compressors.get(key);
    if (comp == null) {
      comp = createCompressor(codecName);
      compressors.put(key, comp);
    }
    return comp;
  }

  @Override
  public BytesCompressor getCompressor(CompressionCodecName codecName, int level) {
    String key = cacheKey(codecName, level);
    BytesCompressor comp = compressors.get(key);
    if (comp == null) {
      comp = createCompressorAtLevel(codecName, level);
      compressors.put(key, comp);
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
    return compressorForCodec(codecName, getCodec(codecName));
  }

  protected BytesDecompressor createDecompressor(CompressionCodecName codecName) {
    CompressionCodec codec = getCodec(codecName);
    return codec == null ? NO_OP_DECOMPRESSOR : new HeapBytesDecompressor(codec);
  }

  private BytesCompressor createCompressorAtLevel(CompressionCodecName codecName, int level) {
    return compressorForCodec(codecName, getCodecAtLevel(codecName, level));
  }

  private BytesCompressor compressorForCodec(CompressionCodecName codecName, CompressionCodec codec) {
    return codec == null ? NO_OP_COMPRESSOR : new HeapBytesCompressor(codecName, codec);
  }

  private static void validateZstdLevel(int level) {
    if (level < 1 || level > 22) {
      throw new BadConfigurationException("Unsupported ZSTD compression level: " + level
          + ". Valid range is 1 (fastest) to 22 (best compression).");
    }
  }

  private static void validateBrotliLevel(int level) {
    if (level < 0 || level > 11) {
      throw new BadConfigurationException("Unsupported Brotli compression level: " + level
          + ". Valid range is 0 (fastest) to 11 (best compression).");
    }
  }

  private static void validateGzipLevel(int level) {
    if (level != -1 && (level < 0 || level > 9)) {
      throw new BadConfigurationException("Unsupported GZIP compression level: " + level
          + ". Valid range is 0 (no compression) to 9 (best compression), or -1 for default.");
    }
  }

  private static ZlibCompressor.CompressionLevel zlibCompressionLevel(int level) {
    switch (level) {
      case -1: return ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION;
      case 0:  return ZlibCompressor.CompressionLevel.NO_COMPRESSION;
      case 1:  return ZlibCompressor.CompressionLevel.BEST_SPEED;
      case 2:  return ZlibCompressor.CompressionLevel.TWO;
      case 3:  return ZlibCompressor.CompressionLevel.THREE;
      case 4:  return ZlibCompressor.CompressionLevel.FOUR;
      case 5:  return ZlibCompressor.CompressionLevel.FIVE;
      case 6:  return ZlibCompressor.CompressionLevel.SIX;
      case 7:  return ZlibCompressor.CompressionLevel.SEVEN;
      case 8:  return ZlibCompressor.CompressionLevel.EIGHT;
      case 9:  return ZlibCompressor.CompressionLevel.BEST_COMPRESSION;
      default: throw new BadConfigurationException("Unsupported GZIP compression level: " + level
          + ". Valid range is 0 (no compression) to 9 (best compression), or -1 for default.");
    }
  }

  /**
   * Returns a {@link CompressionCodec} instance configured at the specified compression level.
   * A level-specific {@link Configuration} snapshot is built so that the codec is initialized
   * with the requested level rather than the global configuration value.
   * For codecs that do not support levels the method falls back to {@link #getCodec(CompressionCodecName)}.
   */
  private CompressionCodec getCodecAtLevel(CompressionCodecName codecName, int level) {
    String codecClassName = codecName.getHadoopCompressionCodecClassName();
    if (codecClassName == null) {
      return null;
    }
    String key = cacheKey(codecName, level);
    CompressionCodec codec = CODEC_BY_NAME.get(key);
    if (codec != null) {
      return codec;
    }
    // Build a Configuration snapshot with the level explicitly set for this codec.
    Configuration levelConf = new Configuration(ConfigurationUtil.createHadoopConfiguration(conf));
    switch (codecName) {
      case ZSTD:
        validateZstdLevel(level);
        levelConf.setInt(ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, level);
        break;
      case GZIP:
        validateGzipLevel(level);
        levelConf.setEnum(GZIP_COMPRESS_LEVEL, zlibCompressionLevel(level));
        break;
      case BROTLI:
        validateBrotliLevel(level);
        levelConf.setInt(BROTLI_COMPRESS_QUALITY, level);
        break;
      default:
        // Codec does not support levels; fall back to the default codec instance.
        LOG.warn("Compression level {} is not supported for codec {} and will be ignored.", level, codecName);
        return getCodec(codecName);
    }
    try {
      Class<?> codecClass;
      try {
        codecClass = Class.forName(codecClassName);
      } catch (ClassNotFoundException e) {
        codecClass = new Configuration(false).getClassLoader().loadClass(codecClassName);
      }
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, levelConf);
      CODEC_BY_NAME.put(key, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
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
      case GZIP:
        level = conf.get(GZIP_COMPRESS_LEVEL);
        break;
      case BROTLI:
        level = conf.get(BROTLI_COMPRESS_QUALITY);
        break;
      case ZSTD:
        level = conf.get("parquet.compression.codec.zstd.level");
        break;
      default:
        // compression level is not supported; ignore it
    }
    String codecClass = codecName.getHadoopCompressionCodecClassName();
    return level == null ? codecClass : codecClass + ":" + level;
  }

  private String cacheKey(CompressionCodecName codecName, int level) {
    String codecClass = codecName.getHadoopCompressionCodecClassName();
    return (codecClass == null ? codecName.name() : codecClass) + ":" + level;
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
}
