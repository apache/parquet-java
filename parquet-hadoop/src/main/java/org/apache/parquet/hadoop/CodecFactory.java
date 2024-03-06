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
import org.apache.parquet.hadoop.codec.ZstandardCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

public class CodecFactory implements CompressionCodecFactory {

  protected static final Map<String, CompressionCodec> CODEC_BY_NAME =
      Collections.synchronizedMap(new HashMap<String, CompressionCodec>());

  private final Map<CompressionCodecName, BytesCompressor> compressors = new HashMap<>();
  private final Map<CompressionCodecName, BytesDecompressor> decompressors = new HashMap<>();

  protected final ParquetConfiguration configuration;
  protected final int pageSize;

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
    this.configuration = configuration;
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
      // avoid off-heap memory fragmentation issue, see https://issues.apache.org/jira/browse/PARQUET-2160.
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
    CompressionCodec codec = getCodec(codecName);
    return codec == null ? NO_OP_COMPRESSOR : new HeapBytesCompressor(codecName, codec);
  }

  protected BytesDecompressor createDecompressor(CompressionCodecName codecName) {
    CompressionCodec codec = getCodec(codecName);
    return codec == null ? NO_OP_DECOMPRESSOR : new HeapBytesDecompressor(codec);
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
          ReflectionUtils.newInstance(codecClass, ConfigurationUtil.createHadoopConfiguration(configuration));
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
        level = configuration.get("zlib.compress.level");
        break;
      case BROTLI:
        level = configuration.get("compression.brotli.quality");
        break;
      case ZSTD:
        level = configuration.get("parquet.compression.codec.zstd.level");
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
}
