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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoHadoopStreams;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.codec.ZstandardCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

public class CodecFactory implements CompressionCodecFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CodecFactory.class);

  protected static final Map<String, CompressionCodec> CODEC_BY_NAME =
      Collections.synchronizedMap(new HashMap<String, CompressionCodec>());

  private final Map<CompressionCodecName, BytesCompressor> compressors = new HashMap<>();
  private final Map<CompressionCodecName, BytesDecompressor> decompressors = new HashMap<>();

  protected final ParquetConfiguration conf;
  protected final int pageSize;

  // May be null if parquetConfiguration is not an instance of org.apache.parquet.conf.HadoopParquetConfiguration
  @Deprecated
  protected final Configuration configuration;

  /**
   * Reflection-based helper for brotli4j (runtime-only dependency).
   * Initialized eagerly at class-load time; all fields are null if
   * brotli4j is not on the classpath.
   *
   * <p>Uses {@code Encoder.compress(byte[], Encoder.Parameters)} for compression and
   * {@code Decoder.decompress(byte[], int, int)} for decompression — the latter returns
   * {@code byte[]} directly and avoids loading {@code DirectDecompress} which references
   * {@code io.netty.buffer.ByteBuf} (optional Netty dependency not on our classpath).
   */
  static final class Brotli4j {
    static final boolean AVAILABLE;
    // Encoder.compress(byte[], Object/*Encoder.Parameters*/) -> byte[]
    private static final Method COMPRESS;
    // Decoder.decompress(byte[], int/*offset*/, int/*length*/) -> byte[]
    private static final Method DECOMPRESS;
    // Encoder.Parameters class
    private static final Class<?> PARAMS_CLASS;
    // Encoder.Parameters.setQuality(int) -> Encoder.Parameters
    private static final Method SET_QUALITY;

    static {
      boolean loaded = false;
      Method compress = null, decompress = null, setQuality = null;
      Class<?> paramsClass = null;
      try {
        // Load native library
        Class<?> loader = Class.forName("com.aayushatharva.brotli4j.Brotli4jLoader");
        loader.getMethod("ensureAvailability").invoke(null);

        // Encoder.compress(byte[], Encoder.Parameters) -> byte[]
        paramsClass = Class.forName("com.aayushatharva.brotli4j.encoder.Encoder$Parameters");
        Class<?> encoder = Class.forName("com.aayushatharva.brotli4j.encoder.Encoder");
        compress = encoder.getMethod("compress", byte[].class, paramsClass);

        // Decoder.decompress(byte[], int, int) -> byte[]
        // This avoids loading DirectDecompress which references io.netty.buffer.ByteBuf
        Class<?> decoder = Class.forName("com.aayushatharva.brotli4j.decoder.Decoder");
        decompress = decoder.getMethod("decompress", byte[].class, int.class, int.class);

        // Encoder.Parameters.setQuality(int) -> Encoder.Parameters
        setQuality = paramsClass.getMethod("setQuality", int.class);

        loaded = true;
      } catch (Throwable t) {
        // brotli4j not available — BROTLI will fall through to Hadoop codec path
        LOG.info("brotli4j not available, BROTLI codec will use Hadoop codec path: {}", t.toString());
      }
      AVAILABLE = loaded;
      COMPRESS = compress;
      DECOMPRESS = decompress;
      PARAMS_CLASS = paramsClass;
      SET_QUALITY = setQuality;
    }

    /** Create an {@code Encoder.Parameters} instance with the given quality. */
    static Object newParams(int quality) {
      try {
        Object params = PARAMS_CLASS.getConstructor().newInstance();
        SET_QUALITY.invoke(params, quality);
        return params;
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Failed to create Brotli encoder parameters", e);
      }
    }

    /** Compress using {@code Encoder.compress(byte[], Encoder.Parameters)}. */
    static byte[] compress(byte[] input, Object params) throws IOException {
      try {
        return (byte[]) COMPRESS.invoke(null, input, params);
      } catch (ReflectiveOperationException e) {
        throw new IOException("Brotli compression failed", e);
      }
    }

    /** Decompress using {@code Decoder.decompress(byte[], offset, length)}. */
    static byte[] decompress(byte[] input) throws IOException {
      try {
        return (byte[]) DECOMPRESS.invoke(null, input, 0, input.length);
      } catch (ReflectiveOperationException e) {
        throw new IOException("Brotli decompression failed", e);
      }
    }
  }

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
      decompressed = BytesInput.from(is, decompressedSize);
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
        return new ZstdBytesCompressor(
            conf.getInt(
                ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL,
                ZstandardCodec.DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL),
            conf.getInt(
                ZstandardCodec.PARQUET_COMPRESS_ZSTD_WORKERS,
                ZstandardCodec.DEFAULTPARQUET_COMPRESS_ZSTD_WORKERS));
      case LZ4_RAW:
        return new Lz4RawBytesCompressor();
      case GZIP:
        int gzipLevel = conf.getInt("zlib.compress.level", Deflater.DEFAULT_COMPRESSION);
        return new GzipBytesCompressor(gzipLevel, pageSize);
      case LZO:
        return new LzoBytesCompressor(pageSize);
      case BROTLI:
        if (Brotli4j.AVAILABLE) {
          int brotliQuality = conf.getInt("compression.brotli.quality", 1);
          return new BrotliBytesCompressor(brotliQuality);
        }
        // fall through to Hadoop codec path
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
        return new ZstdBytesDecompressor();
      case LZ4_RAW:
        return new Lz4RawBytesDecompressor();
      case GZIP:
        return new GzipBytesDecompressor();
      case LZO:
        return new LzoBytesDecompressor();
      case BROTLI:
        if (Brotli4j.AVAILABLE) {
          return new BrotliBytesDecompressor();
        }
        // fall through to Hadoop codec path
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
   * Decompresses using Snappy's JNI API directly. The {@link ByteBuffer} overload uses
   * {@link Snappy#uncompress(ByteBuffer, ByteBuffer)} which, for direct buffers, passes
   * native memory addresses straight to the snappy library with no JNI array pinning or
   * intermediate copies.
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
      int origInputLimit = input.limit();
      input.limit(input.position() + compressedSize);
      int origOutputLimit = output.limit();
      output.limit(output.position() + decompressedSize);
      // Use slices so native API works on independent buffers; advance positions manually.
      Snappy.uncompress(input.slice(), output.slice());
      input.position(input.limit());
      input.limit(origInputLimit);
      output.position(output.limit());
      output.limit(origOutputLimit);
    }

    @Override
    public void release() {}
  }

  // ---- Optimized ZSTD compressor/decompressor using zstd-jni context API directly ----

  /**
   * Compresses using a reusable {@link ZstdCompressCtx}, bypassing the Hadoop codec
   * framework ({@code ZstandardCodec}, {@code CodecPool}, {@code CompressionOutputStream}
   * wrapper). The context is created once at construction and reused across calls,
   * avoiding per-call JNI context creation, internal buffer allocation, and Java stream
   * overhead. This is 1.5-3.4x faster than the streaming approach for typical Parquet
   * page sizes (64KB-1MB). Multi-threaded compression via {@code workers > 0} is
   * supported through {@link ZstdCompressCtx#setWorkers(int)}.
   */
  static class ZstdBytesCompressor extends BytesCompressor {
    private final ZstdCompressCtx context;
    private byte[] outputBuffer;

    ZstdBytesCompressor(int level, int workers) {
      this.context = new ZstdCompressCtx();
      this.context.setLevel(level);
      if (workers > 0) {
        this.context.setWorkers(workers);
      }
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      int maxLen = (int) Zstd.compressBound(input.length);
      if (outputBuffer == null || outputBuffer.length < maxLen) {
        outputBuffer = new byte[maxLen];
      }
      int compressed = context.compressByteArray(outputBuffer, 0, outputBuffer.length, input, 0, input.length);
      return BytesInput.from(outputBuffer, 0, compressed);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.ZSTD;
    }

    @Override
    public void release() {
      context.close();
      outputBuffer = null;
    }
  }

  /**
   * Decompresses using a reusable {@link ZstdDecompressCtx}, bypassing the Hadoop
   * codec framework. The context is created once at construction and reused across
   * calls, avoiding per-call JNI context creation, internal buffer allocation, and
   * Java stream overhead. The {@link ByteBuffer} overload uses
   * {@link Zstd#decompress(ByteBuffer, ByteBuffer)} to pass buffers directly to the
   * native library without intermediate copies.
   */
  static class ZstdBytesDecompressor extends BytesDecompressor {
    private final ZstdDecompressCtx context;

    ZstdBytesDecompressor() {
      this.context = new ZstdDecompressCtx();
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      byte[] input = bytes.toByteArray();
      byte[] output = new byte[decompressedSize];
      int decompressed = context.decompressByteArray(output, 0, decompressedSize, input, 0, input.length);
      if (decompressed != decompressedSize) {
        throw new IOException("Unexpected decompressed size: " + decompressed + " != " + decompressedSize);
      }
      return BytesInput.from(output);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      int origInputLimit = input.limit();
      input.limit(input.position() + compressedSize);
      int origOutputLimit = output.limit();
      output.limit(output.position() + decompressedSize);
      // Zstd.decompress uses (dst, src) parameter order, matching the native zstd convention.
      // Use slices so native API works on independent buffers; advance positions manually.
      Zstd.decompress(output.slice(), input.slice());
      input.position(input.limit());
      input.limit(origInputLimit);
      output.position(output.limit());
      output.limit(origOutputLimit);
    }

    @Override
    public void release() {
      context.close();
    }
  }

  // ---- Optimized LZ4_RAW compressor/decompressor using airlift LZ4 directly ----

  /**
   * Compresses using airlift's LZ4 compressor directly with heap ByteBuffers,
   * bypassing the Hadoop stream abstraction and NonBlockedCompressor's direct
   * buffer copies.
   */
  static class Lz4RawBytesCompressor extends BytesCompressor {
    private final Lz4Compressor compressor = new Lz4Compressor();
    private ByteBuffer directInputBuf;
    private ByteBuffer directOutputBuf;

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      int maxLen = compressor.maxCompressedLength(input.length);

      // Grow reusable direct input buffer if needed
      if (directInputBuf == null || directInputBuf.capacity() < input.length) {
        directInputBuf = ByteBuffer.allocateDirect(input.length);
      }
      directInputBuf.clear();
      directInputBuf.put(input);
      directInputBuf.flip();

      // Grow reusable direct output buffer if needed
      if (directOutputBuf == null || directOutputBuf.capacity() < maxLen) {
        directOutputBuf = ByteBuffer.allocateDirect(maxLen);
      }
      directOutputBuf.clear();

      compressor.compress(directInputBuf, directOutputBuf);
      int compressedSize = directOutputBuf.position();

      // Copy result to heap byte array
      directOutputBuf.flip();
      byte[] output = new byte[compressedSize];
      directOutputBuf.get(output);
      return BytesInput.from(output, 0, compressedSize);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.LZ4_RAW;
    }

    @Override
    public void release() {
      directInputBuf = null;
      directOutputBuf = null;
    }
  }

  /**
   * Decompresses using airlift's LZ4 decompressor with reusable direct ByteBuffers.
   * Aircompressor's LZ4 native implementation is significantly faster (~25%) on direct
   * (off-heap) buffers because it can use raw native pointers via Unsafe, avoiding the
   * overhead of JNI array pinning required for heap-backed buffers. The cost of copying
   * data to/from the reusable direct buffers is more than offset by the faster native
   * decompression, especially at typical Parquet page sizes (64KB-1MB).
   */
  static class Lz4RawBytesDecompressor extends BytesDecompressor {
    private final Lz4Decompressor decompressor = new Lz4Decompressor();
    private ByteBuffer directInputBuf;
    private ByteBuffer directOutputBuf;

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      int inputSize = Math.toIntExact(bytes.size());

      // Grow reusable direct input buffer if needed
      if (directInputBuf == null || directInputBuf.capacity() < inputSize) {
        directInputBuf = ByteBuffer.allocateDirect(inputSize);
      }
      directInputBuf.clear().limit(inputSize);
      // toByteArray() is zero-copy for ByteArrayBytesInput (returns backing array directly)
      directInputBuf.put(bytes.toByteArray(), 0, inputSize);
      directInputBuf.flip();

      // Grow reusable direct output buffer if needed
      if (directOutputBuf == null || directOutputBuf.capacity() < decompressedSize) {
        directOutputBuf = ByteBuffer.allocateDirect(decompressedSize);
      }
      directOutputBuf.clear().limit(decompressedSize);

      decompressor.decompress(directInputBuf.slice(), directOutputBuf.slice());

      // Copy result to heap — returning a ByteArrayBytesInput allows callers to
      // get the byte[] via toByteArray() without an additional copy.
      byte[] output = new byte[decompressedSize];
      directOutputBuf.position(0).limit(decompressedSize);
      directOutputBuf.get(output);
      return BytesInput.from(output);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      int origInputLimit = input.limit();
      input.limit(input.position() + compressedSize);
      int origOutputLimit = output.limit();
      output.limit(output.position() + decompressedSize);
      // Use slices so native API works on independent buffers; advance positions manually.
      decompressor.decompress(input.slice(), output.slice());
      input.position(input.limit());
      input.limit(origInputLimit);
      output.position(output.limit());
      output.limit(origOutputLimit);
    }

    @Override
    public void release() {
      directInputBuf = null;
      directOutputBuf = null;
    }
  }

  // ---- Optimized GZIP compressor/decompressor using JDK GZIPOutputStream/GZIPInputStream directly ----

  /**
   * Compresses using {@link GZIPOutputStream} directly, bypassing Hadoop's
   * GzipCodec and the associated codec pool / stream wrapper overhead.
   *
   * <p>Note: this implementation always uses Java's built-in zlib via
   * {@link GZIPOutputStream}. It does <em>not</em> use Hadoop native libraries,
   * so hardware-accelerated compression via Intel ISA-L will not be used even if
   * the native libraries are installed. The overhead reduction from bypassing the
   * Hadoop codec framework typically outweighs the ISA-L advantage for the page
   * sizes used by Parquet.
   */
  static class GzipBytesCompressor extends BytesCompressor {
    private final int level;
    private final ByteArrayOutputStream baos;

    GzipBytesCompressor(int level, int pageSize) {
      this.level = level;
      this.baos = new ByteArrayOutputStream(pageSize);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      baos.reset();
      try (GZIPOutputStream gos = new GZIPOutputStream(baos) {
        {
          def.setLevel(level);
        }
      }) {
        bytes.writeAllTo(gos);
      }
      return BytesInput.from(baos);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.GZIP;
    }

    @Override
    public void release() {}
  }

  /**
   * Decompresses using {@link GZIPInputStream} directly, bypassing Hadoop's
   * GzipCodec and the associated codec pool / stream wrapper overhead.
   * CRC32 and size verification is handled by the JDK implementation.
   *
   * <p>Note: this implementation always uses Java's built-in zlib via
   * {@link GZIPInputStream}. It does <em>not</em> use Hadoop native libraries,
   * so hardware-accelerated decompression via Intel ISA-L will not be used even if
   * the native libraries are installed.
   */
  static class GzipBytesDecompressor extends BytesDecompressor {
    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      try (GZIPInputStream gis = new GZIPInputStream(bytes.toInputStream())) {
        byte[] output = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = gis.read(output, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of GZIP stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        return BytesInput.from(output);
      }
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      // Wrap the input ByteBuffer slice in an InputStream to avoid allocating a temp byte array.
      // GZIPInputStream is stream-based so we still need a temp output array.
      ByteBuffer inputSlice = input.slice();
      inputSlice.limit(compressedSize);
      try (GZIPInputStream gis = new GZIPInputStream(ByteBufferInputStream.wrap(inputSlice))) {
        byte[] outputBytes = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = gis.read(outputBytes, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of GZIP stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        output.put(outputBytes);
      }
      input.position(input.position() + compressedSize);
    }

    @Override
    public void release() {}
  }

  // ---- Optimized LZO compressor/decompressor using aircompressor's Hadoop-framed LZO directly ----

  /**
   * Compresses using aircompressor's LZO Hadoop-framed streams directly,
   * bypassing the GPL-licensed {@code com.hadoop.compression.lzo.LzoCodec} and
   * the associated Hadoop codec pool / stream wrapper overhead. The framing
   * format (big-endian length-prefixed blocks) is wire-compatible with Hadoop's
   * LzoCodec, so files produced by this compressor are readable by any standard
   * Parquet reader.
   */
  static class LzoBytesCompressor extends BytesCompressor {
    private static final LzoHadoopStreams LZO_STREAMS = new LzoHadoopStreams();
    private final ByteArrayOutputStream baos;

    LzoBytesCompressor(int pageSize) {
      this.baos = new ByteArrayOutputStream(pageSize);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      baos.reset();
      try (OutputStream los = LZO_STREAMS.createOutputStream(baos)) {
        bytes.writeAllTo(los);
      }
      return BytesInput.from(baos);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.LZO;
    }

    @Override
    public void release() {}
  }

  /**
   * Decompresses using aircompressor's LZO Hadoop-framed streams directly,
   * bypassing the GPL-licensed Hadoop LzoCodec. Reads the same big-endian
   * length-prefixed block framing that Hadoop's LzoCodec produces.
   */
  static class LzoBytesDecompressor extends BytesDecompressor {
    private static final LzoHadoopStreams LZO_STREAMS = new LzoHadoopStreams();

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      try (InputStream lis = LZO_STREAMS.createInputStream(bytes.toInputStream())) {
        byte[] output = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = lis.read(output, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of LZO stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        return BytesInput.from(output);
      }
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      ByteBuffer inputSlice = input.slice();
      inputSlice.limit(compressedSize);
      try (InputStream lis = LZO_STREAMS.createInputStream(ByteBufferInputStream.wrap(inputSlice))) {
        byte[] outputBytes = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = lis.read(outputBytes, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of LZO stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        output.put(outputBytes);
      }
      input.position(input.position() + compressedSize);
    }

    @Override
    public void release() {}
  }

  /**
   * Brotli compressor using brotli4j ({@code com.aayushatharva.brotli4j}) via reflection.
   * Single-call byte-array API — no streaming overhead. Default quality=1
   * matches the old jbrotli default and gives a good speed/ratio trade-off.
   */
  static class BrotliBytesCompressor extends BytesCompressor {
    private final Object params;

    BrotliBytesCompressor(int quality) {
      this.params = Brotli4j.newParams(quality);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      byte[] input = bytes.toByteArray();
      byte[] compressed = Brotli4j.compress(input, params);
      return BytesInput.from(compressed);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.BROTLI;
    }

    @Override
    public void release() {}
  }

  /**
   * Brotli decompressor using brotli4j ({@code com.aayushatharva.brotli4j}) via reflection.
   * Single-call byte-array API. For the ByteBuffer overload the input slice
   * is copied to a heap array, decompressed, and the result put into the
   * output buffer — Brotli is slow enough that the copy overhead is negligible.
   */
  static class BrotliBytesDecompressor extends BytesDecompressor {

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      byte[] compressed = bytes.toByteArray();
      byte[] decompressed = Brotli4j.decompress(compressed);
      return BytesInput.from(decompressed);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      ByteBuffer inputSlice = input.slice();
      inputSlice.limit(compressedSize);
      byte[] compressedBytes = new byte[compressedSize];
      inputSlice.get(compressedBytes);

      byte[] decompressed = Brotli4j.decompress(compressedBytes);
      output.put(decompressed);
      input.position(input.position() + compressedSize);
    }

    @Override
    public void release() {}
  }
}
