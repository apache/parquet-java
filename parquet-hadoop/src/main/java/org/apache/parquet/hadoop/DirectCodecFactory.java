/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.ReusingByteBufferAllocator;
import org.apache.parquet.hadoop.codec.ZstandardCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Factory to produce compressors and decompressors that operate on java
 * direct memory, without requiring a copy into heap memory (where possible).
 */
class DirectCodecFactory extends CodecFactory implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DirectCodecFactory.class);

  private final ByteBufferAllocator allocator;

  // Any of these can be null depending on the version of hadoop on the classpath
  private static final Class<?> DIRECT_DECOMPRESSION_CODEC_CLASS;
  private static final Method DECOMPRESS_METHOD;
  private static final Method CREATE_DIRECT_DECOMPRESSOR_METHOD;

  static {
    Class<?> tempClass = null;
    Method tempCreateMethod = null;
    Method tempDecompressMethod = null;
    try {
      tempClass = Class.forName("org.apache.hadoop.io.compress.DirectDecompressionCodec");
      tempCreateMethod = tempClass.getMethod("createDirectDecompressor");
      Class<?> tempClass2 = Class.forName("org.apache.hadoop.io.compress.DirectDecompressor");
      tempDecompressMethod = tempClass2.getMethod("decompress", ByteBuffer.class, ByteBuffer.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      // do nothing, the class will just be assigned null
    }
    DIRECT_DECOMPRESSION_CODEC_CLASS = tempClass;
    CREATE_DIRECT_DECOMPRESSOR_METHOD = tempCreateMethod;
    DECOMPRESS_METHOD = tempDecompressMethod;
  }

  /**
   * See docs on CodecFactory#createDirectCodecFactory which is how this class is
   * exposed publicly and is just a pass-through factory method for this constructor
   * to hide the rest of this class from public access.
   *
   * @throws NullPointerException if allocator is {@code null}
   */
  DirectCodecFactory(Configuration config, ByteBufferAllocator allocator, int pageSize) {
    super(config, pageSize);

    this.allocator = Objects.requireNonNull(allocator, "allocator cannot be null");
    Preconditions.checkState(
        allocator.isDirect(),
        "A %s requires a direct buffer allocator be provided.",
        getClass().getSimpleName());
  }

  @Override
  protected BytesCompressor createCompressor(final CompressionCodecName codecName) {
    switch (codecName) {
      case SNAPPY:
        // avoid using the default Snappy codec since it allocates direct buffers at awkward spots.
        return new SnappyCompressor();
      case ZSTD:
        return new ZstdCompressor();
      // todo: create class similar to the SnappyCompressor for zlib and exclude it as
      // snappy is above since it also generates allocateDirect calls.
      default:
        return super.createCompressor(codecName);
    }
  }

  @Override
  protected BytesDecompressor createDecompressor(final CompressionCodecName codecName) {
    switch (codecName) {
      case SNAPPY:
        return new SnappyDecompressor();
      case ZSTD:
        return new ZstdDecompressor();
      default:
        CompressionCodec codec = getCodec(codecName);
        if (codec == null) {
          return NO_OP_DECOMPRESSOR;
        }
        DirectCodecPool.CodecPool pool = DirectCodecPool.INSTANCE.codec(codec);
        if (pool.supportsDirectDecompression()) {
          return new FullDirectDecompressor(pool.borrowDirectDecompressor());
        } else {
          return new IndirectDecompressor(pool.borrowDecompressor());
        }
    }
  }

  public void close() {
    release();
  }

  /**
   * Wrapper around legacy hadoop compressors that do not implement a direct memory
   * based version of the decompression algorithm.
   */
  public class IndirectDecompressor extends BytesDecompressor {
    private final Decompressor decompressor;

    public IndirectDecompressor(CompressionCodec codec) {
      this(DirectCodecPool.INSTANCE.codec(codec).borrowDecompressor());
    }

    private IndirectDecompressor(Decompressor decompressor) {
      this.decompressor = decompressor;
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      decompressor.reset();
      byte[] inputBytes = bytes.toByteArray();
      decompressor.setInput(inputBytes, 0, inputBytes.length);
      byte[] output = new byte[decompressedSize];
      decompressor.decompress(output, 0, decompressedSize);
      return BytesInput.from(output);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {

      decompressor.reset();
      byte[] inputBytes = new byte[compressedSize];
      input.get(inputBytes);
      decompressor.setInput(inputBytes, 0, inputBytes.length);
      byte[] outputBytes = new byte[decompressedSize];
      decompressor.decompress(outputBytes, 0, decompressedSize);
      output.put(outputBytes);
    }

    @Override
    public void release() {
      DirectCodecPool.INSTANCE.returnDecompressor(decompressor);
    }
  }

  private abstract class BaseDecompressor extends BytesDecompressor {
    private final ReusingByteBufferAllocator inputAllocator;
    private final ReusingByteBufferAllocator outputAllocator;

    BaseDecompressor() {
      inputAllocator = ReusingByteBufferAllocator.strict(allocator);
      // Using unsafe reusing allocator because we give out the output ByteBuffer wrapped in a BytesInput. But
      // that's what BytesInputs are for. It is expected to copy the data from the returned BytesInput before
      // using this decompressor again.
      outputAllocator = ReusingByteBufferAllocator.unsafe(allocator);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      try (ByteBufferReleaser releaser = inputAllocator.getReleaser()) {
        ByteBuffer input = bytes.toByteBuffer(releaser);
        ByteBuffer output = outputAllocator.allocate(decompressedSize);
        int size = decompress(input.slice(), output.slice());
        if (size != decompressedSize) {
          throw new DirectCodecPool.ParquetCompressionCodecException(
              "Unexpected decompressed size: " + size + " != " + decompressedSize);
        }
        output.limit(size);
        return BytesInput.from(output);
      }
    }

    abstract int decompress(ByteBuffer input, ByteBuffer output) throws IOException;

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      int origInputLimit = input.limit();
      input.limit(input.position() + compressedSize);
      int origOutputLimit = output.limit();
      output.limit(output.position() + decompressedSize);
      int size = decompress(input.slice(), output.slice());
      if (size != decompressedSize) {
        throw new DirectCodecPool.ParquetCompressionCodecException(
            "Unexpected decompressed size: " + size + " != " + decompressedSize);
      }
      input.position(input.limit());
      input.limit(origInputLimit);
      output.position(output.limit());
      output.limit(origOutputLimit);
    }

    @Override
    public void release() {
      AutoCloseables.uncheckedClose(outputAllocator, inputAllocator, this::closeDecompressor);
    }

    abstract void closeDecompressor();
  }

  private abstract class BaseCompressor extends BytesCompressor {
    private final ReusingByteBufferAllocator inputAllocator;
    private final ReusingByteBufferAllocator outputAllocator;

    BaseCompressor() {
      inputAllocator = ReusingByteBufferAllocator.strict(allocator);
      // Using unsafe reusing allocator because we give out the output ByteBuffer wrapped in a BytesInput. But
      // that's what BytesInputs are for. It is expected to copy the data from the returned BytesInput before
      // using this compressor again.
      outputAllocator = ReusingByteBufferAllocator.unsafe(allocator);
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      try (ByteBufferReleaser releaser = inputAllocator.getReleaser()) {
        ByteBuffer input = bytes.toByteBuffer(releaser);
        ByteBuffer output = outputAllocator.allocate(maxCompressedSize(Math.toIntExact(bytes.size())));
        int size = compress(input.slice(), output.slice());
        output.limit(size);
        return BytesInput.from(output);
      }
    }

    abstract int maxCompressedSize(int size);

    abstract int compress(ByteBuffer input, ByteBuffer output) throws IOException;

    @Override
    public void release() {
      AutoCloseables.uncheckedClose(outputAllocator, inputAllocator, this::closeCompressor);
    }

    abstract void closeCompressor();
  }

  /**
   * Wrapper around new Hadoop compressors that implement a direct memory
   * based version of a particular decompression algorithm. To maintain
   * compatibility with Hadoop 1.x these classes that implement
   * {@link org.apache.hadoop.io.compress.DirectDecompressionCodec}
   * are currently retrieved and have their decompression method invoked
   * with reflection.
   */
  public class FullDirectDecompressor extends BaseDecompressor {
    private final Object decompressor;

    public FullDirectDecompressor(CompressionCodecName codecName) {
      this(DirectCodecPool.INSTANCE
          .codec(Objects.requireNonNull(getCodec(codecName)))
          .borrowDirectDecompressor());
    }

    private FullDirectDecompressor(Object decompressor) {
      this.decompressor = decompressor;
    }

    @Override
    public BytesInput decompress(BytesInput compressedBytes, int decompressedSize) throws IOException {
      // Similarly to non-direct decompressors, we reset before use, if possible (see HeapBytesDecompressor)
      if (decompressor instanceof Decompressor) {
        ((Decompressor) decompressor).reset();
      }
      return super.decompress(compressedBytes, decompressedSize);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      // Similarly to non-direct decompressors, we reset before use, if possible (see HeapBytesDecompressor)
      if (decompressor instanceof Decompressor) {
        ((Decompressor) decompressor).reset();
      }
      super.decompress(input, compressedSize, output, decompressedSize);
    }

    @Override
    int decompress(ByteBuffer input, ByteBuffer output) {
      int startPos = output.position();
      try {
        DECOMPRESS_METHOD.invoke(decompressor, input, output);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new DirectCodecPool.ParquetCompressionCodecException(e);
      }
      int size = output.position() - startPos;
      // Some decompressors flip the output buffer, some don't:
      // Let's rely on the limit if the position did not change
      return size == 0 ? output.limit() : size;
    }

    @Override
    void closeDecompressor() {
      DirectCodecPool.INSTANCE.returnDirectDecompressor(decompressor);
    }
  }

  /**
   * @deprecated Use {@link CodecFactory#NO_OP_DECOMPRESSOR} instead
   */
  @Deprecated
  public class NoopDecompressor extends BytesDecompressor {

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      NO_OP_DECOMPRESSOR.decompress(input, compressedSize, output, decompressedSize);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      return NO_OP_DECOMPRESSOR.decompress(bytes, decompressedSize);
    }

    @Override
    public void release() {
      NO_OP_DECOMPRESSOR.release();
    }
  }

  public class SnappyDecompressor extends BaseDecompressor {
    @Override
    int decompress(ByteBuffer input, ByteBuffer output) throws IOException {
      return Snappy.uncompress(input, output);
    }

    @Override
    void closeDecompressor() {
      // no-op
    }
  }

  public class SnappyCompressor extends BaseCompressor {

    @Override
    int compress(ByteBuffer input, ByteBuffer output) throws IOException {
      return Snappy.compress(input, output);
    }

    @Override
    int maxCompressedSize(int size) {
      return Snappy.maxCompressedLength(size);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.SNAPPY;
    }

    @Override
    void closeCompressor() {
      // no-op
    }
  }

  private class ZstdDecompressor extends BaseDecompressor {
    private final ZstdDecompressCtx context;

    ZstdDecompressor() {
      context = new ZstdDecompressCtx();
    }

    @Override
    int decompress(ByteBuffer input, ByteBuffer output) {
      return context.decompress(output, input);
    }

    @Override
    void closeDecompressor() {
      context.close();
    }
  }

  private class ZstdCompressor extends BaseCompressor {
    private final ZstdCompressCtx context;

    ZstdCompressor() {
      context = new ZstdCompressCtx();
      context.setLevel(conf.getInt(
          ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, ZstandardCodec.DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL));
      context.setWorkers(conf.getInt(
          ZstandardCodec.PARQUET_COMPRESS_ZSTD_WORKERS, ZstandardCodec.DEFAULTPARQUET_COMPRESS_ZSTD_WORKERS));
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.ZSTD;
    }

    @Override
    int maxCompressedSize(int size) {
      return Math.toIntExact(Zstd.compressBound(size));
    }

    @Override
    int compress(ByteBuffer input, ByteBuffer output) {
      return context.compress(output, input);
    }

    @Override
    void closeCompressor() {
      context.close();
    }
  }

  /**
   * @deprecated Use {@link CodecFactory#NO_OP_COMPRESSOR} instead
   */
  @Deprecated
  public static class NoopCompressor extends BytesCompressor {

    public NoopCompressor() {}

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      return NO_OP_COMPRESSOR.compress(bytes);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return NO_OP_COMPRESSOR.getCodecName();
    }

    @Override
    public void release() {
      NO_OP_COMPRESSOR.release();
    }
  }

  static class DirectCodecPool {

    public static final DirectCodecPool INSTANCE = new DirectCodecPool();

    private final Map<CompressionCodec, CodecPool> codecs =
        Collections.synchronizedMap(new HashMap<CompressionCodec, CodecPool>());
    private final Map<Class<?>, GenericObjectPool> directDePools =
        Collections.synchronizedMap(new HashMap<Class<?>, GenericObjectPool>());
    private final Map<Class<?>, GenericObjectPool> dePools =
        Collections.synchronizedMap(new HashMap<Class<?>, GenericObjectPool>());
    private final Map<Class<?>, GenericObjectPool> cPools =
        Collections.synchronizedMap(new HashMap<Class<?>, GenericObjectPool>());

    private DirectCodecPool() {}

    public class CodecPool {
      private final GenericObjectPool compressorPool;
      private final GenericObjectPool decompressorPool;
      private final GenericObjectPool directDecompressorPool;
      private final boolean supportDirectDecompressor;
      private static final String BYTE_BUF_IMPL_NOT_FOUND_MSG =
          "Unable to find ByteBuffer based %s for codec %s, will use a byte array based implementation instead.";

      private CodecPool(final CompressionCodec codec) {
        try {
          boolean supportDirectDecompressor = DIRECT_DECOMPRESSION_CODEC_CLASS != null
              && DIRECT_DECOMPRESSION_CODEC_CLASS.isAssignableFrom(codec.getClass());
          compressorPool = new GenericObjectPool(
              new BasePoolableObjectFactory() {
                public Object makeObject() throws Exception {
                  return codec.createCompressor();
                }
              },
              Integer.MAX_VALUE);

          Object com = compressorPool.borrowObject();
          if (com != null) {
            cPools.put(com.getClass(), compressorPool);
            compressorPool.returnObject(com);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format(
                  BYTE_BUF_IMPL_NOT_FOUND_MSG,
                  "compressor",
                  codec.getClass().getName()));
            }
          }

          decompressorPool = new GenericObjectPool(
              new BasePoolableObjectFactory() {
                public Object makeObject() throws Exception {
                  return codec.createDecompressor();
                }
              },
              Integer.MAX_VALUE);

          Object decom = decompressorPool.borrowObject();
          if (decom != null) {
            dePools.put(decom.getClass(), decompressorPool);
            decompressorPool.returnObject(decom);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format(
                  BYTE_BUF_IMPL_NOT_FOUND_MSG,
                  "decompressor",
                  codec.getClass().getName()));
            }
          }

          if (supportDirectDecompressor) {
            directDecompressorPool = new GenericObjectPool(
                new BasePoolableObjectFactory() {
                  public Object makeObject() throws Exception {
                    return CREATE_DIRECT_DECOMPRESSOR_METHOD.invoke(codec);
                  }
                },
                Integer.MAX_VALUE);

            Object ddecom = directDecompressorPool.borrowObject();
            if (ddecom != null) {
              directDePools.put(ddecom.getClass(), directDecompressorPool);
              directDecompressorPool.returnObject(ddecom);
            } else {
              supportDirectDecompressor = false;
              if (LOG.isDebugEnabled()) {
                LOG.debug(String.format(
                    BYTE_BUF_IMPL_NOT_FOUND_MSG,
                    "compressor",
                    codec.getClass().getName()));
              }
            }
          } else {
            directDecompressorPool = null;
          }

          this.supportDirectDecompressor = supportDirectDecompressor;
        } catch (Exception e) {
          throw new ParquetCompressionCodecException("Error creating compression codec pool.", e);
        }
      }

      public Object borrowDirectDecompressor() {
        Preconditions.checkArgument(
            supportDirectDecompressor, "Tried to get a direct Decompressor from a non-direct codec.");
        try {
          return directDecompressorPool.borrowObject();
        } catch (Exception e) {
          throw new ParquetCompressionCodecException(e);
        }
      }

      public boolean supportsDirectDecompression() {
        return supportDirectDecompressor;
      }

      public Decompressor borrowDecompressor() {
        return borrow(decompressorPool);
      }

      public Compressor borrowCompressor() {
        return borrow(compressorPool);
      }
    }

    public CodecPool codec(CompressionCodec codec) {
      CodecPool pools = codecs.get(codec);
      if (pools == null) {
        synchronized (this) {
          pools = codecs.get(codec);
          if (pools == null) {
            pools = new CodecPool(codec);
            codecs.put(codec, pools);
          }
        }
      }
      return pools;
    }

    private void returnToPool(Object obj, Map<Class<?>, GenericObjectPool> pools) {
      try {
        GenericObjectPool pool = pools.get(obj.getClass());
        if (pool == null) {
          throw new IllegalStateException("Received unexpected compressor or decompressor, "
              + "cannot be returned to any available pool: "
              + obj.getClass().getSimpleName());
        }
        pool.returnObject(obj);
      } catch (Exception e) {
        throw new ParquetCompressionCodecException(e);
      }
    }

    /**
     * Borrow an object from a pool.
     *
     * @param pool - the pull to borrow from, must not be null
     * @return - an object from the pool
     */
    @SuppressWarnings("unchecked")
    public <T> T borrow(GenericObjectPool pool) {
      try {
        return (T) pool.borrowObject();
      } catch (Exception e) {
        throw new ParquetCompressionCodecException(e);
      }
    }

    public void returnCompressor(Compressor compressor) {
      returnToPool(compressor, cPools);
    }

    public void returnDecompressor(Decompressor decompressor) {
      returnToPool(decompressor, dePools);
    }

    public void returnDirectDecompressor(Object decompressor) {
      returnToPool(decompressor, directDePools);
    }

    public static class ParquetCompressionCodecException extends ParquetRuntimeException {

      public ParquetCompressionCodecException() {
        super();
      }

      public ParquetCompressionCodecException(String message, Throwable cause) {
        super(message, cause);
      }

      public ParquetCompressionCodecException(String message) {
        super(message);
      }

      public ParquetCompressionCodecException(Throwable cause) {
        super(cause);
      }
    }
  }
}
