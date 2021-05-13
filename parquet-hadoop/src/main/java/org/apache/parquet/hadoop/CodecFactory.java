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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class CodecFactory implements CompressionCodecFactory {

  protected static final Map<String, CompressionCodec> CODEC_BY_NAME = Collections
      .synchronizedMap(new HashMap<String, CompressionCodec>());

  private final Map<CompressionCodecName, BytesCompressor> compressors = new HashMap<>();
  private final Map<CompressionCodecName, BytesDecompressor> decompressors = new HashMap<>();

  protected final Configuration configuration;
  protected final int pageSize;

  /**
   * Create a new codec factory.
   *
   * @param configuration used to pass compression codec configuration information
   * @param pageSize the expected page size, does not set a hard limit, currently just
   *                 used to set the initial size of the output stream used when
   *                 compressing a buffer. If this factory is only used to construct
   *                 decompressors this parameter has no impact on the function of the factory
   */
  public CodecFactory(Configuration configuration, int pageSize) {
    this.configuration = configuration;
    this.pageSize = pageSize;
  }

  /**
   * Create a codec factory that will provide compressors and decompressors
   * that will work natively with ByteBuffers backed by direct memory.
   *
   * @param config configuration options for different compression codecs
   * @param allocator an allocator for creating result buffers during compression
   *                  and decompression, must provide buffers backed by Direct
   *                  memory and return true for the isDirect() method
   *                  on the ByteBufferAllocator interface
   * @param pageSize the default page size. This does not set a hard limit on the
   *                 size of buffers that can be compressed, but performance may
   *                 be improved by setting it close to the expected size of buffers
   *                 (in the case of parquet, pages) that will be compressed. This
   *                 setting is unused in the case of decompressing data, as parquet
   *                 always records the uncompressed size of a buffer. If this
   *                 CodecFactory is only going to be used for decompressors, this
   *                 parameter will not impact the function of the factory.
   * @return a configured direct codec factory
   */
  public static CodecFactory createDirectCodecFactory(Configuration config, ByteBufferAllocator allocator, int pageSize) {
    return new DirectCodecFactory(config, allocator, pageSize);
  }

  class HeapBytesDecompressor extends BytesDecompressor {

    private final CompressionCodec codec;
    private final Decompressor decompressor;

    HeapBytesDecompressor(CompressionCodecName codecName) {
      this.codec = getCodec(codecName);
      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
      } else {
        decompressor = null;
      }
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      final BytesInput decompressed;
      if (codec != null) {
        if (decompressor != null) {
          decompressor.reset();
        }
        InputStream is = codec.createInputStream(bytes.toInputStream(), decompressor);
        decompressed = BytesInput.from(is, uncompressedSize);
      } else {
        decompressed = bytes;
      }
      return decompressed;
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
      ByteBuffer decompressed = decompress(BytesInput.from(input), uncompressedSize).toByteBuffer();
      output.put(decompressed);
    }

    @Override
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

    HeapBytesCompressor(CompressionCodecName codecName) {
      this.codecName = codecName;
      this.codec = getCodec(codecName);
      if (codec != null) {
        this.compressor = CodecPool.getCompressor(codec);
        this.compressedOutBuffer = new ByteArrayOutputStream(pageSize);
      } else {
        this.compressor = null;
        this.compressedOutBuffer = null;
      }
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      final BytesInput compressedBytes;
      if (codec == null) {
        compressedBytes = bytes;
      } else {
        compressedOutBuffer.reset();
        if (compressor != null) {
          // null compressor for non-native gzip
          compressor.reset();
        }
        CompressionOutputStream cos = codec.createOutputStream(compressedOutBuffer, compressor);
        bytes.writeAllTo(cos);
        cos.finish();
        cos.close();
        compressedBytes = BytesInput.from(compressedOutBuffer);
      }
      return compressedBytes;
    }

    @Override
    public void release() {
      if (compressor != null) {
        CodecPool.returnCompressor(compressor);
      }
    }

    @Override
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
    return new HeapBytesCompressor(codecName);
  }

  protected BytesDecompressor createDecompressor(CompressionCodecName codecName) {
    return new HeapBytesDecompressor(codecName);
  }

  /**
   *
   * @param codecName
   *          the requested codec
   * @return the corresponding hadoop codec. null if UNCOMPRESSED
   */
  protected CompressionCodec getCodec(CompressionCodecName codecName) {
    String codecClassName = codecName.getHadoopCompressionCodecClassName();
    if (codecClassName == null) {
      return null;
    }
    CompressionCodec codec = CODEC_BY_NAME.get(codecClassName);
    if (codec != null) {
      return codec;
    }

    try {
      Class<?> codecClass;
      try {
        codecClass = Class.forName(codecClassName);
      } catch (ClassNotFoundException e) {
        // Try to load the class using the job classloader
        codecClass = configuration.getClassLoader().loadClass(codecClassName);
      }
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
      CODEC_BY_NAME.put(codecClassName, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
    }
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
  public static abstract class BytesCompressor implements CompressionCodecFactory.BytesInputCompressor {
    @Override
    public abstract BytesInput compress(BytesInput bytes) throws IOException;
    @Override
    public abstract CompressionCodecName getCodecName();
    @Override
    public abstract void release();
  }

  /**
   * @deprecated will be removed in 2.0.0; use CompressionCodecFactory.BytesInputDecompressor instead.
   */
  @Deprecated
  public static abstract class BytesDecompressor implements CompressionCodecFactory.BytesInputDecompressor {
    @Override
    public abstract BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException;
    @Override
    public abstract void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException;
    @Override
    public abstract void release();
  }
}
