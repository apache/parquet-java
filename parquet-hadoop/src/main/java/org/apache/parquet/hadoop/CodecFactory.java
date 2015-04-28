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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

class CodecFactory {

  public class BytesDecompressor {

    private final CompressionCodec codec;
    private final Decompressor decompressor;

    public BytesDecompressor(CompressionCodec codec) {
      this.codec = codec;
      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
      } else {
        decompressor = null;
      }
    }

    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      final BytesInput decompressed;
      if (codec != null) {
        decompressor.reset();
        InputStream is = codec.createInputStream(new ByteArrayInputStream(bytes.toByteArray()), decompressor);
        decompressed = BytesInput.from(is, uncompressedSize);
      } else {
        decompressed = bytes;
      }
      return decompressed;
    }

    private void release() {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }

  /**
   * Encapsulates the logic around hadoop compression
   *
   * @author Julien Le Dem
   *
   */
  public static class BytesCompressor {

    private final CompressionCodec codec;
    private final Compressor compressor;
    private final ByteArrayOutputStream compressedOutBuffer;
    private final CompressionCodecName codecName;

    public BytesCompressor(CompressionCodecName codecName, CompressionCodec codec, int pageSize) {
      this.codecName = codecName;
      this.codec = codec;
      if (codec != null) {
        this.compressor = CodecPool.getCompressor(codec);
        this.compressedOutBuffer = new ByteArrayOutputStream(pageSize);
      } else {
        this.compressor = null;
        this.compressedOutBuffer = null;
      }
    }

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

    private void release() {
      if (compressor != null) {
        CodecPool.returnCompressor(compressor);
      }
    }

    public CompressionCodecName getCodecName() {
      return codecName;
    }

  }

  private final Map<CompressionCodecName, BytesCompressor> compressors = new HashMap<CompressionCodecName, BytesCompressor>();
  private final Map<CompressionCodecName, BytesDecompressor> decompressors = new HashMap<CompressionCodecName, BytesDecompressor>();
  private final Map<String, CompressionCodec> codecByName = new HashMap<String, CompressionCodec>();
  private final Configuration configuration;

  public CodecFactory(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   *
   * @param codecName the requested codec
   * @return the corresponding hadoop codec. null if UNCOMPRESSED
   */
  private CompressionCodec getCodec(CompressionCodecName codecName) {
    String codecClassName = codecName.getHadoopCompressionCodecClassName();
    if (codecClassName == null) {
      return null;
    }
    CompressionCodec codec = codecByName.get(codecClassName);
    if (codec != null) {
      return codec;
    }

    try {
      Class<?> codecClass = Class.forName(codecClassName);
      codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, configuration);
      codecByName.put(codecClassName, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
    }
  }

  public BytesCompressor getCompressor(CompressionCodecName codecName, int pageSize) {
    BytesCompressor comp = compressors.get(codecName);
    if (comp == null) {
      CompressionCodec codec = getCodec(codecName);
      comp = new BytesCompressor(codecName, codec, pageSize);
      compressors.put(codecName, comp);
    }
    return comp;
  }

  public BytesDecompressor getDecompressor(CompressionCodecName codecName) {
    BytesDecompressor decomp = decompressors.get(codecName);
    if (decomp == null) {
      CompressionCodec codec = getCodec(codecName);
      decomp = new BytesDecompressor(codec);
      decompressors.put(codecName, decomp);
    }
    return decomp;
  }

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
}
