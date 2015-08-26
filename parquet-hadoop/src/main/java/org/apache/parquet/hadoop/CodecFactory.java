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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public abstract class CodecFactory<C extends CodecFactory.BytesCompressor, D extends CodecFactory.BytesDecompressor> {

  protected static final Map<String, CompressionCodec> CODEC_BY_NAME = Collections
      .synchronizedMap(new HashMap<String, CompressionCodec>());

  private final Map<CompressionCodecName, C> compressors = new HashMap<CompressionCodecName, C>();
  private final Map<CompressionCodecName, D> decompressors = new HashMap<CompressionCodecName, D>();

  protected final Configuration configuration;

  protected CodecFactory(Configuration configuration) {
    this.configuration = configuration;
  }

  public C getCompressor(CompressionCodecName codecName, int pageSize) {
    C comp = compressors.get(codecName);
    if (comp == null) {

      CompressionCodec codec = getCodec(codecName);
      comp = createCompressor(codecName, codec, pageSize);
      compressors.put(codecName, comp);
    }
    return comp;
  }

  public D getDecompressor(CompressionCodecName codecName) {
    D decomp = decompressors.get(codecName);
    if (decomp == null) {
      CompressionCodec codec = getCodec(codecName);
      decomp = createDecompressor(codec);
      decompressors.put(codecName, decomp);
    }
    return decomp;
  }

  protected abstract C createCompressor(CompressionCodecName codecName, CompressionCodec codec, int pageSize);

  protected abstract D createDecompressor(CompressionCodec codec);

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
      Class<?> codecClass = Class.forName(codecClassName);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
      CODEC_BY_NAME.put(codecClassName, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
    }
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

  public static abstract class BytesCompressor {
    public abstract BytesInput compress(BytesInput bytes) throws IOException;

    public abstract CompressionCodecName getCodecName();

    protected abstract void release();
  }

  public static abstract class BytesDecompressor {
    public abstract BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException;

    protected abstract void release();
  }
}
