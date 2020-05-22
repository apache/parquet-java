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
package org.apache.parquet.hadoop.codec;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * ZSTD compression codec for Parquet.  We do not use the default hadoop
 * one because it requires 1) to set up hadoop on local development machine;
 * 2) to upgrade hadoop to the newer version to have ZSTD support which is
 * more cumbersome than upgrading parquet version.
 *
 * This implementation relies on ZSTD JNI(https://github.com/luben/zstd-jni)
 * which is already a dependency for Parquet. ZSTD JNI ZstdOutputStream and
 * ZstdInputStream use Zstd internally. So no need to create compressor and
 * decompressor in ZstdCodec.
 */
public class ZstdCodec implements Configurable, CompressionCodec {

  public final static String PARQUET_COMPRESS_ZSTD_LEVEL = "parquet.compression.codec.zstd.level";
  public final static int DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL = 3;
  public final static String PARQUET_COMPRESS_ZSTD_WORKERS = "parquet.compression.codec.zstd.workers";
  public final static int DEFAULTPARQUET_COMPRESS_ZSTD_WORKERS = 0;

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Compressor createCompressor() {
    // ZstdOutputStream calls static Zstd compressor, so no compressor is created
    return null;
  }

  @Override
  public Decompressor createDecompressor() {
    // ZstdInputStream calls static Zstd decompressor, so no decompressor is created
    return null;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream stream, Decompressor decompressor) throws IOException {
    // Ignore decompressor because ZstdInputStream calls static Zstd decompressor
    return createInputStream(stream);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream stream) throws IOException {
    return new ZstdDecompressorStream(stream);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream stream, Compressor compressor) throws IOException {
    // Ignore compressor because ZstdOutputStream calls static Zstd compressor
    return createOutputStream(stream);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream stream) throws IOException {
    return new ZstdCompressorStream(stream, conf.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL),
      conf.getInt(PARQUET_COMPRESS_ZSTD_WORKERS, DEFAULTPARQUET_COMPRESS_ZSTD_WORKERS));
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return null;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return null;
  }

  @Override
  public String getDefaultExtension() {
    return ".zstd";
  }  
}
