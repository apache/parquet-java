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
import org.apache.hadoop.io.compress.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Lz4 raw compression codec for Parquet. This codec type has been introduced
 * into the parquet format since version 2.9.0.
 */
public class Lz4RawCodec implements Configurable, CompressionCodec {

  private Configuration conf;
  // Hadoop config for how big to make intermediate buffers.
  private final String BUFFER_SIZE_CONFIG = "io.file.buffer.size";

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
    return new Lz4RawCompressor();
  }

  @Override
  public Decompressor createDecompressor() {
    return new Lz4RawDecompressor();
  }

  @Override
  public CompressionInputStream createInputStream(InputStream stream)
    throws IOException {
    return createInputStream(stream, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream stream,
                                                  Decompressor decompressor) throws IOException {
    return new NonBlockedDecompressorStream(stream, decompressor,
      conf.getInt(BUFFER_SIZE_CONFIG, 4*1024));
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream stream)
    throws IOException {
    return createOutputStream(stream, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream stream,
                                                    Compressor compressor) throws IOException {
    return new NonBlockedCompressorStream(stream, compressor,
      conf.getInt(BUFFER_SIZE_CONFIG, 4*1024));
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return Lz4RawCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return Lz4RawDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".lz4";
  }
}
