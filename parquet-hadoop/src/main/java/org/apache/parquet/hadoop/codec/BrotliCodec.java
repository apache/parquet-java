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

import com.aayushatharva.brotli4j.Brotli4jLoader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.hadoop.BadConfigurationException;

/**
 * Hadoop codec for Brotli, backed by the maintained brotli4j native bindings.
 *
 * <p>The previous implementation depended on the abandoned jbrotli-based brotli-codec artifact,
 * which bundled an old Brotli decoder. This codec is loaded reflectively by {@link
 * org.apache.parquet.hadoop.CodecFactory} and only requires brotli4j when Brotli is actually used.
 */
public class BrotliCodec implements CompressionCodec, Configurable {

  public static final String BROTLI_QUALITY = "compression.brotli.quality";
  public static final int DEFAULT_QUALITY = 1;
  private static final int DEFAULT_MAX_OUTPUT_SIZE = Integer.MAX_VALUE - 8;

  private Configuration conf;
  private int quality = DEFAULT_QUALITY;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (conf != null) {
      quality = conf.getInt(BROTLI_QUALITY, DEFAULT_QUALITY);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    if (quality < 0 || quality > 11) {
      throw new BadConfigurationException("Invalid Brotli compression quality: " + quality + ". Expected 0..11.");
    }
    ensureBrotliAvailable();
    try {
      return new BrotliCompressionOutputStream(out, quality);
    } catch (LinkageError e) {
      throw missingBrotli(e);
    }
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
    return createOutputStream(out);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return null;
  }

  @Override
  public Compressor createCompressor() {
    return null;
  }

  /**
   * Decodes a complete Brotli stream into memory. Callers that handle untrusted data should use
   * {@link #createInputStream(InputStream, int)} so the decoded output is bounded by the size they
   * expect.
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return createInputStream(in, DEFAULT_MAX_OUTPUT_SIZE);
  }

  /**
   * Creates a decoder with an upper bound on the decompressed size. Parquet passes the size from
   * the page header, so a crafted page cannot expand without limit during decoding.
   *
   * @throws IOException if the bound is negative or the decoded output exceeds it
   */
  public CompressionInputStream createInputStream(InputStream in, int maxOutputSize) throws IOException {
    ensureBrotliAvailable();
    try {
      return new BrotliCompressionInputStream(in, maxOutputSize);
    } catch (LinkageError e) {
      throw missingBrotli(e);
    }
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
    return createInputStream(in);
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return null;
  }

  @Override
  public Decompressor createDecompressor() {
    return null;
  }

  @Override
  public String getDefaultExtension() {
    return ".br";
  }

  private static void ensureBrotliAvailable() throws IOException {
    try {
      Brotli4jLoader.ensureAvailability();
    } catch (LinkageError e) {
      throw missingBrotli(e);
    }
  }

  private static IOException missingBrotli(LinkageError cause) {
    return new IOException(
        "Brotli codec requires the optional brotli4j dependency and its native library "
            + "for this platform on the classpath",
        cause);
  }
}
