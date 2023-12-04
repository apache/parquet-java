/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.compression;

import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * {@link CompressionCodecFactory} implementation that delegates to the {@link CompressionCodecFactory} implementations it was constructed with.
 */
public class FallbackCodecFactory implements CompressionCodecFactory {

  private final CompressionCodecFactory[] codecFactories;

  /**
   * Constructs a FallbackCodecFactory using other {@link CompressionCodecFactory} implementations.
   * The FallbackCodecFactory recognizes that a codec could not be provided by the {@link CompressionCodecFactory} implementations throwing a {@link CodecNotImplementedException}.
   *
   * @param codecFactories the {@link CompressionCodecFactory} implementations to delegate to, in order.
   */
  public FallbackCodecFactory(CompressionCodecFactory... codecFactories) {
    Preconditions.checkArgument(
        codecFactories != null && codecFactories.length > 0,
        "FallbackCodecFactory needs to be constructed using at least one CompressionCodecFactory to fall back on.");
    this.codecFactories = codecFactories;
  }

  @Override
  public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
    for (CompressionCodecFactory codecFactory : codecFactories) {
      try {
        return codecFactory.getCompressor(codecName);
      } catch (CodecNotImplementedException ignored) {
      }
    }
    throw new CodecNotImplementedException("FallbackCodecFactory could not get the " + codecName
        + " compressor from the supplied CompressionCodecFactory implementations.");
  }

  @Override
  public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
    for (CompressionCodecFactory codecFactory : codecFactories) {
      try {
        return codecFactory.getDecompressor(codecName);
      } catch (CodecNotImplementedException ignored) {
      }
    }
    throw new CodecNotImplementedException("FallbackCodecFactory could not get the " + codecName
        + " decompressor from the supplied CompressionCodecFactory implementations.");
  }

  @Override
  public void release() {
    for (CompressionCodecFactory codecFactory : codecFactories) {
      codecFactory.release();
    }
  }
}
