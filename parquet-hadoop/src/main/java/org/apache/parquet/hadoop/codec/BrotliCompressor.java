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

package org.apache.parquet.hadoop.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.meteogroup.jbrotli.Brotli;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import static org.apache.parquet.schema.OriginalType.JSON;
import static org.apache.parquet.schema.OriginalType.UTF8;

public class BrotliCompressor extends NonBlockingCompressor {
  private static final Set<OriginalType> TEXT_ANNOTATIONS = new HashSet<OriginalType>();
  static {
    TEXT_ANNOTATIONS.add(UTF8);
    TEXT_ANNOTATIONS.add(JSON);
  }

  private final org.meteogroup.jbrotli.BrotliCompressor compressor =
      new org.meteogroup.jbrotli.BrotliCompressor();
  private Brotli.Parameter parameter;

  private static boolean isText(PrimitiveType primitiveType) {
    return (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) &&
        TEXT_ANNOTATIONS.contains(primitiveType.getOriginalType());
  }

  public BrotliCompressor(Configuration conf) {
    this.parameter = makeParameter(conf);
  }

  public BrotliCompressor(PrimitiveType primitive, int quality) {
    Preconditions.checkArgument(quality >= 1 && quality <= 11,
        "Invalid Brotli quality: %s", quality);
    this.parameter = new Brotli.Parameter();
    parameter.setMode(isText(primitive) ? Brotli.Mode.TEXT : Brotli.Mode.GENERIC);
    parameter.setQuality(quality);
  }

  @Override
  protected int getMaxCompressedLength(int numInputBytes) {
    // this is based on https://github.com/google/brotli/issues/274
    // that page is not very clear, so this is much more conservative
    return numInputBytes * 2;
  }

  @Override
  public int compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer) throws IOException {
    return compressor.compress(parameter, inputBuffer, outputBuffer);
  }

  @Override
  public void reinit(Configuration conf) {
    super.reinit(conf);
    this.parameter = makeParameter(conf);
  }

  private static Brotli.Parameter makeParameter(Configuration conf) {
    if (conf != null) {
      int quality = conf.getInt(BrotliNonBlockingCodec.QUALITY_LEVEL_PROP, 1);
      Preconditions.checkArgument(quality >= 1 && quality <= 11,
          "Invalid Brotli quality: %s", quality);

      boolean isText = conf.getBoolean(BrotliNonBlockingCodec.IS_TEXT_PROP, false);

      Brotli.Parameter parameter = new Brotli.Parameter();
      parameter.setMode(isText ? Brotli.Mode.TEXT : Brotli.Mode.GENERIC);
      parameter.setQuality(quality);

      return parameter;
    }

    return new Brotli.Parameter();
  }
}
