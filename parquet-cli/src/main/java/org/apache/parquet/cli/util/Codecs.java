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

package org.apache.parquet.cli.util;

import org.apache.avro.file.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Locale;

public class Codecs {
  public static CompressionCodecName parquetCodec(String codec) {
    try {
      return CompressionCodecName.valueOf(codec.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown compression codec: " + codec);
    }
  }

  public static CodecFactory avroCodec(String codec) {
    CompressionCodecName parquetCodec = parquetCodec(codec);
    switch (parquetCodec) {
      case UNCOMPRESSED:
        return CodecFactory.nullCodec();
      case SNAPPY:
        return CodecFactory.snappyCodec();
      case GZIP:
        return CodecFactory.deflateCodec(9);
      default:
        throw new IllegalArgumentException(
            "Codec incompatible with Avro: " + codec);
    }
  }
}
