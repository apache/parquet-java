/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop.metadata;

import parquet.format.CompressionCodec;

public enum CompressionCodecName {
  UNCOMPRESSED(null, CompressionCodec.UNCOMPRESSED, ""),
  SNAPPY("parquet.hadoop.codec.SnappyCodec", CompressionCodec.SNAPPY, ".snappy"),
  GZIP("org.apache.hadoop.io.compress.GzipCodec", CompressionCodec.GZIP, ".gz"),
  LZO("com.hadoop.compression.lzo.LzoCodec", CompressionCodec.LZO, ".lzo");

  public static CompressionCodecName fromConf(String name) {
     if (name == null) {
       return UNCOMPRESSED;
     }
     return valueOf(name.toUpperCase());
  }

  public static CompressionCodecName fromCompressionCodec(Class<?> clazz) {
    if (clazz == null) {
      return UNCOMPRESSED;
    }
    String name = clazz.getName();
    for (CompressionCodecName codec : CompressionCodecName.values()) {
      if (name.equals(codec.getHadoopCompressionCodecClass())) {
        return codec;
      }
    }
    throw new IllegalArgumentException("Unknown compression codec " + clazz);
  }

  public static CompressionCodecName fromParquet(CompressionCodec codec) {
    for (CompressionCodecName codecName : CompressionCodecName.values()) {
      if (codec.equals(codecName.parquetCompressionCodec)) {
        return codecName;
      }
    }
    throw new IllegalArgumentException("Unknown compression codec " + codec);
  }

  private final String hadoopCompressionCodecClass;
  private final CompressionCodec parquetCompressionCodec;
  private final String extension;

  private CompressionCodecName(String hadoopCompressionCodecClass, CompressionCodec parquetCompressionCodec, String extension) {
    this.hadoopCompressionCodecClass = hadoopCompressionCodecClass;
    this.parquetCompressionCodec = parquetCompressionCodec;
    this.extension = extension;
  }

  public String getHadoopCompressionCodecClass() {
    return hadoopCompressionCodecClass;
  }

  public CompressionCodec getParquetCompressionCodec() {
    return parquetCompressionCodec;
  }

  public String getExtension() {
    return extension;
  }

}
