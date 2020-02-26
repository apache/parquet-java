package org.apache.parquet.hadoop;

import org.apache.hadoop.fs.CommonConfigurationKeys;

public class CodecConfigurationKeys extends CommonConfigurationKeys{
  /** Customized Compression Codec name */
  public static final String CUSTOMIZED_COMPRESSION_CODEC_NAME =
    "io.compression.codec.name";

  /** Customized Compression Codec class name */
  public static final String CUSTOMIZED_COMPRESSION_CODEC_CLASS_NAME =
    "io.compression.codec.name";

}
