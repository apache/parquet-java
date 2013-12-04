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
package parquet.hadoop.codec;

import org.apache.hadoop.conf.Configuration;
import parquet.Log;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import static parquet.Log.INFO;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

/**
 * Template class for accessing codec related configurations in different APIs(mapreduce or mapred)
 *
 */
public abstract class HadoopCodecConfig {
  private static final Log LOG = Log.getLog(HadoopCodecConfig.class);

  /**
   *
   * @return if a compress flag is set from hadoop
   */
  public abstract boolean isHadoopCompressionSet();

  /**
   *
   * @param defaultCodec the codec to use when codec is not set in conf
   * @return codec specified in hadoop config
   */
  public abstract Class getHadoopOutputCompressorClass(Class defaultCodec);

  /**
   *
   * @return configuration of the job
   */
  public abstract Configuration getConfiguration();

  public boolean isParquetCompressionSet() {
    return getConfiguration().get(ParquetOutputFormat.COMPRESSION) != null;
  }

  public static CompressionCodecName getParquetCompressionCodec(Configuration configuration) {
    return CompressionCodecName.fromConf(configuration.get(ParquetOutputFormat.COMPRESSION, UNCOMPRESSED.name()));
  }

  public CompressionCodecName getCodec(){
    CompressionCodecName codec;

    if (isParquetCompressionSet()) { // explicit parquet config
      codec = getParquetCompressionCodec(getConfiguration());
    } else if (isHadoopCompressionSet()) { // from hadoop config
      codec = getHadoopCompressionCodec();
    } else {
      if (INFO)LOG.info("Compression set to false");
      codec = CompressionCodecName.UNCOMPRESSED;
    }

    if (INFO)LOG.info("Compression: " + codec.name());
    return codec;
  }

  private CompressionCodecName getHadoopCompressionCodec() {
    CompressionCodecName codec;
    try {
     // find the right codec
     Class<?> codecClass = getHadoopOutputCompressorClass(CompressionCodecName.UNCOMPRESSED.getHadoopCompressionCodecClass());
     if (INFO) LOG.info("Compression set through hadoop codec: " + codecClass.getName());
     codec = CompressionCodecName.fromCompressionCodec(codecClass);
    } catch (CompressionCodecNotSupportedException e){
      if (INFO) LOG.info("codec defined in hadoop config is not supported by parquet [" + e.getCodecClass().getName()+"] and will use UNCOMPRESSED");
      codec = CompressionCodecName.UNCOMPRESSED;
    } catch (IllegalArgumentException e){
      if (INFO) LOG.info("codec class not found: " + e.getMessage());
      codec = CompressionCodecName.UNCOMPRESSED;
    }
    return codec;
  }
}
