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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

/**
 * Template class and factory for accessing codec related configurations in
 * different APIs(mapreduce or mapred), use
 * {@link #from(org.apache.hadoop.mapred.JobConf)} for mapred API, use
 * {@link #from(org.apache.hadoop.mapreduce.TaskAttemptContext)} for mapreduce
 * API
 */
public abstract class CodecConfig {
  private static final Logger LOG = LoggerFactory.getLogger(CodecConfig.class);

  /**
   * @return if a compress flag is set from hadoop
   */
  public abstract boolean isHadoopCompressionSet();

  /**
   * @param defaultCodec the codec to use when codec is not set in conf
   * @return codec specified in hadoop config
   */
  public abstract Class getHadoopOutputCompressorClass(Class defaultCodec);

  /**
   * @return configuration of the job
   */
  public abstract Configuration getConfiguration();

  /**
   * use mapred api to read codec config
   * 
   * @param jobConf a mapred job configuration
   * @return MapredCodecConfig
   */
  public static CodecConfig from(JobConf jobConf) {
    return new MapredCodecConfig(jobConf);
  }

  /**
   * use mapreduce api to read codec config
   * 
   * @param context a mapreduce task attempt context
   * @return MapreduceCodecConfig
   */
  public static CodecConfig from(TaskAttemptContext context) {
    return new MapreduceCodecConfig(context);
  }

  public static boolean isParquetCompressionSet(Configuration conf) {
    return conf.get(ParquetOutputFormat.COMPRESSION) != null;
  }

  public static CompressionCodecName getParquetCompressionCodec(Configuration configuration) {
    return CompressionCodecName.fromConf(configuration.get(ParquetOutputFormat.COMPRESSION, UNCOMPRESSED.name()));
  }

  public CompressionCodecName getCodec() {
    CompressionCodecName codec;
    Configuration configuration = getConfiguration();
    if (isParquetCompressionSet(configuration)) { // explicit parquet config
      codec = getParquetCompressionCodec(configuration);
    } else if (isHadoopCompressionSet()) { // from hadoop config
      codec = getHadoopCompressionCodec();
    } else {
      LOG.info("Compression set to false");
      codec = CompressionCodecName.UNCOMPRESSED;
    }

    LOG.info("Compression: {}", codec.name());
    return codec;
  }

  private CompressionCodecName getHadoopCompressionCodec() {
    CompressionCodecName codec;
    try {
      // find the right codec
      Class<?> codecClass = getHadoopOutputCompressorClass(
          CompressionCodecName.UNCOMPRESSED.getHadoopCompressionCodecClass());
      LOG.info("Compression set through hadoop codec: {}", codecClass.getName());
      codec = CompressionCodecName.fromCompressionCodec(codecClass);
    } catch (CompressionCodecNotSupportedException e) {
      LOG.warn("codec defined in hadoop config is not supported by parquet [{}] and will use UNCOMPRESSED",
          e.getCodecClass().getName(), e);
      codec = CompressionCodecName.UNCOMPRESSED;
    } catch (IllegalArgumentException e) {
      LOG.warn("codec class not found: {}", e.getMessage(), e);
      codec = CompressionCodecName.UNCOMPRESSED;
    }
    return codec;
  }

  /**
   * Access codec related configurations in mapreduce API
   */
  private static class MapreduceCodecConfig extends CodecConfig {
    private final TaskAttemptContext context;

    public MapreduceCodecConfig(TaskAttemptContext context) {
      this.context = context;
    }

    @Override
    public boolean isHadoopCompressionSet() {
      return FileOutputFormat.getCompressOutput(context);
    }

    @Override
    public Class getHadoopOutputCompressorClass(Class defaultCodec) {
      return FileOutputFormat.getOutputCompressorClass(context, defaultCodec);
    }

    @Override
    public Configuration getConfiguration() {
      return ContextUtil.getConfiguration(context);
    }
  }

  /**
   * Access codec related configurations in mapred API
   */
  private static class MapredCodecConfig extends CodecConfig {
    private final JobConf conf;

    public MapredCodecConfig(JobConf conf) {
      this.conf = conf;
    }

    @Override
    public boolean isHadoopCompressionSet() {
      return org.apache.hadoop.mapred.FileOutputFormat.getCompressOutput(conf);
    }

    @Override
    public Class getHadoopOutputCompressorClass(Class defaultCodec) {
      return org.apache.hadoop.mapred.FileOutputFormat.getOutputCompressorClass(conf, defaultCodec);
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }
}
