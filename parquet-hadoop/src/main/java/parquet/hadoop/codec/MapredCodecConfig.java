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
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * Access codec related configurations in mapred API
 */
public class MapredCodecConfig extends HadoopCodecConfig {
  private final JobConf conf;

  public MapredCodecConfig(JobConf conf) {
    this.conf = conf;
  }

  @Override
  public boolean isHadoopCompressionSet() {
    return FileOutputFormat.getCompressOutput(conf);
  }

  @Override
  public Class getHadoopOutputCompressorClass(Class defaultCodec) {
    return FileOutputFormat.getOutputCompressorClass(conf, defaultCodec);
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }
}
