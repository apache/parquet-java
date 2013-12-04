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
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import parquet.hadoop.util.ContextUtil;

/**
 * Access codec related configurations in mapreduce API
 */
public class MapReduceCodecConfig extends HadoopCodecConfig {
  private final TaskAttemptContext context;

  public MapReduceCodecConfig(TaskAttemptContext context) {
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
    Configuration configuration = ContextUtil.getConfiguration(context);
    return configuration;
  }
}
