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


import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.junit.Test;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class HadoopCodecConfigTest {
  @Test
  public void testReadingCodecs() throws IOException {
    shouldUseParquetFlagToSetCodec("gzip", CompressionCodecName.GZIP);
    shouldUseHadoopFlagToSetCodec(CompressionCodecName.GZIP.getHadoopCompressionCodecClassName(), CompressionCodecName.GZIP);
    shouldUseParquetFlagToSetCodec("snappy", CompressionCodecName.SNAPPY);
    shouldUseHadoopFlagToSetCodec(CompressionCodecName.SNAPPY.getHadoopCompressionCodecClassName(), CompressionCodecName.SNAPPY);
    //When codec is unrecognized, use uncompressed
    shouldUseHadoopFlagToSetCodec("unexistedCodec", CompressionCodecName.UNCOMPRESSED);
    //For unsupported codec, use uncompressed
    shouldUseHadoopFlagToSetCodec("org.apache.hadoop.io.compress.DefaultCodec", CompressionCodecName.UNCOMPRESSED);
  }

  public void shouldUseParquetFlagToSetCodec(String codecNameStr, CompressionCodecName expectedCodec) throws IOException {

    //Test mapreduce API
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    conf.set(ParquetOutputFormat.COMPRESSION, codecNameStr);
    TaskAttemptContext task = new TaskAttemptContext(conf, new TaskAttemptID(new TaskID(new JobID("test", 1), false, 1), 1));
    Assert.assertEquals(new MapReduceCodecConfig(task).getCodec(), expectedCodec);

    //Test mapred API
    JobConf jobConf = new JobConf();
    jobConf.set(ParquetOutputFormat.COMPRESSION, codecNameStr);
    Assert.assertEquals(new MapredCodecConfig(jobConf).getCodec(), expectedCodec);
  }

  public void shouldUseHadoopFlagToSetCodec(String codecClassStr, CompressionCodecName expectedCodec) throws IOException {
    //Test mapreduce API
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.codec", codecClassStr);
    TaskAttemptContext task = new TaskAttemptContext(conf, new TaskAttemptID(new TaskID(new JobID("test", 1), false, 1), 1));
    Assert.assertEquals(expectedCodec, new MapReduceCodecConfig(task).getCodec());

    //Test mapred API
    JobConf jobConf = new JobConf();
    jobConf.setBoolean("mapred.output.compress", true);
    jobConf.set("mapred.output.compression.codec", codecClassStr);
    Assert.assertEquals(new MapredCodecConfig(jobConf).getCodec(), expectedCodec);
  }


}
