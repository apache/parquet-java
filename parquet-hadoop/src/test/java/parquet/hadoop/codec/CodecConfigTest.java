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
package parquet.hadoop.codec;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.junit.Test;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import parquet.hadoop.util.ContextUtil;

public class CodecConfigTest {
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
    TaskAttemptContext task = ContextUtil.newTaskAttemptContext(conf, new TaskAttemptID(new TaskID(new JobID("test", 1), false, 1), 1));
    Assert.assertEquals(CodecConfig.from(task).getCodec(), expectedCodec);

    //Test mapred API
    JobConf jobConf = new JobConf();
    jobConf.set(ParquetOutputFormat.COMPRESSION, codecNameStr);
    Assert.assertEquals(CodecConfig.from(jobConf).getCodec(), expectedCodec);
  }

  public void shouldUseHadoopFlagToSetCodec(String codecClassStr, CompressionCodecName expectedCodec) throws IOException {
    //Test mapreduce API
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.codec", codecClassStr);
    TaskAttemptContext task = ContextUtil.newTaskAttemptContext(conf, new TaskAttemptID(new TaskID(new JobID("test", 1), false, 1), 1));
    Assert.assertEquals(expectedCodec, CodecConfig.from(task).getCodec());

    //Test mapred API
    JobConf jobConf = new JobConf();
    jobConf.setBoolean("mapred.output.compress", true);
    jobConf.set("mapred.output.compression.codec", codecClassStr);
    Assert.assertEquals(CodecConfig.from(jobConf).getCodec(), expectedCodec);
  }


}
