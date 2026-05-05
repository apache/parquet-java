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
package org.apache.parquet.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Before;
import org.junit.Test;

/**
 * DeprecatedParquetInputFormat is used by cascading. It initializes the recordReader using an initialize method with
 * different parameters than ParquetInputFormat
 */
public class DeprecatedOutputFormatTest {
  final Path parquetPath = new Path("target/test/example/TestInputOutputFormat/parquet");
  final Path inputPath = new Path("src/test/java/org/apache/parquet/hadoop/example/TestInputOutputFormat.java");
  final Path outputPath = new Path("target/test/example/TestInputOutputFormat/out");
  JobConf jobConf;
  RunningJob mapRedJob;
  private String writeSchema;
  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    jobConf = new JobConf();
    writeSchema = "message example {\n" + "required int32 line;\n" + "required binary content;\n" + "}";
  }

  private void runMapReduceJob(CompressionCodecName codec)
      throws IOException, ClassNotFoundException, InterruptedException {

    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      jobConf.setInputFormat(TextInputFormat.class);
      TextInputFormat.addInputPath(jobConf, inputPath);
      jobConf.setNumReduceTasks(0);

      jobConf.setOutputFormat(DeprecatedParquetOutputFormat.class);
      DeprecatedParquetOutputFormat.setCompression(jobConf, codec);
      DeprecatedParquetOutputFormat.setOutputPath(jobConf, parquetPath);
      DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, GroupWriteSupport.class);
      GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), jobConf);

      jobConf.setMapperClass(DeprecatedMapper.class);
      mapRedJob = JobClient.runJob(jobConf);
    }
  }

  @Test
  public void testReadWrite() throws Exception {
    runMapReduceJob(CompressionCodecName.GZIP);
    assert (mapRedJob.isSuccessful());
  }

  public static class DeprecatedMapper implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Void, Group> {
    private SimpleGroupFactory factory;

    public void configure(JobConf job) {
      factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(job));
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Void, Group> outputCollector, Reporter reporter)
        throws IOException {
      Group group = factory.newGroup().append("line", (int) key.get()).append("content", value.toString());
      outputCollector.collect(null, group);
    }

    @Override
    public void close() {}
  }
}
