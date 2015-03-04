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
package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Before;
import org.junit.Test;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.mapred.Container;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageTypeParser;

import java.io.IOException;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * DeprecatedParquetInputFormat is used by cascading. It initializes the recordReader using an initialize method with
 * different parameters than ParquetInputFormat
 * @author Tianshuo Deng
 */
public class DeprecatedInputFormatTest {
  final Path parquetPath = new Path("target/test/example/TestInputOutputFormat/parquet");
  final Path inputPath = new Path("src/test/java/parquet/hadoop/example/TestInputOutputFormat.java");
  final Path outputPath = new Path("target/test/example/TestInputOutputFormat/out");
  Job writeJob;
  JobConf jobConf;
  RunningJob mapRedJob;
  private String writeSchema;
  private String readSchema;
  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    jobConf = new JobConf();
    writeSchema = "message example {\n" +
            "required int32 line;\n" +
            "required binary content;\n" +
            "}";

    readSchema = "message example {\n" +
            "required int32 line;\n" +
            "required binary content;\n" +
            "}";
  }

  private void runMapReduceJob(CompressionCodecName codec) throws IOException, ClassNotFoundException, InterruptedException {

    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      writeJob = new Job(conf, "write");
      TextInputFormat.addInputPath(writeJob, inputPath);
      writeJob.setInputFormatClass(TextInputFormat.class);
      writeJob.setNumReduceTasks(0);
      ExampleOutputFormat.setCompression(writeJob, codec);
      ExampleOutputFormat.setOutputPath(writeJob, parquetPath);
      writeJob.setOutputFormatClass(ExampleOutputFormat.class);
      writeJob.setMapperClass(ReadMapper.class);
      ExampleOutputFormat.setSchema(
              writeJob,
              MessageTypeParser.parseMessageType(
                      writeSchema));
      writeJob.submit();
      waitForJob(writeJob);
    }
    {
      jobConf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
      jobConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, GroupReadSupport.class.getCanonicalName());
      jobConf.setInputFormat(MyDeprecatedInputFormat.class);
      MyDeprecatedInputFormat.setInputPaths(jobConf, parquetPath);
      jobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
      org.apache.hadoop.mapred.TextOutputFormat.setOutputPath(jobConf, outputPath);
      jobConf.setMapperClass(DeprecatedWriteMapper.class);
      jobConf.setNumReduceTasks(0);
      mapRedJob = JobClient.runJob(jobConf);
    }
  }

  @Test
  public void testReadWriteWithCountDeprecated() throws Exception {
    runMapReduceJob(CompressionCodecName.GZIP);
    assertTrue(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytesread").getValue() > 0L);
    assertTrue(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytestotal").getValue() > 0L);
    assertTrue(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytesread").getValue()
            == mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytestotal").getValue());
    //not testing the time read counter since it could be zero due to the size of data is too small
  }

  @Test
  public void testReadWriteWithoutCounter() throws Exception {
    jobConf.set("parquet.benchmark.time.read", "false");
    jobConf.set("parquet.benchmark.bytes.total", "false");
    jobConf.set("parquet.benchmark.bytes.read", "false");
    runMapReduceJob(CompressionCodecName.GZIP);
    assertEquals(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytesread").getValue(), 0L);
    assertEquals(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytestotal").getValue(), 0L);
    assertEquals(mapRedJob.getCounters().getGroup("parquet").getCounterForName("timeread").getValue(), 0L);
  }

  private void waitForJob(Job job) throws InterruptedException, IOException {
    while (!job.isComplete()) {
      System.out.println("waiting for job " + job.getJobName());
      sleep(100);
    }
    System.out.println("status for job " + job.getJobName() + ": " + (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }

  public static class ReadMapper extends Mapper<LongWritable, Text, Void, Group> {
    private SimpleGroupFactory factory;

    protected void setup(Context context) throws IOException, InterruptedException {
      factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context)));
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Group group = factory.newGroup()
              .append("line", (int) key.get())
              .append("content", value.toString());
      context.write(null, group);
    }
  }

  public static class DeprecatedWriteMapper implements org.apache.hadoop.mapred.Mapper<Void, Container<Group>, LongWritable, Text> {

    @Override
    public void map(Void aVoid, Container<Group> valueContainer, OutputCollector<LongWritable, Text> longWritableTextOutputCollector, Reporter reporter) throws IOException {
      Group value = valueContainer.get();
      longWritableTextOutputCollector.collect(new LongWritable(value.getInteger("line", 0)), new Text(value.getString("content", 0)));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf entries) {
    }
  }

  static class MyDeprecatedInputFormat extends DeprecatedParquetInputFormat<Group> {

  }
}
