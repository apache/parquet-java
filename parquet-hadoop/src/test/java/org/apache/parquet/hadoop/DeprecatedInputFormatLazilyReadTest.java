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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.mapred.Container;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Before;
import org.junit.Test;

import static java.lang.Thread.sleep;

import static org.junit.Assert.assertEquals;

/**
 * @author Rajat Ahuja
 */
public class DeprecatedInputFormatLazilyReadTest {
  final Path parquetPath = new Path("src/test/resources/parquetinputfiles/");
  final Path inputPath = new Path("src/test/resources/inputfiles/part-00000");
  final Path outputPath = new Path("src/test/resources/outputfiles");
  Job writeJob;
  JobConf jobConf;
  RunningJob mapRedJob;
  private static String writeSchema;
  private static String readSchema;
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
      jobConf.setOutputFormat(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(jobConf, outputPath);
      jobConf.setMapperClass(DeprecatedWriteMapper.class);
      jobConf.setNumReduceTasks(0);
      mapRedJob = JobClient.runJob(jobConf);
    }
  }

  /*
  This test case will make sure that parquet record-reader reads the correct value
  from the file even after updating parquet record-reader with the dummy first value
   */
  @Test
  public void testLazilyReadValues() throws Exception {
    jobConf.set("parquet.benchmark.time.read", "false");
    jobConf.set("parquet.benchmark.bytes.total", "false");
    jobConf.set("parquet.benchmark.bytes.read", "false");
    runMapReduceJob(CompressionCodecName.GZIP);
    assertEquals(FileUtils.readLines(new File(inputPath.toString())), FileUtils.readLines(new File(outputPath.toString()+"/"+"part-00000")));
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

  public static class DeprecatedWriteMapper implements org.apache.hadoop.mapred.Mapper<Void, Container<Group>, Void, Text> {

    @Override
    public void map(Void aVoid, Container<Group> valueContainer, OutputCollector< Void, Text> longWritableTextOutputCollector, Reporter reporter) throws IOException {
      Group value = valueContainer.get();
      longWritableTextOutputCollector.collect(aVoid, new Text(value.getString("content", 0)));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf entries) {
    }
  }

  /*
  Updating the first record of parquet record-reader with the dummy value.
  Even after doing that parquet record-reader reads the correct value from the file
  since we do not pre-fetch the first value from the record-reader
  so this update will not affect as per the latest changes
   */
  static class MyDeprecatedInputFormat extends DeprecatedParquetInputFormat<Group> {

    @Override
    public RecordReader<Void, Container<Group>> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
      RecordReader<Void, Container<Group>> recordReader = super.getRecordReader(split, job, reporter);
      Group dummyFirstValue = new SimpleGroup(MessageTypeParser
        .parseMessageType(writeSchema))
        .append("line",0 )
        .append("content", "dummylastvalue");
      recordReader.createValue().set(dummyFirstValue);
      return recordReader;
    }
  }
}
