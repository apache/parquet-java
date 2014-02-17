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
package parquet.hadoop.data;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static parquet.hadoop.metadata.CompressionCodecName.GZIP;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import parquet.Log;
import parquet.data.Group;
import parquet.data.GroupBuilder;
import parquet.data.materializer.GroupBuilderImpl;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.util.ContextUtil;
import parquet.io.api.Binary;
import parquet.schema.MessageTypeParser;

public class TestInputOutputFormat {
  private static final Log LOG = Log.getLog(TestInputOutputFormat.class);
  private final Path parquetPath = new Path("target/test/parquet/hadoop/data/TestInputOutputFormat/parquet");
  private final Path inputPath = new Path("src/test/java/parquet/hadoop/data/TestInputOutputFormat.java");
  private final Path outputPath = new Path("target/test/parquet/hadoop/data/TestInputOutputFormat/out");
  private Job writeJob;
  private Job readJob;
  private final String writeSchema = "message example {\n" +
      "required int32 line;\n" +
      "required binary content;\n" +
      "}";
  private final String readSchema = "message example {\n" +
      "required int32 line;\n" +
      "required binary content;\n" +
      "}";
  private final String partialSchema = "message example {\n" +
      "required int32 line;\n" +
      "}";

  public static class ReadMapper extends Mapper<LongWritable, Text, Void, Group> {
    private GroupBuilder builder;

    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Void, Group>.Context context) throws java.io.IOException, InterruptedException {
      builder = GroupBuilderImpl.newGroupBuilderImpl(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context)));
    }

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Void, Group>.Context context) throws java.io.IOException, InterruptedException {
      Group group = builder.startMessage()
            .addIntValue("line", (int)key.get())
            .addBinaryValue("content", Binary.fromByteArray(value.getBytes(), 0, value.getLength()))
            .endMessage();
//      System.out.println("line: " + key.get());
//      System.out.println("content: " + value.toString());
//      System.out.println(group);
      context.write(null, group);
    }
  }

  public static class WriteMapper extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, Group value, Mapper<Void, Group, LongWritable, Text>.Context context) throws IOException, InterruptedException {
      context.write(new LongWritable(value.getInt("line")), new Text(value.getBinary("content").getBytes()));
    }
  }
  public static class PartialWriteMapper extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, Group value, Mapper<Void, Group, LongWritable, Text>.Context context) throws IOException, InterruptedException {
      context.write(new LongWritable(value.getInt("line")), new Text("dummy"));
    }
  }

  private void runMapReduceJob(CompressionCodecName codec, Class<? extends Mapper> writeMapperClass, String readSchema) throws IOException, ClassNotFoundException, InterruptedException {
    runMapReduceJob(codec, new Configuration(), writeMapperClass, readSchema);
  }

  private void runMapReduceJob(CompressionCodecName codec, Configuration conf, Class<? extends Mapper> writeMapperClass, String readSchema) throws IOException, ClassNotFoundException, InterruptedException {

    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      writeJob = new Job(conf, "write");
      TextInputFormat.addInputPath(writeJob, inputPath);
      writeJob.setInputFormatClass(TextInputFormat.class);
      writeJob.setNumReduceTasks(0);
      GroupOutputFormat.setCompression(writeJob, codec);
      GroupOutputFormat.setOutputPath(writeJob, parquetPath);
      writeJob.setOutputFormatClass(GroupOutputFormat.class);
      writeJob.setMapperClass(ReadMapper.class);

      GroupOutputFormat.setSchema(
              writeJob,
              MessageTypeParser.parseMessageType(
                      writeSchema));
      writeJob.submit();
      waitForJob(writeJob);
    }
    {

      conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
      readJob = new Job(conf, "read");

      readJob.setInputFormatClass(GroupInputFormat.class);

      GroupInputFormat.setInputPaths(readJob, parquetPath);
      readJob.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(readJob, outputPath);
      readJob.setMapperClass(writeMapperClass);
      readJob.setNumReduceTasks(0);
      readJob.submit();
      waitForJob(readJob);
    }
  }

  private void testReadWrite(CompressionCodecName codec) throws IOException, ClassNotFoundException, InterruptedException {
    runMapReduceJob(codec, WriteMapper.class, readSchema);
    final BufferedReader in = new BufferedReader(new FileReader(new File(inputPath.toString())));
    final BufferedReader out = new BufferedReader(new FileReader(new File(outputPath.toString(), "part-m-00000")));
    String lineIn;
    String lineOut = null;
    int lineNumber = 0;
    while ((lineIn = in.readLine()) != null && (lineOut = out.readLine()) != null) {
      ++lineNumber;
      lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
      assertEquals("line " + lineNumber, lineIn, lineOut);
    }
    assertNull("line " + lineNumber, lineIn);
    assertNull("line " + lineNumber, out.readLine());
    in.close();
    out.close();
  }

  @Test
  public void testReadWrite() throws IOException, ClassNotFoundException, InterruptedException {
    testReadWrite(CompressionCodecName.UNCOMPRESSED);
  }

  @Test
  public void testProjection() throws Exception{
    runMapReduceJob(CompressionCodecName.GZIP, PartialWriteMapper.class, partialSchema);
  }

  @Test
  public void testReadWriteWithCounter() throws Exception {
    runMapReduceJob(CompressionCodecName.GZIP, WriteMapper.class, readSchema);
    assertTrue(readJob.getCounters().getGroup("parquet").findCounter("bytesread").getValue() > 0L);
    assertTrue(readJob.getCounters().getGroup("parquet").findCounter("bytestotal").getValue() > 0L);
    assertTrue(readJob.getCounters().getGroup("parquet").findCounter("bytesread").getValue()
            == readJob.getCounters().getGroup("parquet").findCounter("bytestotal").getValue());
    //not testing the time read counter since it could be zero due to the size of data is too small
  }

  @Test
  public void testReadWriteWithoutCounter() throws Exception {
    Configuration conf = new Configuration();
    conf.set("parquet.benchmark.time.read", "false");
    conf.set("parquet.benchmark.bytes.total", "false");
    conf.set("parquet.benchmark.bytes.read", "false");
    runMapReduceJob(GZIP, conf, WriteMapper.class, readSchema);
    assertTrue(readJob.getCounters().getGroup("parquet").findCounter("bytesread").getValue() == 0L);
    assertTrue(readJob.getCounters().getGroup("parquet").findCounter("bytestotal").getValue() == 0L);
    assertTrue(readJob.getCounters().getGroup("parquet").findCounter("timeread").getValue() == 0L);
  }

  private void waitForJob(Job job) throws InterruptedException, IOException {
    while (!job.isComplete()) {
      LOG.debug("waiting for job " + job.getJobName());
      sleep(100);
    }
    LOG.info("status for job " + job.getJobName() + ": " + (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }
}
