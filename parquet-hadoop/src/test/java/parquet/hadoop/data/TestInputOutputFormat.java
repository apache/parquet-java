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
import static parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static parquet.hadoop.data.GroupOutputFormat.setSchema;
import static parquet.io.api.Binary.fromString;
import static parquet.schema.MessageTypeParser.parseMessageType;

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
import parquet.hadoop.util.ContextUtil;

public class TestInputOutputFormat {
  private static final Log LOG = Log.getLog(TestInputOutputFormat.class);
  private final Path inputPath = new Path("src/test/java/parquet/hadoop/data/TestInputOutputFormat.java");
  private final String outputBase = "target/test/parquet/hadoop/data/TestInputOutputFormat/";
  private final Path parquetPath = new Path(outputBase + "parquet");
  private final Path outputPath = new Path(outputBase + "out");
  private final String schema = "message example {\n" +
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
            .addBinaryValue("content", fromString(value.toString()))
            .endMessage();
//      System.out.println("line: " + key.get());
//      System.out.println("content: " + value.toString());
//      System.out.println(group);
//      System.out.println(group.getBinary("content").toStringUsingUTF8());
      context.write(null, group);
    }
  }

  public static class WriteMapper extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, Group value, Mapper<Void, Group, LongWritable, Text>.Context context) throws IOException, InterruptedException {
//      System.out.println(value.getBinary("content").toStringUsingUTF8());
      context.write(new LongWritable(value.getInt("line")), new Text(value.getBinary("content").toStringUsingUTF8()));
    }
  }
  public static class PartialWriteMapper extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, Group value, Mapper<Void, Group, LongWritable, Text>.Context context) throws IOException, InterruptedException {
      context.write(new LongWritable(value.getInt("line")), new Text("dummy"));
    }
  }

  private void runMapReduceJob(Class<? extends Mapper<Void, Group, LongWritable, Text>> writeMapperClass, String readSchema) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      Job writeJob = new Job(conf, "write");
      TextInputFormat.addInputPath(writeJob, inputPath);
      writeJob.setInputFormatClass(TextInputFormat.class);
      writeJob.setNumReduceTasks(0);
      GroupOutputFormat.setOutputPath(writeJob, parquetPath);
      writeJob.setOutputFormatClass(GroupOutputFormat.class);
      writeJob.setMapperClass(ReadMapper.class);
      setSchema(writeJob, parseMessageType(schema));
      writeJob.submit();
      waitForJob(writeJob);
    }
    {
      conf.set(PARQUET_READ_SCHEMA, readSchema);
      Job readJob = new Job(conf, "read");
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

  @Test
  public void testReadWrite() throws IOException, ClassNotFoundException, InterruptedException {
    runMapReduceJob(WriteMapper.class, schema);
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
  public void testProjection() throws Exception{
    runMapReduceJob(PartialWriteMapper.class, partialSchema);
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
