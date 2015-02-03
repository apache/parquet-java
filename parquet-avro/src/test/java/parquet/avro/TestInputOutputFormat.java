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
package parquet.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestInputOutputFormat {
  private static final Log LOG = Log.getLog(TestInputOutputFormat.class);

  private static Schema avroSchema;
  static {
    avroSchema = Schema.createRecord("record1", null, null, false);
    avroSchema.setFields(
        Arrays.asList(new Schema.Field("a",
            Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))),
            null, null)));
  }

  public static GenericRecord nextRecord(Integer i) {
    return new GenericRecordBuilder(avroSchema).set("a", i).build();
  };

  public static class MyMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {

    public void run(Context context) throws IOException ,InterruptedException {
      for (int i = 0; i < 10; i++) {
        GenericRecord a;
        a = TestInputOutputFormat.nextRecord(i == 4 ? null : i);
        context.write(null, a);
      }
    }
  }

  public static class MyMapper2 extends Mapper<Void, GenericRecord, LongWritable, Text> {
    protected void map(Void key, GenericRecord value, Context context) throws IOException ,InterruptedException {
      context.write(null, new Text(value.toString()));
    }

  }

  @Test
  public void testReadWrite() throws Exception {

    final Configuration conf = new Configuration();
    final Path inputPath = new Path("src/test/java/parquet/avro/TestInputOutputFormat.java");
    final Path parquetPath = new Path("target/test/hadoop/TestInputOutputFormat/parquet");
    final Path outputPath = new Path("target/test/hadoop/TestInputOutputFormat/out");
    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      final Job job = new Job(conf, "write");

      // input not really used
      TextInputFormat.addInputPath(job, inputPath);
      job.setInputFormatClass(TextInputFormat.class);

      job.setMapperClass(TestInputOutputFormat.MyMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(AvroParquetOutputFormat.class);
      AvroParquetOutputFormat.setOutputPath(job, parquetPath);
      AvroParquetOutputFormat.setSchema(job, avroSchema);

      waitForJob(job);
    }
    {
      final Job job = new Job(conf, "read");
      job.setInputFormatClass(AvroParquetInputFormat.class);
      AvroParquetInputFormat.setInputPaths(job, parquetPath);

      job.setMapperClass(TestInputOutputFormat.MyMapper2.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(job, outputPath);

      waitForJob(job);
    }

    final BufferedReader out = new BufferedReader(new FileReader(new File(outputPath.toString(), "part-m-00000")));
    String lineOut = null;
    int lineNumber = 0;
    while ((lineOut = out.readLine()) != null) {
      lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
      GenericRecord a = nextRecord(lineNumber == 4 ? null : lineNumber);
      assertEquals("line " + lineNumber, a.toString(), lineOut);
      ++ lineNumber;
    }
    assertNull("line " + lineNumber, out.readLine());
    out.close();
  }

  private void waitForJob(Job job) throws Exception {
    job.submit();
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
