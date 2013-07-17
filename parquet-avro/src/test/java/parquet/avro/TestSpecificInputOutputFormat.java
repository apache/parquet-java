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
package parquet.avro;

import com.google.common.collect.Lists;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestSpecificInputOutputFormat {
  private static final Log LOG = Log.getLog(TestSpecificInputOutputFormat.class);

  public static Car nextRecord(int i) {
      Car.Builder carBuilder = Car.newBuilder()
              .setDoors(2)
              .setEngine(Engine.newBuilder()
                      .setCapacity(85.0f)
                      .setHasTurboCharger(false)
                      .setType(EngineType.ELECTRIC).build())
              .setMake("Tesla")
              .setModel("Model X")
              .setYear(2014)
              .setRegistration("Calfornia");
      if (i % 4 == 0) {
          List<Service> serviceList = Lists.newArrayList();
              serviceList.add(Service.newBuilder()
                      .setDate(1374084640)
                      .setMechanic("Elon Musk").build());
          carBuilder.setServiceHistory(serviceList);
      }
      return carBuilder.build();
  }

  public static class MyMapper extends Mapper<LongWritable, Text, Void, Car> {

    public void run(Context context) throws IOException ,InterruptedException {
      for (int i = 0; i < 10; i++) {
        context.write(null, nextRecord(i));
      }
    }
  }

  public static class MyMapper2 extends Mapper<Void, Car, LongWritable, Text> {
    @Override
    protected void map(Void key, Car value, Context context) throws IOException ,InterruptedException {
      context.write(null, new Text(value.toString()));
    }

  }

  @Test
  public void testReadWrite() throws Exception {

    final Configuration conf = new Configuration();
    final Path inputPath = new Path("src/test/java/parquet/avro/TestSpecificInputOutputFormat.java");
    final Path parquetPath = new Path("target/test/hadoop/TestSpecificInputOutputFormat/parquet");
    final Path outputPath = new Path("target/test/hadoop/TestSpecificInputOutputFormat/out");
    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      final Job job = new Job(conf, "write");

      // input not really used
      TextInputFormat.addInputPath(job, inputPath);
      job.setInputFormatClass(TextInputFormat.class);

      job.setMapperClass(TestSpecificInputOutputFormat.MyMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(AvroParquetOutputFormat.class);
      AvroParquetOutputFormat.setOutputPath(job, parquetPath);
      AvroParquetOutputFormat.setSchema(job, Car.SCHEMA$);

      waitForJob(job);
    }
    {
      final Job job = new Job(conf, "read");
      job.setInputFormatClass(AvroParquetInputFormat.class);
      AvroParquetInputFormat.setInputPaths(job, parquetPath);

      job.setMapperClass(TestSpecificInputOutputFormat.MyMapper2.class);
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
      Car a = nextRecord(lineNumber);
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
