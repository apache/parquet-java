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
package parquet.hadoop.thrift;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
import parquet.example.data.Group;
import parquet.hadoop.metadata.CompressionCodecName;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;

public class TestInputOutputFormat {
  private static final Log LOG = Log.getLog(TestInputOutputFormat.class);

  public static AddressBook nextAddressbook(int i) {
    final ArrayList<Person> persons = new ArrayList<Person>();
    for (int j = 0; j < i % 3; j++) {
      final ArrayList<PhoneNumber> phones = new ArrayList<PhoneNumber>();
      for (int k = 0; k < i%4; k++) {
        phones.add(new PhoneNumber("12345"+i));
      }
      persons.add(new Person(new Name("John"+i, "Roberts"), i, "John@example.com" + i, phones));
    }
    AddressBook a = new AddressBook(persons);
    return a;
  };

  public static class MyMapper extends Mapper<LongWritable, Text, Void, AddressBook> {

    public void run(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Void,AddressBook>.Context context) throws IOException ,InterruptedException {
      for (int i = 0; i < 10; i++) {
        AddressBook a = TestInputOutputFormat.nextAddressbook(i);
        context.write(null, a);
      }
    }
  }

  public static class MyMapper2 extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, AddressBook value, Mapper<Void,Group,LongWritable,Text>.Context context) throws IOException ,InterruptedException {
      context.write(null, new Text(value.toString()));
    }

  }

  @Test
  public void testReadWrite() throws Exception {
    final Configuration conf = new Configuration();
    final Path inputPath = new Path("src/test/java/parquet/hadoop/thrift/TestInputOutputFormat.java");
    final Path parquetPath = new Path("target/test/thrift/TestInputOutputFormat/parquet");
    final Path outputPath = new Path("target/test/thrift/TestInputOutputFormat/out");
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

      job.setOutputFormatClass(ParquetThriftOutputFormat.class);
      ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.GZIP);
      ParquetThriftOutputFormat.setOutputPath(job, parquetPath);
      ParquetThriftOutputFormat.setThriftClass(job, AddressBook.class);

      waitForJob(job);
    }
    {
      final Job job = new Job(conf, "read");
      job.setInputFormatClass(ParquetThriftInputFormat.class);
      ParquetThriftInputFormat.setInputPaths(job, parquetPath);

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
      AddressBook a = nextAddressbook(lineNumber);
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
