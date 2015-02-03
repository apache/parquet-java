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
package parquet.hadoop.thrift;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.thrift.TBase;
import org.junit.Test;

import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.thrift.test.compat.StructV1;
import parquet.thrift.test.compat.StructV2;
import parquet.thrift.test.compat.StructV3;

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

    public void run(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Void,AddressBook>.Context context) throws IOException, InterruptedException {
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

  public static class SchemaEvolutionMapper1 extends Mapper<LongWritable, Text, Void, StructV1> {
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Void,StructV1>.Context context) throws IOException ,InterruptedException {
      context.write(null, new StructV1(value.toString() + 1));
    };
  }

  public static class SchemaEvolutionMapper2 extends Mapper<LongWritable, Text, Void, StructV2> {
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Void,StructV2>.Context context) throws IOException ,InterruptedException {
      final StructV2 s = new StructV2(value.toString() + 2);
      s.setAge("undetermined");
      context.write(null, s);
    };
  }

  public static class SchemaEvolutionMapper3 extends Mapper<LongWritable, Text, Void, StructV3> {
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Void,StructV3>.Context context) throws IOException ,InterruptedException {
      final StructV3 s = new StructV3(value.toString() + 3);
      s.setAge("average");
      s.setGender("unavailable");
      context.write(null, s);
    };
  }

  public static class SchemaEvolutionReadMapper extends Mapper<LongWritable, Text, Void, StructV3> {
    protected void map(LongWritable key, StructV3 value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Void,Text>.Context context) throws IOException ,InterruptedException {
      context.write(null, new Text(value.toString()));
    };
  }

  @Test
  public void testSchemaEvolution() throws Exception {
    final Configuration conf = new Configuration();
    final Path inputPath = new Path("target/test/thrift/schema_evolution/in");
    final Path parquetPath = new Path("target/test/thrift/schema_evolution/parquet");
    final Path outputPath = new Path("target/test/thrift/schema_evolution/out");
    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(inputPath, true);
    final FSDataOutputStream in = fileSystem.create(inputPath);
    in.writeUTF("Alice\nBob\nCharles\n");
    in.close();
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      write(conf, inputPath, new Path(parquetPath, "V1"), TestInputOutputFormat.SchemaEvolutionMapper1.class, StructV1.class);
      write(conf, inputPath, new Path(parquetPath, "V2"), TestInputOutputFormat.SchemaEvolutionMapper2.class, StructV2.class);
      write(conf, inputPath, new Path(parquetPath, "V3"), TestInputOutputFormat.SchemaEvolutionMapper3.class, StructV3.class);
    }
    {
      final Job job = new Job(conf, "read");
      job.setInputFormatClass(ParquetThriftInputFormat.class);
      ParquetThriftInputFormat.setInputPaths(job, new Path(parquetPath, "*"));
      ParquetThriftInputFormat.setThriftClass(job.getConfiguration(), StructV3.class);
      job.setMapperClass(TestInputOutputFormat.SchemaEvolutionReadMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(job, outputPath);

      waitForJob(job);
    }

    read(outputPath + "/part-m-00000", 3);
    read(outputPath + "/part-m-00001", 3);
    read(outputPath + "/part-m-00002", 3);
  }

  private void read(String outputPath, int expected) throws FileNotFoundException,
      IOException {
    final BufferedReader out = new BufferedReader(new FileReader(new File(outputPath.toString())));
    String lineOut = null;
    int lineNumber = 0;
    while ((lineOut = out.readLine()) != null) {
      lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
      System.out.println(lineOut);
      ++ lineNumber;
    }
    out.close();
    Assert.assertEquals(expected, lineNumber);
  }

  private void write(final Configuration conf, final Path inputPath,
      final Path parquetPath, Class<? extends Mapper> mapperClass, Class<? extends TBase<?, ?>> outputClass) throws IOException, Exception {
    final Job job = new Job(conf, "write");

    // input not really used
    TextInputFormat.addInputPath(job, inputPath);
    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(ParquetThriftOutputFormat.class);
    ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.GZIP);
    ParquetThriftOutputFormat.setOutputPath(job, parquetPath);
    ParquetThriftOutputFormat.setThriftClass(job, outputClass);

    waitForJob(job);
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
