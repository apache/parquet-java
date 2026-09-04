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
package org.apache.parquet.hadoop.example;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.DelegatingWriteSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parameterized on Vectored IO enabled/disabled so can verify that
 * ranged reads work through the bridge on compatible hadoop versions.
 */
public class TestInputOutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(TestInputOutputFormat.class);

  final Path parquetPath = new Path("target/test/example/TestInputOutputFormat/parquet");
  final Path inputPath = new Path("src/test/java/org/apache/parquet/hadoop/example/TestInputOutputFormat.java");
  final Path outputPath = new Path("target/test/example/TestInputOutputFormat/out");
  Job writeJob;
  Job readJob;
  private String writeSchema;
  private String readSchema;
  private String partialSchema;

  private Class<? extends Mapper<?, ?, ?, ?>> readMapperClass;
  private Class<? extends Mapper<?, ?, ?, ?>> writeMapperClass;

  @BeforeEach
  public void setUp() {
    writeSchema = "message example {\n" + "required int32 line;\n" + "required binary content;\n" + "}";

    readSchema = "message example {\n" + "required int32 line;\n" + "required binary content;\n" + "}";

    partialSchema = "message example {\n" + "required int32 line;\n" + "}";

    readMapperClass = ReadMapper.class;
    writeMapperClass = WriteMapper.class;
  }

  private Configuration createConf(boolean readType) {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED, readType);
    return conf;
  }

  public static final class MyWriteSupport extends DelegatingWriteSupport<Group> {

    private long count = 0;

    public MyWriteSupport() {
      super(new GroupWriteSupport());
    }

    @Override
    public void write(Group record) {
      super.write(record);
      ++count;
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext finalizeWrite() {
      Map<String, String> extraMetadata = new HashMap<String, String>();
      extraMetadata.put("my.count", String.valueOf(count));
      return new FinalizedWriteContext(extraMetadata);
    }
  }

  public static final class MyReadSupport extends DelegatingReadSupport<Group> {
    public MyReadSupport() {
      super(new GroupReadSupport());
    }

    @Override
    public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
      Set<String> counts = context.getKeyValueMetadata().get("my.count");
      assertThat(counts).as("counts: " + counts).isNotEmpty();
      return super.init(context);
    }
  }

  public static class ReadMapper extends Mapper<LongWritable, Text, Void, Group> {
    private SimpleGroupFactory factory;

    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Void, Group>.Context context)
        throws java.io.IOException, InterruptedException {
      factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context)));
    }

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Void, Group>.Context context)
        throws java.io.IOException, InterruptedException {
      Group group = factory.newGroup().append("line", (int) key.get()).append("content", value.toString());
      context.write(null, group);
    }
  }

  public static class WriteMapper extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, Group value, Mapper<Void, Group, LongWritable, Text>.Context context)
        throws IOException, InterruptedException {
      context.write(new LongWritable(value.getInteger("line", 0)), new Text(value.getString("content", 0)));
    }
  }

  public static class PartialWriteMapper extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, Group value, Mapper<Void, Group, LongWritable, Text>.Context context)
        throws IOException, InterruptedException {
      context.write(new LongWritable(value.getInteger("line", 0)), new Text("dummy"));
    }
  }

  private void runMapReduceJob(Configuration conf, CompressionCodecName codec)
      throws IOException, ClassNotFoundException, InterruptedException {
    runMapReduceJob(conf, codec, Collections.<String, String>emptyMap());
  }

  private void runMapReduceJob(Configuration conf, CompressionCodecName codec, Map<String, String> extraConf)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration jobConf = new Configuration(conf);
    for (Map.Entry<String, String> entry : extraConf.entrySet()) {
      jobConf.set(entry.getKey(), entry.getValue());
    }
    final FileSystem fileSystem = parquetPath.getFileSystem(jobConf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      writeJob = new Job(jobConf, "write");
      TextInputFormat.addInputPath(writeJob, inputPath);
      writeJob.setInputFormatClass(TextInputFormat.class);
      writeJob.setNumReduceTasks(0);
      ParquetOutputFormat.setCompression(writeJob, codec);
      ParquetOutputFormat.setOutputPath(writeJob, parquetPath);
      writeJob.setOutputFormatClass(ParquetOutputFormat.class);
      writeJob.setMapperClass(readMapperClass);

      ParquetOutputFormat.setWriteSupportClass(writeJob, MyWriteSupport.class);
      GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), writeJob.getConfiguration());
      writeJob.submit();
      waitForJob(writeJob);
    }
    {
      jobConf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
      readJob = new Job(jobConf, "read");

      readJob.setInputFormatClass(ParquetInputFormat.class);
      ParquetInputFormat.setReadSupportClass(readJob, MyReadSupport.class);

      ParquetInputFormat.setInputPaths(readJob, parquetPath);
      readJob.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(readJob, outputPath);
      readJob.setMapperClass(writeMapperClass);
      readJob.setNumReduceTasks(0);
      readJob.submit();
      waitForJob(readJob);
    }
  }

  private void testReadWrite(Configuration conf, CompressionCodecName codec)
      throws IOException, ClassNotFoundException, InterruptedException {
    testReadWrite(conf, codec, Collections.<String, String>emptyMap());
  }

  private void testReadWrite(Configuration conf, CompressionCodecName codec, Map<String, String> extraConf)
      throws IOException, ClassNotFoundException, InterruptedException {
    runMapReduceJob(conf, codec, extraConf);
    final BufferedReader in = new BufferedReader(new FileReader(new File(inputPath.toString())));
    final BufferedReader out = new BufferedReader(new FileReader(new File(outputPath.toString(), "part-m-00000")));
    String lineIn;
    String lineOut = null;
    int lineNumber = 0;
    while ((lineIn = in.readLine()) != null && (lineOut = out.readLine()) != null) {
      ++lineNumber;
      lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
      assertThat(lineOut).as("line " + lineNumber).isEqualTo(lineIn);
    }
    assertThat(out.readLine()).as("line " + lineNumber).isNull();
    assertThat(lineIn).as("line " + lineNumber).isNull();
    in.close();
    out.close();
  }

  @ParameterizedTest(name = "vectored : {0}")
  @ValueSource(booleans = {true, false})
  public void testReadWrite(boolean readType) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = createConf(readType);
    // TODO: Lzo requires additional external setup steps so leave it out for now
    testReadWrite(conf, CompressionCodecName.GZIP);
    testReadWrite(conf, CompressionCodecName.UNCOMPRESSED);
    testReadWrite(conf, CompressionCodecName.SNAPPY);
    testReadWrite(conf, CompressionCodecName.ZSTD);
  }

  @ParameterizedTest(name = "vectored : {0}")
  @ValueSource(booleans = {true, false})
  public void testReadWriteTaskSideMD(boolean readType)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = createConf(readType);
    testReadWrite(conf, CompressionCodecName.UNCOMPRESSED, new HashMap<String, String>() {
      {
        put("parquet.task.side.metadata", "true");
      }
    });
  }

  /**
   * Uses a filter that drops all records to test handling of tasks (mappers) that need to do no work at all
   */
  @ParameterizedTest(name = "vectored : {0}")
  @ValueSource(booleans = {true, false})
  public void testReadWriteTaskSideMDAggressiveFilter(boolean readType)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    // this filter predicate should trigger row group filtering that drops all row-groups
    ParquetInputFormat.setFilterPredicate(conf, FilterApi.eq(FilterApi.intColumn("line"), -1000));
    final String fpString = conf.get(ParquetInputFormat.FILTER_PREDICATE);

    runMapReduceJob(conf, CompressionCodecName.UNCOMPRESSED, new HashMap<String, String>() {
      {
        put("parquet.task.side.metadata", "true");
        put(ParquetInputFormat.FILTER_PREDICATE, fpString);
      }
    });

    File file = new File(outputPath.toString(), "part-m-00000");
    List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
    assertThat(lines).isEmpty();
  }

  @ParameterizedTest(name = "vectored : {0}")
  @ValueSource(booleans = {true, false})
  public void testReadWriteFilter(boolean readType) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    // this filter predicate should keep some records but not all (first 500 characters)
    // "line" is actually position in the file...
    ParquetInputFormat.setFilterPredicate(conf, FilterApi.lt(FilterApi.intColumn("line"), 500));
    final String fpString = conf.get(ParquetInputFormat.FILTER_PREDICATE);

    runMapReduceJob(conf, CompressionCodecName.UNCOMPRESSED, new HashMap<String, String>() {
      {
        put("parquet.task.side.metadata", "true");
        put(ParquetInputFormat.FILTER_PREDICATE, fpString);
      }
    });

    File file = new File(inputPath.toString());
    List<String> expected = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

    // grab the lines that contain the first 500 characters (including the rest of the line past 500 characters)
    int size = 0;
    Iterator<String> iter = expected.iterator();
    while (iter.hasNext()) {
      String next = iter.next();

      if (size < 500) {
        size += next.length();
        continue;
      }

      iter.remove();
    }

    // put the output back into it's original format (remove the character counts / tabs)
    File file2 = new File(outputPath.toString(), "part-m-00000");
    List<String> found = Files.readAllLines(file2.toPath(), StandardCharsets.UTF_8);
    StringBuilder sbFound = new StringBuilder();
    for (String line : found) {
      sbFound.append(line.split("\t", -1)[1]);
      sbFound.append("\n");
    }

    sbFound.deleteCharAt(sbFound.length() - 1);

    assertThat(sbFound).asString().isEqualTo(String.join("\n", expected));
  }

  @ParameterizedTest(name = "vectored : {0}")
  @ValueSource(booleans = {true, false})
  public void testProjection(boolean readType) throws Exception {
    Configuration conf = createConf(readType);
    readSchema = partialSchema;
    writeMapperClass = PartialWriteMapper.class;
    runMapReduceJob(conf, CompressionCodecName.GZIP);
  }

  private static long value(Job job, String groupName, String name) throws Exception {
    // getGroup moved to AbstractCounters
    Method getGroup = org.apache.hadoop.mapreduce.Counters.class.getMethod("getGroup", String.class);
    // CounterGroup changed to an interface
    Method findCounter = org.apache.hadoop.mapreduce.CounterGroup.class.getMethod("findCounter", String.class);
    // Counter changed to an interface
    Method getValue = org.apache.hadoop.mapreduce.Counter.class.getMethod("getValue");
    CounterGroup group = (CounterGroup) getGroup.invoke(job.getCounters(), groupName);
    Counter counter = (Counter) findCounter.invoke(group, name);
    return (Long) getValue.invoke(counter);
  }

  @ParameterizedTest(name = "vectored : {0}")
  @ValueSource(booleans = {true, false})
  public void testReadWriteWithCounter(boolean readType) throws Exception {
    Configuration conf = createConf(readType);
    runMapReduceJob(conf, CompressionCodecName.GZIP);

    assertThat(value(readJob, "parquet", "bytesread")).isPositive();
    assertThat(value(readJob, "parquet", "bytestotal")).isPositive();
    assertThat(value(readJob, "parquet", "bytesread"))
        .as("bytestotal != bytesread")
        .isEqualTo(value(readJob, "parquet", "bytestotal"));
    // not testing the time read counter since it could be zero due to the size of data is too small
  }

  @ParameterizedTest(name = "vectored : {0}")
  @ValueSource(booleans = {true, false})
  public void testReadWriteWithoutCounter(boolean readType) throws Exception {
    Configuration conf = createConf(readType);
    conf.set("parquet.benchmark.time.read", "false");
    conf.set("parquet.benchmark.bytes.total", "false");
    conf.set("parquet.benchmark.bytes.read", "false");
    runMapReduceJob(conf, CompressionCodecName.GZIP);
    assertThat(value(readJob, "parquet", "bytesread")).isZero();
    assertThat(value(readJob, "parquet", "bytestotal")).isZero();
    assertThat(value(readJob, "parquet", "timeread")).isZero();
  }

  private void waitForJob(Job job) throws InterruptedException, IOException {
    while (!job.isComplete()) {
      LOG.debug("waiting for job {}", job.getJobName());
      sleep(100);
    }
    LOG.info("status for job {}: {}", job.getJobName(), (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }
}
