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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import static java.lang.Thread.sleep;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class TestInputOutputFormatWithPadding {
  public static final String FILE_CONTENT = "" + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,"
      + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,"
      + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  public static MessageType PARQUET_TYPE = Types.buildMessage().required(BINARY).as(UTF8).named("uuid").required(BINARY)
      .as(UTF8).named("char").named("FormatTestObject");

  /**
   * ParquetInputFormat that will not split the input file (easier validation)
   */
  private static class NoSplits extends ParquetInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
      return false;
    }
  }

  public static class Writer extends Mapper<LongWritable, Text, Void, Group> {
    public static final SimpleGroupFactory GROUP_FACTORY = new SimpleGroupFactory(PARQUET_TYPE);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // writes each character of the line with a UUID
      String line = value.toString();
      for (int i = 0; i < line.length(); i += 1) {
        Group group = GROUP_FACTORY.newGroup();
        group.add(0, Binary.fromString(UUID.randomUUID().toString()));
        group.add(1, Binary.fromString(line.substring(i, i + 1)));
        context.write(null, group);
      }
    }
  }

  public static class Reader extends Mapper<Void, Group, LongWritable, Text> {
    @Override
    protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
      context.write(null, new Text(value.getString("char", 0)));
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testBasicBehaviorWithPadding() throws Exception {
    HadoopOutputFile.getBlockFileSystems().add("file");

    File inputFile = temp.newFile();
    FileOutputStream out = new FileOutputStream(inputFile);
    out.write(FILE_CONTENT.getBytes("UTF-8"));
    out.close();

    File tempFolder = temp.newFolder();
    tempFolder.delete();
    Path tempPath = new Path(tempFolder.toURI());

    File outputFolder = temp.newFile();
    outputFolder.delete();

    Configuration conf = new Configuration();
    // May test against multiple hadoop versions
    conf.set("dfs.block.size", "1024");
    conf.set("dfs.blocksize", "1024");
    conf.set("dfs.blockSize", "1024");
    conf.set("fs.local.block.size", "1024");

    // don't use a cached FS with a different block size
    conf.set("fs.file.impl.disable.cache", "true");

    // disable summary metadata, it isn't needed
    conf.set("parquet.enable.summary-metadata", "false");
    conf.set("parquet.example.schema", PARQUET_TYPE.toString());

    {
      Job writeJob = new Job(conf, "write");
      writeJob.setInputFormatClass(TextInputFormat.class);
      TextInputFormat.addInputPath(writeJob, new Path(inputFile.toString()));

      writeJob.setOutputFormatClass(ParquetOutputFormat.class);
      writeJob.setMapperClass(Writer.class);
      writeJob.setNumReduceTasks(0); // write directly to Parquet without reduce
      ParquetOutputFormat.setWriteSupportClass(writeJob, GroupWriteSupport.class);
      ParquetOutputFormat.setBlockSize(writeJob, 1024);
      ParquetOutputFormat.setPageSize(writeJob, 512);
      ParquetOutputFormat.setDictionaryPageSize(writeJob, 512);
      ParquetOutputFormat.setEnableDictionary(writeJob, true);
      ParquetOutputFormat.setMaxPaddingSize(writeJob, 1023); // always pad
      ParquetOutputFormat.setOutputPath(writeJob, tempPath);

      waitForJob(writeJob);
    }

    // make sure padding was added
    File parquetFile = getDataFile(tempFolder);
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, new Path(parquetFile.toString()),
        ParquetMetadataConverter.NO_FILTER);
    for (BlockMetaData block : footer.getBlocks()) {
      Assert.assertTrue("Block should start at a multiple of the block size", block.getStartingPos() % 1024 == 0);
    }

    {
      Job readJob = new Job(conf, "read");
      readJob.setInputFormatClass(NoSplits.class);
      ParquetInputFormat.setReadSupportClass(readJob, GroupReadSupport.class);
      TextInputFormat.addInputPath(readJob, tempPath);

      readJob.setOutputFormatClass(TextOutputFormat.class);
      readJob.setMapperClass(Reader.class);
      readJob.setNumReduceTasks(0); // write directly to text without reduce
      TextOutputFormat.setOutputPath(readJob, new Path(outputFolder.toString()));

      waitForJob(readJob);
    }

    File dataFile = getDataFile(outputFolder);
    Assert.assertNotNull("Should find a data file", dataFile);

    StringBuilder contentBuilder = new StringBuilder();
    for (String line : Files.readAllLines(dataFile.toPath(), StandardCharsets.UTF_8)) {
      contentBuilder.append(line);
    }
    String reconstructed = contentBuilder.toString();
    Assert.assertEquals("Should match written file content", FILE_CONTENT, reconstructed);

    HadoopOutputFile.getBlockFileSystems().remove("file");
  }

  private void waitForJob(Job job) throws Exception {
    job.submit();
    while (!job.isComplete()) {
      sleep(100);
    }
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }

  private File getDataFile(File location) {
    File[] files = location.listFiles();
    File dataFile = null;
    if (files != null) {
      for (File file : files) {
        if (file.getName().startsWith("part-")) {
          dataFile = file;
          break;
        }
      }
    }
    return dataFile;
  }
}
