/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.hadoop;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import static java.lang.Thread.sleep;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class TestInputOutputFormatWithBloomFilter {

  public final String FILE_CONTENT = generateData();
  public static MessageType PARQUET_TYPE = Types.buildMessage()
    .required(BINARY).as(stringType()).named("uuid")
    .required(INT32).named("value")
    .named("FormatWithBF");

  final static double FPP = 0.01;

  private static final Logger LOGGER = LoggerFactory.getLogger(TestInputOutputFormatWithBloomFilter.class);

  public static class Writer extends Mapper<LongWritable, Text, Void, Group> {
    public static final SimpleGroupFactory GROUP_FACTORY = new SimpleGroupFactory(PARQUET_TYPE);
    @Override
    protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      // Create a group from each line
      String line = value.toString();
      for (String tuple : line.split("_")) {
        String[] elements = tuple.split(":");
        Group group = GROUP_FACTORY.newGroup();
        group.add(0, Binary.fromString(elements[0]));
        group.add(1,Integer.parseInt(elements[1]));
        context.write(null, group);
      }
    }
  }
  public static class Reader extends Mapper<LongWritable, Text, Void, Group> {
    public static final SimpleGroupFactory GROUP_FACTORY = new SimpleGroupFactory(PARQUET_TYPE);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Void, Group>.Context context) throws java.io.IOException, InterruptedException {
      Group group = GROUP_FACTORY.newGroup()
        .append("uuid", key.toString())
        .append("value", Integer.parseInt(value.toString()));
      context.write(null, group);
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  /*
  Test bloom filter with two blocks for two columns.
   */
  @Test
  public void testBloomFilter() throws Exception {

    final String[] EXPECTED_DISTINCT_NUMBERS = {"200","50"};

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

    // Don't use a cached FS with a different block size
    conf.set("fs.file.impl.disable.cache", "true");

    // Disable summary metadata.
    conf.set("parquet.summary.metadata.level", "none");
    conf.set("parquet.example.schema", PARQUET_TYPE.toString());

    {
      conf.set("parquet.bloom.filter.column.names", "uuid,value");
      conf.set("parquet.bloom.filter.expected.ndv", String.join(",",EXPECTED_DISTINCT_NUMBERS));

      Job writeJob = new Job(conf, "writeWithBloomFilter");
      writeJob.setInputFormatClass(TextInputFormat.class);
      TextInputFormat.addInputPath(writeJob, new Path(inputFile.toString()));
      writeJob.setOutputFormatClass(ParquetOutputFormat.class);
      writeJob.setMapperClass(TestInputOutputFormatWithBloomFilter.Writer.class);
      writeJob.setNumReduceTasks(0); // write directly to Parquet without reduce
      ParquetOutputFormat.setWriteSupportClass(writeJob, GroupWriteSupport.class);
      ParquetOutputFormat.setBlockSize(writeJob, 2048);
      ParquetOutputFormat.setPageSize(writeJob, 512);
      ParquetOutputFormat.setEnableDictionary(writeJob, false);
      ParquetOutputFormat.setMaxPaddingSize(writeJob, 1023); // always pad
      ParquetOutputFormat.setOutputPath(writeJob, tempPath);

      // Check Bloom filter parameters
      HashMap expectedParameter = new HashMap<String,Long>(){{
        put("uuid",200L);
        put("value",50L);
      }};
      Assert.assertTrue("Bloom filter parameter should be correct inside Hadoop conf.", expectedParameter.equals(ParquetOutputFormat.getBloomFilterColumnExpectedNDVs(conf)));

      waitForJob(writeJob);

    }

    // Check bloom filter for each block and column
    Path path = getDataPath(tempFolder);

    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path);
    ParquetFileReader r = new ParquetFileReader(conf,path,readFooter);

    int startBlock = 0;
    for(BlockMetaData block : readFooter.getBlocks()){
      int rowCount = (int) block.getRowCount();
      int endBlock = rowCount + startBlock;
      int columnIndex = 0;
      for(ColumnChunkMetaData columnMetaData : block.getColumns()) {

        Long expectedDistinctNumbers = Long.parseLong(EXPECTED_DISTINCT_NUMBERS[columnIndex]);

        // Build bloom filter from the generated data
        BloomFilter expectedBloomFilter = buildBloomFilter(columnMetaData, startBlock, endBlock,columnIndex, expectedDistinctNumbers);

        // Read bloom filter from the parquet file
        BloomFilterReader bloomFilterReader = r.getBloomFilterDataReader(block);
        BloomFilter bloomFilter = bloomFilterReader.readBloomFilter(columnMetaData);

        Assert.assertTrue("BloomFilter bitset should be the same", bloomFilter.equals(expectedBloomFilter));

        columnIndex++;
      }
      startBlock = endBlock;
    }

  }

  private String generateData(){
    StringBuilder data = new StringBuilder();
    for(int i =0; i< 200; i++) {
      String uuid = UUID.randomUUID().toString();
      Random random = new Random();
      int value = random.nextInt(50);
      data.append(uuid).append(":").append(Integer.toString(value)).append("_");
    }
    return StringUtils.chop(data.toString());


  }

  private BloomFilter buildBloomFilter(ColumnChunkMetaData columnMetaData, int startBlock,int endBlock, int columnIndex, long expectedDistinctNumbers) {

    String[] tuples = this.FILE_CONTENT.split("_");

    int optimalNumOfBits = BlockSplitBloomFilter.optimalNumOfBits(expectedDistinctNumbers, FPP);
    BloomFilter expectedBloomFilter = new BlockSplitBloomFilter(optimalNumOfBits / 8);

    for (int i = startBlock; i < endBlock; i += 1) {
      String[] elements = tuples[i].split(":");
      if (columnMetaData.getPrimitiveType().getPrimitiveTypeName().equals(BINARY)) {
        expectedBloomFilter.insertHash(expectedBloomFilter.hash(Binary.fromString(elements[columnIndex])));
      } else {
        expectedBloomFilter.insertHash(expectedBloomFilter.hash(Integer.parseInt(elements[columnIndex])));
      }
    }
    return expectedBloomFilter;
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

  private Path getDataPath(File location) {
    File[] files = location.listFiles();
    Path path = null;
    if (files != null) {
      for (File file : files) {
        if (file.getName().startsWith("part-")) {
          path = new Path(file.toString());
          break;
        }
      }
    }
    return path;
  }

}

