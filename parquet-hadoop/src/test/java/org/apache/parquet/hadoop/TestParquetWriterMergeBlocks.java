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
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

public class TestParquetWriterMergeBlocks {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public static final int FILE_SIZE = 10000;
  public static final Configuration CONF = new Configuration();
  public static final Map<String, String> EMPTY_METADATA =
    new HashMap<String, String>();
  public static final MessageType FILE_SCHEMA = Types.buildMessage()
    .required(INT32).named("id")
    .required(BINARY).as(UTF8).named("string")
    .named("AppendTest");
  public static final SimpleGroupFactory GROUP_FACTORY =
    new SimpleGroupFactory(FILE_SCHEMA);

  public Path file1;
  public List<Group> file1content = new ArrayList<Group>();
  public Path file2;
  public List<Group> file2content = new ArrayList<Group>();

  @Before
  public void createSourceData() throws IOException {
    this.file1 = newTemp();
    this.file2 = newTemp();

    ParquetWriter<Group> writer1 = ExampleParquetWriter.builder(file1)
      .withType(FILE_SCHEMA)
      .build();
    ParquetWriter<Group> writer2 = ExampleParquetWriter.builder(file2)
      .withType(FILE_SCHEMA)
      .build();

    for (int i = 0; i < FILE_SIZE; i += 1) {
      Group group1 = GROUP_FACTORY.newGroup();
      group1.add("id", i);
      group1.add("string", UUID.randomUUID().toString());
      writer1.write(group1);
      file1content.add(group1);

      Group group2 = GROUP_FACTORY.newGroup();
      group2.add("id", FILE_SIZE+i);
      group2.add("string", UUID.randomUUID().toString());
      writer2.write(group2);
      file2content.add(group2);
    }

    writer1.close();
    writer2.close();
  }

  @Test
  public void testBasicBehavior() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(
      CONF, FILE_SCHEMA, combinedFile);

    // Merge schema and extraMeta
    List<Path> inputFiles = asList(file1, file2);
    FileMetaData mergedMeta = ParquetFileWriter.mergeMetadataFiles(inputFiles, CONF).getFileMetaData();
    List<InputFile> inputFileList = toInputFiles(inputFiles);
    CodecFactory.BytesCompressor compressor = new CodecFactory(CONF, DEFAULT_PAGE_SIZE).getCompressor(CompressionCodecName.SNAPPY);

    writer.merge(inputFileList, compressor, mergedMeta.getCreatedBy(), 128 * 1024 * 1024);

    LinkedList<Group> expected = new LinkedList<>();
    expected.addAll(file1content);
    expected.addAll(file2content);

    ParquetReader<Group> reader = ParquetReader
      .builder(new GroupReadSupport(), combinedFile)
      .build();

    Group next;
    while ((next = reader.read()) != null) {
      Group expectedNext = expected.removeFirst();
      // check each value; equals is not supported for simple records
      Assert.assertEquals("Each id should match",
        expectedNext.getInteger("id", 0), next.getInteger("id", 0));
      Assert.assertEquals("Each string should match",
        expectedNext.getString("string", 0), next.getString("string", 0));
    }

    Assert.assertEquals("All records should be present", 0, expected.size());
  }

  private List<InputFile> toInputFiles(List<Path> inputFiles) {
    return inputFiles.stream()
      .map(input -> {
        try {
          return HadoopInputFile.fromPath(input, CONF);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());
  }

  @Test
  public void testMergedMetadata() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(
      CONF, FILE_SCHEMA, combinedFile);

    // Merge schema and extraMeta
    List<Path> inputFiles = asList(file1, file2);
    FileMetaData mergedMeta = ParquetFileWriter.mergeMetadataFiles(inputFiles, CONF).getFileMetaData();
    List<InputFile> inputFileList = toInputFiles(inputFiles);
    CompressionCodecName codecName = CompressionCodecName.GZIP;
    CodecFactory.BytesCompressor compressor = new CodecFactory(CONF, DEFAULT_PAGE_SIZE).getCompressor(codecName);
    writer.merge(inputFileList, compressor, mergedMeta.getCreatedBy(), 128 * 1024 * 1024);

    ParquetMetadata combinedFooter = ParquetFileReader.readFooter(
      CONF, combinedFile, NO_FILTER);
    ParquetMetadata f1Footer = ParquetFileReader.readFooter(
      CONF, file1, NO_FILTER);
    ParquetMetadata f2Footer = ParquetFileReader.readFooter(
      CONF, file2, NO_FILTER);

    LinkedList<BlockMetaData> expectedRowGroups = new LinkedList<>();
    expectedRowGroups.addAll(f1Footer.getBlocks());
    expectedRowGroups.addAll(f2Footer.getBlocks());
    long totalRowCount = expectedRowGroups.stream().mapToLong(BlockMetaData::getRowCount).sum();
    Assert.assertEquals("Combined should have a single row group",
      1,
      combinedFooter.getBlocks().size());

    BlockMetaData rowGroup = combinedFooter.getBlocks().get(0);
    Assert.assertEquals("Row count should match",
      totalRowCount, rowGroup.getRowCount());
    assertColumnsEquivalent(f1Footer.getBlocks().get(0).getColumns(), rowGroup.getColumns(), codecName);
  }

  public void assertColumnsEquivalent(List<ColumnChunkMetaData> expected,
                                      List<ColumnChunkMetaData> actual,
                                      CompressionCodecName codecName) {
    Assert.assertEquals("Should have the expected columns",
      expected.size(), actual.size());
    for (int i = 0; i < actual.size(); i += 1) {
      long numNulls = 0;
      long valueCount = 0;
      ColumnChunkMetaData current = actual.get(i);
      Statistics statistics = current.getStatistics();
      numNulls += statistics.getNumNulls();
      valueCount += current.getValueCount();
      if (i != 0) {
        ColumnChunkMetaData previous = actual.get(i - 1);
        long expectedStart = previous.getStartingPos() + previous.getTotalSize();
        Assert.assertEquals("Should start after the previous column",
          expectedStart, current.getStartingPos());
      }

      assertColumnMetadataEquivalent(expected.get(i), current, codecName, numNulls, valueCount);
    }
  }

  public void assertColumnMetadataEquivalent(ColumnChunkMetaData expected,
                                             ColumnChunkMetaData actual,
                                             CompressionCodecName codecName,
                                             long numNulls,
                                             long valueCount) {
    Assert.assertEquals("Should be the expected column",
      expected.getPath(), expected.getPath());
    Assert.assertEquals("Primitive type should not change",
      expected.getType(), actual.getType());
    Assert.assertEquals("Compression codec should not change",
      codecName, actual.getCodec());
    Assert.assertEquals("Data encodings should not change",
      expected.getEncodings(), actual.getEncodings());
    Assert.assertEquals("Statistics should not change",
      numNulls, actual.getStatistics().getNumNulls());
    Assert.assertEquals("Number of values should not change",
      valueCount, actual.getValueCount());

  }

  @Test
  public void testAllowDroppingColumns() throws IOException {
    MessageType droppedColumnSchema = Types.buildMessage()
      .required(BINARY).as(UTF8).named("string")
      .named("AppendTest");

    Path droppedColumnFile = newTemp();
    List<Path> inputFiles = asList(file1, file2);
    ParquetFileWriter writer = new ParquetFileWriter(
      CONF, droppedColumnSchema, droppedColumnFile);
    List<InputFile> inputFileList = toInputFiles(inputFiles);
    CompressionCodecName codecName = CompressionCodecName.GZIP;
    CodecFactory.BytesCompressor compressor = new CodecFactory(CONF, DEFAULT_PAGE_SIZE).getCompressor(codecName);
    writer.merge(inputFileList, compressor, "", 128*1024*1024);

    LinkedList<Group> expected = new LinkedList<Group>();
    expected.addAll(file1content);
    expected.addAll(file2content);

    ParquetMetadata footer = ParquetFileReader.readFooter(
      CONF, droppedColumnFile, NO_FILTER);
    for (BlockMetaData rowGroup : footer.getBlocks()) {
      Assert.assertEquals("Should have only the string column",
        1, rowGroup.getColumns().size());
    }

    ParquetReader<Group> reader = ParquetReader
      .builder(new GroupReadSupport(), droppedColumnFile)
      .build();

    Group next;
    while ((next = reader.read()) != null) {
      Group expectedNext = expected.removeFirst();
      Assert.assertEquals("Each string should match",
        expectedNext.getString("string", 0), next.getString("string", 0));
    }

    Assert.assertEquals("All records should be present", 0, expected.size());
  }

  private Path newTemp() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.toString());
  }
}
