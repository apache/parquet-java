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

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRowGroupMerging {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public static final int FILE_SIZE = 10000;
  public static final Configuration CONF = new Configuration();
  public static final MessageType FILE_SCHEMA = Types.buildMessage()
    .required(INT32).named("id")
    .required(BINARY).as(LogicalTypeAnnotation.stringType()).named("string1")
    .required(BINARY).as(LogicalTypeAnnotation.stringType()).named("string2")
    .named("AppendTest");
  public static final SimpleGroupFactory GROUP_FACTORY =
    new SimpleGroupFactory(FILE_SCHEMA);

  public Path file1;
  public List<Group> file1content = new ArrayList<>();
  public Path file2;
  public List<Group> file2content = new ArrayList<>();
  public Path file3;
  public List<Group> file3content = new ArrayList<>();

  @Before
  public void createSourceData() throws IOException {
    this.file1 = newTemp();
    this.file2 = newTemp();
    this.file3 = newTemp();

    ParquetWriter<Group> writer1 = newWriter(file1, true);
    ParquetWriter<Group> writer2 = newWriter(file2, false);
    ParquetWriter<Group> writer3 = newWriter(file3, false);

    for (int i = 0; i < FILE_SIZE; i += 1) {
      Group group1 = getGroup(writer1, i);
      file1content.add(group1);

      Group group2 = getGroup(writer2, FILE_SIZE + i);
      file2content.add(group2);

      Group group3 = getGroup(writer3, 2 * FILE_SIZE + i);
      file3content.add(group3);
    }

    writer1.close();
    writer2.close();
    writer3.close();
  }

  @Test
  public void testBasicBehavior() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = createFileWriter(combinedFile);

    Configuration conf = new Configuration();

    merge(writer, conf, Integer.MAX_VALUE, true, SNAPPY, Collections.emptyMap(), file1, file2, file3);


    LinkedList<Group> expected = new LinkedList<>();
    expected.addAll(file1content);
    expected.addAll(file2content);
    expected.addAll(file3content);

    ParquetReader<Group> reader = ParquetReader
      .builder(new GroupReadSupport(), combinedFile)
      .build();

    Group next;
    while ((next = reader.read()) != null) {
      Group expectedNext = expected.removeFirst();
      // check each value; equals is not supported for simple records
      assertEquals("Each id should match",
        expectedNext.getInteger("id", 0), next.getInteger("id", 0));
      assertEquals("Each string should match",
        expectedNext.getString("string1", 0), next.getString("string1", 0));
      assertEquals("Each string should match",
        expectedNext.getString("string2", 0), next.getString("string2", 0));
    }
    assertEquals("All records should be present", 0, expected.size());

  }

  @Test
  public void testFileStructure() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = createFileWriter(combinedFile);

    Configuration conf = new Configuration();

    merge(writer, conf, Integer.MAX_VALUE, false, GZIP, Collections.singletonMap("test-key", "value1"),
      file1, file2, file3);

    ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(combinedFile, conf));
    assertEquals("Should be combined into 1 row group", reader.getRowGroups().size(), 1);
    assertEquals("Schema should match the original", reader.getFileMetaData().getSchema(), FILE_SCHEMA);
    assertEquals("Row count should be sum of the original counts",
      reader.getRowGroups().get(0).getRowCount(), FILE_SIZE * 3);

    assertTrue("all columns are expected to use Gzip compression",
      reader.getRowGroups().stream()
        .flatMap(g -> g.getColumns().stream())
        .allMatch(c -> c.getCodec().equals(GZIP)));

    assertTrue("Column string1(at pos 1) is expected to be dictionary encoded",
      reader.getRowGroups().stream().map(g -> g.getColumns().get(1))
        .allMatch(c -> c.getEncodingStats().hasDictionaryPages() && c.getEncodingStats().hasDictionaryEncodedPages()));
  }

  @Test
  public void testNewUncompressedBlocksSizing() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = createFileWriter(combinedFile);

    Configuration conf = new Configuration();

    long file1Rows = sumTotalFromGroups(conf, file1, BlockMetaData::getRowCount);
    long file2Rows = sumTotalFromGroups(conf, file2, BlockMetaData::getRowCount);
    long file3Rows = sumTotalFromGroups(conf, file3, BlockMetaData::getRowCount);

    long file1Size = sumTotalFromGroups(conf, file1, BlockMetaData::getTotalByteSize);
    long file2Size = sumTotalFromGroups(conf, file2, BlockMetaData::getTotalByteSize);
    long maxRowGroupSize = file1Size + file2Size;

    merge(writer, conf, maxRowGroupSize, false, UNCOMPRESSED, Collections.emptyMap(), file1, file2, file3);

    ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(combinedFile, conf));
    assertEquals("Should be combined into 2 row groups ", 2, reader.getRowGroups().size());
    assertEquals("first row-group count should equal the sum of the first 2 groups combined", file1Rows + file2Rows,
      reader.getRowGroups().get(0).getRowCount());
    assertEquals("the second row-group count should be equal to the to total of the third file", file3Rows,
      reader.getRowGroups().get(1).getRowCount());
  }

  @Test
  public void testNewCompressedBlocksSizing() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = createFileWriter(combinedFile);

    Configuration conf = new Configuration();

    long file1Rows = sumTotalFromGroups(conf, file1, BlockMetaData::getRowCount);
    long file2Rows = sumTotalFromGroups(conf, file2, BlockMetaData::getRowCount);
    long file3Rows = sumTotalFromGroups(conf, file3, BlockMetaData::getRowCount);

    long maxRowGroupSize = sumTotalFromGroups(conf, file1, BlockMetaData::getCompressedSize)
      + sumTotalFromGroups(conf, file2, BlockMetaData::getCompressedSize);

    merge(writer, conf, maxRowGroupSize, true, SNAPPY, Collections.emptyMap(), file1, file2, file3);

    ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(combinedFile, conf));
    assertEquals("Should be combined into 2 row groups ", 2, reader.getRowGroups().size());
    assertEquals("first row-group count should equal the sum of the first 2 groups combined",
      file1Rows + file2Rows, reader.getRowGroups().get(0).getRowCount());

    assertEquals("the second row-group count should be equal to the to total of the third file",
      file3Rows, reader.getRowGroups().get(1).getRowCount());
  }

  private static void merge(ParquetFileWriter writer, Configuration conf, long maxRowGroupSize, boolean useV2,
                            CompressionCodecName codec, Map<String, String> extraMeta, Path... files) throws IOException {
    writer.start();
    writer.mergeRowGroups(Arrays.stream(files).map(f -> toHadoopInputFile(f, conf)).collect(Collectors.toList()),
      maxRowGroupSize, useV2, codec);
    writer.end(extraMeta);
  }

  private long sumTotalFromGroups(Configuration conf, Path file1, ToLongFunction<BlockMetaData> getRowCount) throws IOException {
    return ParquetFileReader.open(HadoopInputFile.fromPath(file1, conf)).getRowGroups().stream()
      .mapToLong(getRowCount)
      .sum();
  }

  private ParquetFileWriter createFileWriter(Path combinedFile) throws IOException {
    return new ParquetFileWriter(
      CONF, FILE_SCHEMA, combinedFile);
  }

  private static HadoopInputFile toHadoopInputFile(Path path, Configuration conf) {
    try {
      return HadoopInputFile.fromPath(path, conf);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Path newTemp() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.toString());
  }

  private Group getGroup(ParquetWriter<Group> writer1, int i) throws IOException {
    Group group1 = GROUP_FACTORY.newGroup();
    group1.add("id", i);
    group1.add("string1", "string1-125ahda-2090-410a-b249-59eb61ca17c6-" + i % 100); //force dictionary
    group1.add("string2", "string2- 125ahda-2090-410a-b249-59eb61ca17c6-" + i);
    writer1.write(group1);
    return group1;
  }

  private ParquetWriter<Group> newWriter(Path file1, boolean v2) throws IOException {
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(file1)
      .withType(FILE_SCHEMA)
      .withCompressionCodec(SNAPPY);
    if (v2)
      builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);
    return builder.build();
  }
}
