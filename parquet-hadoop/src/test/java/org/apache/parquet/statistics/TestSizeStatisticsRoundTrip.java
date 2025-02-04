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
package org.apache.parquet.statistics;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSizeStatisticsRoundTrip {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testBinaryColumnSizeStatistics() throws IOException {
    MessageType schema = Types.buildMessage()
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("name")
        .named("msg");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    GroupFactory factory = new SimpleGroupFactory(schema);
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withPageRowCountLimit(2)
        .withMinRowCountForPageSizeCheck(1)
        .withConf(conf)
        .build()) {
      writer.write(factory.newGroup().append("name", "a"));
      writer.write(factory.newGroup().append("name", "b"));
      writer.write(factory.newGroup().append("name", "c"));
      writer.write(factory.newGroup().append("name", "d"));
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      ParquetMetadata footer = reader.getFooter();
      ColumnChunkMetaData column = footer.getBlocks().get(0).getColumns().get(0);

      SizeStatistics sizeStatistics = column.getSizeStatistics();
      Assert.assertEquals(Optional.of(4L), sizeStatistics.getUnencodedByteArrayDataBytes());
      Assert.assertEquals(Arrays.asList(4L), sizeStatistics.getRepetitionLevelHistogram());
      Assert.assertEquals(Arrays.asList(0L, 4L), sizeStatistics.getDefinitionLevelHistogram());

      ColumnIndex columnIndex = reader.readColumnIndex(column);
      Assert.assertEquals(Arrays.asList(2L, 2L), columnIndex.getRepetitionLevelHistogram());
      Assert.assertEquals(Arrays.asList(0L, 2L, 0L, 2L), columnIndex.getDefinitionLevelHistogram());

      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      Assert.assertEquals(2, offsetIndex.getPageCount());
      Assert.assertEquals(Optional.of(2L), offsetIndex.getUnencodedByteArrayDataBytes(0));
      Assert.assertEquals(Optional.of(2L), offsetIndex.getUnencodedByteArrayDataBytes(1));
    }
  }

  private static final List<List<Integer>> REPEATED_RECORD_VALUES = ImmutableList.of(
      ImmutableList.of(1, 2, 3), ImmutableList.of(1), ImmutableList.of(1, 2), ImmutableList.of());

  private static final MessageType MESSAGE_SCHEMA_REPEATED_FIELD = Types.buildMessage()
      .addField(Types.requiredGroup()
          .as(LogicalTypeAnnotation.listType())
          .addField(Types.repeatedGroup()
              .addField(Types.primitive(INT32, REQUIRED).named("element"))
              .named("list"))
          .named("my_list"))
      .named("MyRecord");

  @Test
  public void testSizeStatsWrittenWithExampleWriter() throws Exception {
    final File tmp = File.createTempFile(TestSizeStatisticsRoundTrip.class.getSimpleName(), ".parquet");
    tmp.deleteOnExit();
    tmp.delete();
    final Path file = new Path(tmp.getPath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .config(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, MESSAGE_SCHEMA_REPEATED_FIELD.toString())
        .build()) {

      for (List<Integer> arrayValue : REPEATED_RECORD_VALUES) {
        final SimpleGroup record = new SimpleGroup(MESSAGE_SCHEMA_REPEATED_FIELD);

        Group listField = record.addGroup("my_list");
        for (Integer value : arrayValue) {
          Group list = listField.addGroup("list");
          if (value != null) {
            list.append("element", value);
          }
        }
        writer.write(record);
      }
    }

    final SizeStatistics stats = getSizeStatisticsFromFile(file);

    Assert.assertEquals(ImmutableList.of(1L, 6L), stats.getDefinitionLevelHistogram());
    Assert.assertEquals(ImmutableList.of(4L, 3L), stats.getRepetitionLevelHistogram());
  }

  @Test
  public void testSizeStatsWrittenWithRecordConsumer() throws Exception {
    final File tmp = File.createTempFile(TestSizeStatisticsRoundTrip.class.getSimpleName(), ".parquet");
    tmp.deleteOnExit();
    tmp.delete();
    final Path file = new Path(tmp.getPath());

    ParquetWriter<List<Integer>> writer = new ParquetWriter<>(file, new WriteSupport<List<Integer>>() {
      RecordConsumer rc = null;

      @Override
      public WriteSupport.WriteContext init(Configuration configuration) {
        return init((ParquetConfiguration) null);
      }

      @Override
      public WriteContext init(ParquetConfiguration configuration) {
        return new WriteContext(MESSAGE_SCHEMA_REPEATED_FIELD, new HashMap<>());
      }

      @Override
      public void prepareForWrite(RecordConsumer recordConsumer) {
        this.rc = recordConsumer;
      }

      @Override
      public void write(List<Integer> arrayValue) {
        rc.startMessage();

        rc.startField("my_list", 0);
        rc.startGroup();
        if (arrayValue != null) {
          for (int i = 0; i < arrayValue.size(); i++) {
            rc.startField("list", 0);
            rc.startGroup();

            rc.startField("element", 0);
            rc.addInteger(arrayValue.get(i));
            rc.endField("element", 0);

            rc.endGroup();
            rc.endField("list", 0);
          }
        }
        rc.endGroup();
        rc.endField("my_list", 0);
        rc.endMessage();
      }
    });

    for (List<Integer> recordArrayValue : REPEATED_RECORD_VALUES) {
      writer.write(recordArrayValue);
    }

    writer.close();

    final SizeStatistics stats = getSizeStatisticsFromFile(file);

    // Assert that these records have the same rep- and def-level histograms as the ExampleParquetWriter test

    // this assertion passes
    Assert.assertEquals(ImmutableList.of(1L, 6L), stats.getDefinitionLevelHistogram());

    // this assertion FAILS, actual repetition list is [7, 0]
    Assert.assertEquals(ImmutableList.of(4L, 3L), stats.getRepetitionLevelHistogram());
  }

  private static SizeStatistics getSizeStatisticsFromFile(Path file) throws IOException {
    final ParquetMetadata footer =
        ParquetFileReader.readFooter(new Configuration(), file, ParquetMetadataConverter.NO_FILTER);
    assert (footer.getBlocks().size() == 1);
    final BlockMetaData blockMetaData = footer.getBlocks().get(0);
    assert (blockMetaData.getColumns().size() == 1);
    return blockMetaData.getColumns().get(0).getSizeStatistics();
  }

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }
}
