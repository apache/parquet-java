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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
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
      Assert.assertEquals(Collections.emptyList(), sizeStatistics.getRepetitionLevelHistogram());
      Assert.assertEquals(Collections.emptyList(), sizeStatistics.getDefinitionLevelHistogram());

      ColumnIndex columnIndex = reader.readColumnIndex(column);
      Assert.assertEquals(Collections.emptyList(), columnIndex.getRepetitionLevelHistogram());
      Assert.assertEquals(Collections.emptyList(), columnIndex.getDefinitionLevelHistogram());

      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      Assert.assertEquals(2, offsetIndex.getPageCount());
      Assert.assertEquals(Optional.of(2L), offsetIndex.getUnencodedByteArrayDataBytes(0));
      Assert.assertEquals(Optional.of(2L), offsetIndex.getUnencodedByteArrayDataBytes(1));
    }
  }

  @Test
  public void testNestedRepeatedOptionalColumnSizeStatistics() throws IOException {
    MessageType schema = Types.buildMessage()
        .optionalGroup()
        .repeatedGroup()
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("value")
        .named("list")
        .named("outer")
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
      // Create groups with different nesting patterns
      Group g1 = factory.newGroup();
      Group outer1 = g1.addGroup("outer");
      Group list1 = outer1.addGroup("list");
      list1.append("value", "a");
      Group list2 = outer1.addGroup("list");
      list2.append("value", "b");
      writer.write(g1);

      Group g2 = factory.newGroup();
      Group outer2 = g2.addGroup("outer");
      Group list3 = outer2.addGroup("list");
      list3.append("value", "c");
      writer.write(g2);
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      ParquetMetadata footer = reader.getFooter();
      ColumnChunkMetaData column = footer.getBlocks().get(0).getColumns().get(0);

      SizeStatistics sizeStatistics = column.getSizeStatistics();
      Assert.assertEquals(Optional.of(3L), sizeStatistics.getUnencodedByteArrayDataBytes());
      Assert.assertEquals(List.of(2L, 1L), sizeStatistics.getRepetitionLevelHistogram());
      Assert.assertEquals(List.of(0L, 0L, 0L, 3L), sizeStatistics.getDefinitionLevelHistogram());

      ColumnIndex columnIndex = reader.readColumnIndex(column);
      Assert.assertEquals(List.of(2L, 1L), sizeStatistics.getRepetitionLevelHistogram());
      Assert.assertEquals(List.of(0L, 0L, 0L, 3L), sizeStatistics.getDefinitionLevelHistogram());

      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      Assert.assertEquals(1, offsetIndex.getPageCount());
      Assert.assertEquals(Optional.of(3L), offsetIndex.getUnencodedByteArrayDataBytes(0));
    }
  }

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }
}
