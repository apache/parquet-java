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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
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
      assertThat(sizeStatistics.getUnencodedByteArrayDataBytes()).isEqualTo(Optional.of(4L));
      assertThat(sizeStatistics.getRepetitionLevelHistogram()).isEmpty();
      assertThat(sizeStatistics.getDefinitionLevelHistogram()).isEmpty();

      ColumnIndex columnIndex = reader.readColumnIndex(column);
      assertThat(columnIndex.getRepetitionLevelHistogram()).isEmpty();
      assertThat(columnIndex.getDefinitionLevelHistogram()).isEmpty();

      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      assertThat(offsetIndex.getPageCount()).isEqualTo(2);
      assertThat(offsetIndex.getUnencodedByteArrayDataBytes(0)).isEqualTo(Optional.of(2L));
      assertThat(offsetIndex.getUnencodedByteArrayDataBytes(1)).isEqualTo(Optional.of(2L));
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
      assertThat(sizeStatistics.getUnencodedByteArrayDataBytes()).isEqualTo(Optional.of(3L));
      assertThat(sizeStatistics.getRepetitionLevelHistogram()).containsExactly(2L, 1L);
      assertThat(sizeStatistics.getDefinitionLevelHistogram()).containsExactly(0L, 0L, 0L, 3L);

      ColumnIndex columnIndex = reader.readColumnIndex(column);
      assertThat(columnIndex.getRepetitionLevelHistogram()).containsExactly(2L, 1L);
      assertThat(columnIndex.getDefinitionLevelHistogram()).containsExactly(0L, 0L, 0L, 3L);

      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      assertThat(offsetIndex.getPageCount()).isEqualTo(1);
      assertThat(offsetIndex.getUnencodedByteArrayDataBytes(0)).isEqualTo(Optional.of(3L));
    }
  }

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }
}
