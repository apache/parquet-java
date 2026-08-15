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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetWriterAppendBlocks {
  @TempDir
  private java.nio.file.Path tempDir;

  public static final int FILE_SIZE = 10000;
  public static final Configuration CONF = new Configuration();
  public static final Map<String, String> EMPTY_METADATA = new HashMap<String, String>();
  public static final MessageType FILE_SCHEMA = Types.buildMessage()
      .required(INT32)
      .named("id")
      .required(BINARY)
      .as(UTF8)
      .named("string")
      .named("AppendTest");
  public static final SimpleGroupFactory GROUP_FACTORY = new SimpleGroupFactory(FILE_SCHEMA);

  private static final Path STATIC_FILE_1 = createPathFromCP("/test-append_1.parquet");
  private static final Path STATIC_FILE_2 = createPathFromCP("/test-append_2.parquet");

  private static Path createPathFromCP(String path) {
    try {
      return new Path(
          TestParquetWriterAppendBlocks.class.getResource(path).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public Path file1;
  public List<Group> file1content = new ArrayList<Group>();
  public Path file2;
  public List<Group> file2content = new ArrayList<Group>();

  @BeforeEach
  public void createSourceData() throws IOException {
    this.file1 = newTemp();
    this.file2 = newTemp();

    ParquetWriter<Group> writer1 =
        ExampleParquetWriter.builder(file1).withType(FILE_SCHEMA).build();
    ParquetWriter<Group> writer2 =
        ExampleParquetWriter.builder(file2).withType(FILE_SCHEMA).build();

    for (int i = 0; i < FILE_SIZE; i += 1) {
      Group group1 = GROUP_FACTORY.newGroup();
      group1.add("id", i);
      group1.add("string", UUID.randomUUID().toString());
      writer1.write(group1);
      file1content.add(group1);

      Group group2 = GROUP_FACTORY.newGroup();
      group2.add("id", FILE_SIZE + i);
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
    ParquetFileWriter writer = new ParquetFileWriter(CONF, FILE_SCHEMA, combinedFile);
    writer.start();
    writer.appendFile(CONF, file1);
    writer.appendFile(CONF, file2);
    writer.end(EMPTY_METADATA);

    LinkedList<Group> expected = new LinkedList<Group>();
    expected.addAll(file1content);
    expected.addAll(file2content);

    ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), combinedFile).build();

    Group next;
    while ((next = reader.read()) != null) {
      Group expectedNext = expected.removeFirst();
      // check each value; equals is not supported for simple records
      assertThat(next.getInteger("id", 0)).as("Each id should match").isEqualTo(expectedNext.getInteger("id", 0));
      assertThat(next.getString("string", 0))
          .as("Each string should match")
          .isEqualTo(expectedNext.getString("string", 0));
    }

    assertThat(expected).as("All records should be present").isEmpty();
  }

  /**
   * This test is similar to {@link #testBasicBehavior()} only that it uses static files generated by a previous release
   * (1.11.1). This test is to validate the fix of PARQUET-2027.
   */
  @Test
  public void testBasicBehaviorWithStaticFiles() throws IOException {
    List<Group> expected = new ArrayList<>();
    readAll(STATIC_FILE_1, expected);
    readAll(STATIC_FILE_2, expected);

    Path combinedFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(CONF, FILE_SCHEMA, combinedFile);
    writer.start();
    writer.appendFile(CONF, STATIC_FILE_1);
    writer.appendFile(CONF, STATIC_FILE_2);
    writer.end(EMPTY_METADATA);

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), combinedFile).build()) {

      for (Group expectedNext : expected) {
        Group next = reader.read();
        // check each value; equals is not supported for simple records
        assertThat(next.getInteger("id", 0))
            .as("Each id should match")
            .isEqualTo(expectedNext.getInteger("id", 0));
        assertThat(next.getString("string", 0))
            .as("Each string should match")
            .isEqualTo(expectedNext.getString("string", 0));
      }
      assertThat(reader.read()).as("No extra records should be present").isNull();
    }
  }

  private void readAll(Path file, List<Group> values) throws IOException {
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      for (Group g = reader.read(); g != null; g = reader.read()) {
        values.add(g);
      }
    }
  }

  @Test
  public void testMergedMetadata() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(CONF, FILE_SCHEMA, combinedFile);
    writer.start();
    writer.appendFile(CONF, file1);
    writer.appendFile(CONF, file2);
    writer.end(EMPTY_METADATA);

    ParquetMetadata combinedFooter = ParquetFileReader.readFooter(CONF, combinedFile, NO_FILTER);
    ParquetMetadata f1Footer = ParquetFileReader.readFooter(CONF, file1, NO_FILTER);
    ParquetMetadata f2Footer = ParquetFileReader.readFooter(CONF, file2, NO_FILTER);

    LinkedList<BlockMetaData> expectedRowGroups = new LinkedList<BlockMetaData>();
    expectedRowGroups.addAll(f1Footer.getBlocks());
    expectedRowGroups.addAll(f2Footer.getBlocks());

    assertThat(combinedFooter.getBlocks())
        .as("Combined should have the right number of row groups")
        .hasSameSizeAs(expectedRowGroups);

    long nextStart = 4;
    for (BlockMetaData rowGroup : combinedFooter.getBlocks()) {
      BlockMetaData expected = expectedRowGroups.removeFirst();
      assertThat(rowGroup.getRowCount()).as("Row count should match").isEqualTo(expected.getRowCount());
      assertThat(rowGroup.getCompressedSize())
          .as("Compressed size should match")
          .isEqualTo(expected.getCompressedSize());
      assertThat(rowGroup.getTotalByteSize())
          .as("Total size should match")
          .isEqualTo(expected.getTotalByteSize());
      assertThat(rowGroup.getStartingPos())
          .as("Start pos should be at the last row group's end")
          .isEqualTo(nextStart);
      assertColumnsEquivalent(expected.getColumns(), rowGroup.getColumns());
      nextStart = rowGroup.getStartingPos() + rowGroup.getTotalByteSize();
    }
  }

  public void assertColumnsEquivalent(List<ColumnChunkMetaData> expected, List<ColumnChunkMetaData> actual) {
    assertThat(actual).as("Should have the expected columns").hasSameSizeAs(expected);
    for (int i = 0; i < actual.size(); i += 1) {
      ColumnChunkMetaData current = actual.get(i);
      if (i != 0) {
        ColumnChunkMetaData previous = actual.get(i - 1);
        long expectedStart = previous.getStartingPos() + previous.getTotalSize();
        assertThat(current.getStartingPos())
            .as("Should start after the previous column")
            .isEqualTo(expectedStart);
      }

      assertColumnMetadataEquivalent(expected.get(i), current);
    }
  }

  public void assertColumnMetadataEquivalent(ColumnChunkMetaData expected, ColumnChunkMetaData actual) {
    assertThat(actual.getPath()).as("Should be the expected column").isEqualTo(expected.getPath());
    assertThat(actual.getType()).as("Primitive type should not change").isEqualTo(expected.getType());
    assertThat(actual.getCodec()).as("Compression codec should not change").isEqualTo(expected.getCodec());
    assertThat(actual.getEncodings()).as("Data encodings should not change").isEqualTo(expected.getEncodings());
    assertThat(actual.getStatistics()).as("Statistics should not change").isEqualTo(expected.getStatistics());
    assertThat(actual.getTotalUncompressedSize())
        .as("Uncompressed size should not change")
        .isEqualTo(expected.getTotalUncompressedSize());
    assertThat(actual.getTotalSize())
        .as("Compressed size should not change")
        .isEqualTo(expected.getTotalSize());
    assertThat(actual.getValueCount())
        .as("Number of values should not change")
        .isEqualTo(expected.getValueCount());
  }

  @Test
  public void testAllowDroppingColumns() throws IOException {
    MessageType droppedColumnSchema =
        Types.buildMessage().required(BINARY).as(UTF8).named("string").named("AppendTest");

    Path droppedColumnFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(CONF, droppedColumnSchema, droppedColumnFile);
    writer.start();
    writer.appendFile(CONF, file1);
    writer.appendFile(CONF, file2);
    writer.end(EMPTY_METADATA);

    LinkedList<Group> expected = new LinkedList<Group>();
    expected.addAll(file1content);
    expected.addAll(file2content);

    ParquetMetadata footer = ParquetFileReader.readFooter(CONF, droppedColumnFile, NO_FILTER);
    for (BlockMetaData rowGroup : footer.getBlocks()) {
      assertThat(rowGroup.getColumns())
          .as("Should have only the string column")
          .hasSize(1);
    }

    ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), droppedColumnFile).build();

    Group next;
    while ((next = reader.read()) != null) {
      Group expectedNext = expected.removeFirst();
      assertThat(next.getString("string", 0))
          .as("Each string should match")
          .isEqualTo(expectedNext.getString("string", 0));
    }

    assertThat(expected).as("All records should be present").isEmpty();
  }

  @Test
  public void testFailDroppingColumns() throws IOException {
    MessageType droppedColumnSchema =
        Types.buildMessage().required(BINARY).as(UTF8).named("string").named("AppendTest");

    final ParquetMetadata footer = ParquetFileReader.readFooter(CONF, file1, NO_FILTER);
    final FSDataInputStream incoming = file1.getFileSystem(CONF).open(file1);

    Path droppedColumnFile = newTemp();
    final ParquetFileWriter writer = new ParquetFileWriter(CONF, droppedColumnSchema, droppedColumnFile);
    writer.start();

    assertThatThrownBy(() -> writer.appendRowGroups(incoming, footer.getBlocks(), false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Columns cannot be copied (missing from target schema): id");
  }

  @Test
  public void testFailMissingColumn() throws IOException {
    MessageType fileSchema = Types.buildMessage()
        .required(INT32)
        .named("id")
        .required(BINARY)
        .as(UTF8)
        .named("string")
        .required(FLOAT)
        .named("value")
        .named("AppendTest");

    Path missingColumnFile = newTemp();
    final ParquetFileWriter writer = new ParquetFileWriter(CONF, fileSchema, missingColumnFile);
    writer.start();

    assertThatThrownBy(() -> writer.appendFile(CONF, file1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing column 'value'");
  }

  private Path newTemp() {
    return new Path(tempDir.resolve(java.util.UUID.randomUUID() + ".tmp").toUri());
  }
}
