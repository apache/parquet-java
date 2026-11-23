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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.UnaryOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.column.ContentDefinedChunkingOptions;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Content defined chunking through the file writers, which the chunking tests of parquet-column, {@code
 * org.apache.parquet.column.impl.TestContentDefinedChunker}, write around.
 */
public class TestContentDefinedChunking {

  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message m { optional binary f0 (STRING); }");
  private static final MessageType LIST_SCHEMA = MessageTypeParser.parseMessageType(
      "message m { optional group f0 (LIST) { repeated group list { optional int64 element; } } }");
  private static final ContentDefinedChunkingOptions OPTIONS =
      new ContentDefinedChunkingOptions(16 * 1024, 64 * 1024, 1);

  @TempDir
  private Path tempDir;

  /**
   * The file writer makes the chunkers once per file, so splitting the rows into row groups moves no
   * page boundary but adds one at the end of each row group.
   */
  @Test
  public void chunkingContinuesAcrossRowGroups() throws IOException {
    List<Group> rows = rows(40_000);
    List<Integer> chunkEnds = pageEnds(write(SCHEMA, rows, "single", UnaryOperator.identity()));
    List<Integer> rowGroupEnds = List.of(10_000, 20_000, 30_000);
    List<Integer> ends =
        pageEnds(write(SCHEMA, rows, "multi", builder -> builder.withRowGroupRowCountLimit(10_000)));

    assertThat(chunkEnds).as("pages of the single row group").hasSizeGreaterThan(1);
    assertThat(ends).as("chunk ends").containsAll(chunkEnds);
    assertThat(ends).as("row group ends").containsAll(rowGroupEnds);
    assertThat(ends).as("other page ends").allMatch(end -> chunkEnds.contains(end) || rowGroupEnds.contains(end));
  }

  /**
   * The page size check schedule of the column write store, which also decides when the held back nulls
   * of null lists reach the column writers, adds no page end: checking after every record ends the same
   * pages. A page size limit only adds page ends, keeping every chunk end.
   */
  @Test
  public void pageSizeCheckScheduleMovesNoPageBoundary() throws IOException {
    List<Group> lists = lists(40_000);
    List<Integer> chunks = pageEnds(write(LIST_SCHEMA, lists, "chunks", UnaryOperator.identity()));
    List<Integer> everyRecord = pageEnds(write(
        LIST_SCHEMA,
        lists,
        "every-record",
        builder -> builder.withMinRowCountForPageSizeCheck(1).withMaxRowCountForPageSizeCheck(1)));
    List<Integer> limited =
        pageEnds(write(LIST_SCHEMA, lists, "limited", builder -> builder.withPageSize(4 * 1024)));

    assertThat(chunks).as("chunked pages").hasSizeGreaterThan(1);
    assertThat(everyRecord).containsExactlyElementsOf(chunks);
    assertThat(limited).containsAll(chunks).hasSizeGreaterThan(2 * chunks.size());
  }

  /** Chunk sizes such as "8m", which the keys do not accept, or a minimum above the maximum. */
  @Test
  public void invalidOptionsFailOnlyWhileEnabled() {
    Configuration conf = outputFormatConf();
    conf.set(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, "8m");

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, false);
    assertThatCode(() -> writeThroughOutputFormat(conf, List.of(), "off")).doesNotThrowAnyException();

    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, 8 * 1024 * 1024);
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    assertThatThrownBy(() -> writeThroughOutputFormat(conf, List.of(), "on"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking size range: maximum chunk size (1048576) must be greater "
            + "than minimum chunk size (8388608)");
    assertThat(tempDir.resolve("on.parquet")).doesNotExist();
  }

  @Test
  public void configurationKeysReachTheWriter() throws Exception {
    List<Group> rows = rows(40_000);
    Configuration conf = outputFormatConf();
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, OPTIONS.getMinChunkSize());
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MAX_SIZE, OPTIONS.getMaxChunkSize());
    conf.setInt(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_NORM_LEVEL, OPTIONS.getNormLevel());

    assertThat(pageEnds(writeThroughOutputFormat(conf, rows, "configured")))
        .containsExactlyElementsOf(pageEnds(write(SCHEMA, rows, "written", UnaryOperator.identity())))
        .hasSizeGreaterThan(1);

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, false);
    assertThat(pageEnds(writeThroughOutputFormat(conf, rows, "unconfigured")))
        .containsExactly(rows.size());
  }

  private static List<Group> rows(int count) {
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    Random random = new Random(0);
    List<Group> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      rows.add(factory.newGroup().append("f0", "str_" + Long.toUnsignedString(random.nextLong())));
    }
    return rows;
  }

  /** Up to three elements a row, about one row in eleven a null list. */
  private static List<Group> lists(int count) {
    SimpleGroupFactory factory = new SimpleGroupFactory(LIST_SCHEMA);
    Random random = new Random(0);
    List<Group> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      long value = random.nextLong();
      Group row = factory.newGroup();
      if (Long.remainderUnsigned(value, 11) != 0) {
        Group list = row.addGroup("f0");
        for (int k = 0; k < Long.remainderUnsigned(value, 4); k++) {
          list.addGroup("list").append("element", value >>> (16 * k));
        }
      }
      rows.add(row);
    }
    return rows;
  }

  /** Writes the rows with {@link #OPTIONS}, without dictionary encoding or page limits unless set. */
  private Path write(
      MessageType schema, List<Group> rows, String label, UnaryOperator<ExampleParquetWriter.Builder> settings)
      throws IOException {
    Path file = tempDir.resolve(label + ".parquet");
    try (ParquetWriter<Group> writer = settings.apply(ExampleParquetWriter.builder(new LocalOutputFile(file))
            .withType(schema)
            .withContentDefinedChunking(OPTIONS)
            .withDictionaryEncoding(false)
            .withPageSize(Integer.MAX_VALUE)
            .withPageRowCountLimit(Integer.MAX_VALUE))
        .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
    return file;
  }

  /** The configuration of a {@link ParquetOutputFormat} without dictionary encoding or page limits. */
  private static Configuration outputFormatConf() {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    conf.setInt(ParquetOutputFormat.PAGE_ROW_COUNT_LIMIT, Integer.MAX_VALUE);
    conf.setInt(ParquetOutputFormat.PAGE_SIZE, Integer.MAX_VALUE);
    conf.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, false);
    return conf;
  }

  private Path writeThroughOutputFormat(Configuration conf, List<Group> rows, String label) throws Exception {
    Path file = tempDir.resolve(label + ".parquet");
    RecordWriter<Void, Group> writer = new ExampleOutputFormat()
        .getRecordWriter(conf, new org.apache.hadoop.fs.Path(file.toUri()), CompressionCodecName.UNCOMPRESSED);
    try {
      for (Group row : rows) {
        writer.write(null, row);
      }
    } finally {
      writer.close(null);
    }
    return file;
  }

  /** Where the pages of the file's first column end, counted in values across its row groups. */
  private static List<Integer> pageEnds(Path file) throws IOException {
    List<Integer> ends = new ArrayList<>();
    int end = 0;
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
      for (PageReadStore rowGroup = reader.readNextRowGroup();
          rowGroup != null;
          rowGroup = reader.readNextRowGroup()) {
        PageReader pages = rowGroup.getPageReader(
            reader.getFileMetaData().getSchema().getColumns().get(0));
        for (DataPage page = pages.readPage(); page != null; page = pages.readPage()) {
          end += page.getValueCount();
          ends.add(end);
        }
      }
    }
    return ends;
  }
}
