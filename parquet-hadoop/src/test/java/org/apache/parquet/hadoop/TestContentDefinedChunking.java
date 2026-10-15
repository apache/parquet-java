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

import static org.apache.parquet.column.impl.ChunkingTestUtil.OPTIONAL_LIST_OF_INT64_PAGES;
import static org.apache.parquet.column.impl.ChunkingTestUtil.lcg;
import static org.apache.parquet.column.impl.ChunkingTestUtil.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestContentDefinedChunking {

  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message t { required int64 id; required binary name (STRING); }");
  private static final CdcOptions OPTIONS = options(16 * 1024, 64 * 1024, 0);
  private static final CdcOptions SMALL_OPTIONS = options(4 * 1024, 16 * 1024, 0);
  private static final int ROWS = 40_000;

  @TempDir
  private java.nio.file.Path tempDir;

  @ParameterizedTest
  @CsvSource({"PARQUET_1_0, true", "PARQUET_1_0, false", "PARQUET_2_0, true", "PARQUET_2_0, false"})
  public void chunkingLosesNoData(WriterVersion version, boolean dictionary) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message t {"
        + " required int32 i32; optional int64 i64; optional float f; required double d; optional boolean b;"
        + " optional binary s (STRING); optional fixed_len_byte_array(8) x; optional int96 t;"
        + " optional group l (LIST) { repeated group list { optional int32 element; } }"
        + " optional group m (MAP) { repeated group key_value { required binary key (STRING); optional int64 value; } }"
        + " optional int64 sparse; }");
    List<Group> rows = randomRows(schema);
    List<Path> files = new ArrayList<>();
    for (boolean chunking : new boolean[] {false, true}) {
      Path file = path("data-" + chunking);
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
          .withType(schema)
          .withWriterVersion(version)
          .withDictionaryEncoding(dictionary)
          .withRowGroupRowCountLimit(7_000)
          .withPageSize(2 * 1024)
          .withPageRowCountLimit(2_000)
          .withDictionaryPageSize(16 * 1024)
          .withContentDefinedChunking(chunking ? SMALL_OPTIONS : null)
          .build()) {
        for (Group row : rows) {
          writer.write(row);
        }
      }
      files.add(file);
    }
    assertThat(read(files.get(1)))
        .containsExactlyElementsOf(rows.stream().map(Group::toString).collect(Collectors.toList()));
    assertThat(pagesByRowGroup(files.get(1), 0, Function.identity()).get(0))
        .first()
        .isInstanceOf(version == WriterVersion.PARQUET_2_0 ? DataPageV2.class : DataPageV1.class);

    assertThat(pageCounts(files.get(1), 1)).isNotEqualTo(pageCounts(files.get(0), 1));
    assertThat(pageCountsByRowGroup(files.get(1), 1)).hasSameSizeAs(pageCountsByRowGroup(files.get(0), 1));
  }

  /**
   * Row groups add page boundaries but move no chunk boundary: the first layout ends a row group a
   * row before a chunk boundary, the second has row groups shorter than the minimum chunk size.
   */
  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  public void chunkingContinuesAcrossRowGroups(int column) throws IOException {
    List<List<Integer>> whole = pageCountsByRowGroup(write("one-group", UnaryOperator.identity()), column);
    assertThat(whole).hasSize(1);
    assertThat(whole.get(0)).hasSizeGreaterThan(4);

    for (int perGroup : new int[] {whole.get(0).get(0) - 1, 100}) {
      List<List<Integer>> groups = pageCountsByRowGroup(
          write("groups-" + perGroup, b -> b.withRowGroupRowCountLimit(perGroup)), column);
      Set<Long> expected = new TreeSet<>(pageEnds(whole));
      for (long end = perGroup; end < ROWS; end += perGroup) {
        expected.add(end);
      }

      assertThat(groups).hasSizeGreaterThan(4);
      assertThat(pageEnds(groups)).as("row groups of %s rows", perGroup).containsExactlyElementsOf(expected);
    }
  }

  @Test
  public void eachColumnChunksOnItsOwn() throws IOException {
    Path both = write("both", UnaryOperator.identity());
    for (int column = 0; column < SCHEMA.getFieldCount(); column++) {
      MessageType alone = new MessageType("t", SCHEMA.getType(column));
      List<Integer> counts = pageCounts(
          write("alone-" + column, alone, IntStream.range(0, ROWS).toArray(), b -> b), 0);

      assertThat(counts).hasSizeGreaterThan(4);
      assertThat(pageCounts(both, column)).containsExactlyElementsOf(counts);
    }
  }

  /**
   * An insertion near the start of one of several row groups, as Arrow C++ tests it: the row groups
   * before it keep every page, and each later one shares all its pages with the original but the
   * first and last, which its shifted boundaries cut.
   */
  @Test
  public void insertionLeavesOtherRowGroupsDeduplicating() throws IOException {
    int perGroup = ROWS / 7;
    int at = 2 * perGroup + perGroup / 8;
    int[] original = IntStream.range(0, ROWS).toArray();
    int[] edited = IntStream.concat(
            IntStream.concat(IntStream.range(0, at), IntStream.range(ROWS, ROWS + 100)),
            IntStream.range(at, ROWS))
        .toArray();
    UnaryOperator<ExampleParquetWriter.Builder> configure =
        b -> b.withRowGroupRowCountLimit(perGroup).withContentDefinedChunking(SMALL_OPTIONS);
    List<List<Binary>> before = pageBytesByRowGroup(write("before", SCHEMA, original, configure), 1);
    List<List<Binary>> after = pageBytesByRowGroup(write("after", SCHEMA, edited, configure), 1);
    Set<Binary> originalPages = before.stream().flatMap(List::stream).collect(Collectors.toSet());

    assertThat(after.subList(0, 2)).containsExactlyElementsOf(before.subList(0, 2));
    for (List<Binary> group : after.subList(3, after.size() - 1)) {
      assertThat(group.subList(1, group.size() - 1)).hasSizeGreaterThan(3).isSubsetOf(originalPages);
    }
  }

  /** The list vector of TestContentDefinedChunker, through a writer that holds back group nulls. */
  @Test
  public void listWrittenByRecordsMatchesArrowCpp() throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message t { optional group v (LIST) { repeated group list { optional int64 element; } } }");
    Path file = path("list");
    try (ParquetWriter<Group> writer = builder(file, schema)
        .withContentDefinedChunking(options(64 * 1024, 256 * 1024, 0))
        .build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(schema);
      for (long x : lcg(200_000)) {
        Group row = f.newGroup();
        if (Long.remainderUnsigned(x, 11) != 0) {
          Group list = row.addGroup("v");
          for (int k = 0; k < Long.remainderUnsigned(x, 4); k++) {
            list.addGroup("list").append("element", x >>> (16 * k));
          }
        }
        writer.write(row);
      }
    }

    assertThat(pageCounts(file, 0)).containsExactlyElementsOf(OPTIONAL_LIST_OF_INT64_PAGES);
  }

  /** Column chunks of nulls alone keep the dictionary, with an empty dictionary page, as in Arrow C++. */
  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void nullsReadBackWithDictionary(WriterVersion version) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message t { required int64 id; optional int32 late;"
        + " optional binary b; optional int32 i; optional int64 l; optional float f; optional double d;"
        + " optional fixed_len_byte_array(4) x; }");
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    List<Group> rows = new ArrayList<>();
    for (int i = 0; i < ROWS; i++) {
      Group row = f.newGroup().append("id", (long) i);
      if (i >= ROWS * 5 / 8) {
        row.append("late", i % 100);
      }
      rows.add(row);
    }
    Path file = path("nulls");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withType(schema)
        .withWriterVersion(version)
        .withRowGroupRowCountLimit(ROWS / 4)
        .withContentDefinedChunking(SMALL_OPTIONS)
        .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }

    assertThat(read(file))
        .containsExactlyElementsOf(rows.stream().map(Group::toString).collect(Collectors.toList()));
    assertThat(encodingsOf(file, 2))
        .anySatisfy(encoding -> assertThat(encoding.usesDictionary()).isTrue());
  }

  @Test
  public void dictionaryFallsBackOnSizeLimitAlone() throws IOException {
    UnaryOperator<ExampleParquetWriter.Builder> dictionary = b -> b.withDictionaryEncoding(true)
        .withDictionaryPageSize(128 * 1024)
        .withContentDefinedChunking(SMALL_OPTIONS);
    int[] repeating = IntStream.range(0, ROWS).map(i -> i % 5_000).toArray();

    assertThat(encodingsOf(write("repeating", SCHEMA, repeating, dictionary), 1))
        .doesNotContain(Encoding.PLAIN);
    assertThat(encodingsOf(write("unique", dictionary), 1)).contains(Encoding.PLAIN);
  }

  @Test
  public void emptyFileReadsBack() throws IOException {
    Path file = write("empty", SCHEMA, new int[0], UnaryOperator.identity());

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      assertThat(reader.read()).isNull();
    }
  }

  @Test
  public void invalidOptionsFailOnlyWhileEnabled() {
    Configuration conf = outputFormatConf();
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, 8 * 1024 * 1024);

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, false);
    assertThatCode(() -> new ParquetOutputFormat<Group>()
            .getRecordWriter(conf, path("off"), CompressionCodecName.UNCOMPRESSED)
            .close(null))
        .doesNotThrowAnyException();

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    assertThatThrownBy(() -> new ParquetOutputFormat<Group>()
            .getRecordWriter(conf, path("on"), CompressionCodecName.UNCOMPRESSED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking size range: maximum chunk size (1048576) must be greater "
            + "than minimum chunk size (8388608)");
  }

  @Test
  public void configurationKeysReachTheWriter() throws Exception {
    Configuration conf = outputFormatConf();
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, 16 * 1024);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MAX_SIZE, 64 * 1024);
    conf.setInt(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_NORM_LEVEL, 1);
    CdcOptions configured = CdcOptions.builder()
        .withMinChunkSize(16 * 1024)
        .withMaxChunkSize(64 * 1024)
        .withNormLevel(1)
        .build();

    assertThat(pageCounts(writeThroughOutputFormat(conf, "configured"), 1))
        .hasSizeGreaterThan(1)
        .containsExactlyElementsOf(
            pageCounts(write("direct", b -> b.withContentDefinedChunking(configured)), 1));

    conf.unset(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED);
    assertThat(pageCounts(writeThroughOutputFormat(conf, "unconfigured"), 1))
        .hasSize(1);
  }

  private static String name(int i) {
    return "row-" + (i * 2654435761L % 1_000_000L);
  }

  private Path path(String label) {
    return new Path(tempDir.resolve(label + ".parquet").toUri());
  }

  /** A writer with chunking on and the position-based page limits lifted, so every page boundary is the chunker's. */
  private static ExampleParquetWriter.Builder builder(Path file, MessageType schema) {
    return ExampleParquetWriter.builder(file)
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withDictionaryEncoding(false)
        .withRowGroupSize(1L << 30)
        .withPageSize(1 << 30)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withContentDefinedChunking(OPTIONS);
  }

  private Path write(String label, UnaryOperator<ExampleParquetWriter.Builder> configure) throws IOException {
    return write(label, SCHEMA, IntStream.range(0, ROWS).toArray(), configure);
  }

  /** A row of {@code id} and {@code name(id)} for every id, as far as {@code schema} has those fields. */
  private Path write(
      String label, MessageType schema, int[] ids, UnaryOperator<ExampleParquetWriter.Builder> configure)
      throws IOException {
    Path file = path(label);
    try (ParquetWriter<Group> writer =
        configure.apply(builder(file, schema)).build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(schema);
      for (int id : ids) {
        Group row = f.newGroup();
        if (schema.containsField("id")) {
          row.append("id", (long) id);
        }
        if (schema.containsField("name")) {
          row.append("name", name(id));
        }
        writer.write(row);
      }
    }
    return file;
  }

  private Configuration outputFormatConf() {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    conf.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, GroupWriteSupport.class.getName());
    conf.setInt(ParquetOutputFormat.PAGE_ROW_COUNT_LIMIT, Integer.MAX_VALUE);
    conf.setInt(ParquetOutputFormat.PAGE_SIZE, 1 << 30);
    conf.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, false);
    return conf;
  }

  private Path writeThroughOutputFormat(Configuration conf, String label) throws Exception {
    Path file = path(label);
    RecordWriter<Void, Group> writer =
        new ParquetOutputFormat<Group>().getRecordWriter(conf, file, CompressionCodecName.UNCOMPRESSED);
    try {
      SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
      for (int i = 0; i < ROWS; i++) {
        writer.write(null, f.newGroup().append("id", (long) i).append("name", name(i)));
      }
    } finally {
      writer.close(null);
    }
    return file;
  }

  private static List<String> read(Path file) throws IOException {
    List<String> rows = new ArrayList<>();
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      for (Group row = reader.read(); row != null; row = reader.read()) {
        rows.add(row.toString());
      }
    }
    return rows;
  }

  /** The encodings of a column in the first row group. */
  private static Set<Encoding> encodingsOf(Path file, int column) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()))) {
      return reader.getFooter()
          .getBlocks()
          .get(0)
          .getColumns()
          .get(column)
          .getEncodings();
    }
  }

  /** Every type, nulls, lists and maps, and a column null in the first two row groups of 7000 rows. */
  private static List<Group> randomRows(MessageType schema) {
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    Random random = new Random(31);
    List<Group> rows = new ArrayList<>();
    for (int i = 0; i < 30_000; i++) {
      Group row = f.newGroup().append("i32", random.nextInt(1000)).append("d", random.nextDouble());
      if (random.nextInt(4) > 0) {
        row.append("i64", random.nextLong());
      }
      if (random.nextInt(8) > 0) {
        row.append("f", random.nextFloat());
      }
      if (random.nextInt(3) > 0) {
        row.append("b", random.nextBoolean());
      }
      if (random.nextInt(5) > 0) {
        row.append("s", "v" + random.nextInt(random.nextBoolean() ? 300 : Integer.MAX_VALUE));
      }
      if (random.nextInt(5) > 0) {
        row.append("x", String.format("%08x", random.nextInt()));
      }
      if (random.nextInt(5) > 0) {
        row.append("t", new NanoTime(random.nextInt(3_000_000), random.nextLong()));
      }
      if (random.nextInt(6) > 0) {
        Group list = row.addGroup("l");
        for (int n = random.nextInt(4); n > 0; n--) {
          Group element = list.addGroup("list");
          if (random.nextInt(5) > 0) {
            element.append("element", random.nextInt());
          }
        }
      }
      if (random.nextInt(6) > 0) {
        Group map = row.addGroup("m");
        for (int n = random.nextInt(3); n > 0; n--) {
          Group entry = map.addGroup("key_value").append("key", "k" + n);
          if (random.nextInt(4) > 0) {
            entry.append("value", random.nextLong());
          }
        }
      }
      if (i >= 14_000 && i % 7_000 >= 5_000) {
        row.append("sparse", random.nextLong());
      }
      rows.add(row);
    }
    return rows;
  }

  /** Where each page ends, counted in values from the start of the file. */
  private static List<Long> pageEnds(List<List<Integer>> groups) {
    List<Long> ends = new ArrayList<>();
    long end = 0;
    for (List<Integer> group : groups) {
      for (int count : group) {
        end += count;
        ends.add(end);
      }
    }
    return ends;
  }

  private static <T> List<List<T>> pagesByRowGroup(Path file, int column, Function<DataPage, T> extract)
      throws IOException {
    List<List<T>> groups = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()))) {
      ColumnDescriptor path =
          reader.getFileMetaData().getSchema().getColumns().get(column);
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        List<T> pages = new ArrayList<>();
        PageReader pageReader = rowGroup.getPageReader(path);
        for (long read = 0; read < pageReader.getTotalValueCount(); ) {
          DataPage page = pageReader.readPage();
          pages.add(extract.apply(page));
          read += page.getValueCount();
        }
        groups.add(pages);
      }
    }
    return groups;
  }

  private static List<List<Integer>> pageCountsByRowGroup(Path file, int column) throws IOException {
    return pagesByRowGroup(file, column, DataPage::getValueCount);
  }

  private static List<List<Binary>> pageBytesByRowGroup(Path file, int column) throws IOException {
    return pagesByRowGroup(file, column, page -> {
      try {
        return Binary.fromConstantByteArray(
            ((DataPageV1) page).getBytes().toByteArray());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  private static List<Integer> pageCounts(Path file, int column) throws IOException {
    return pageCountsByRowGroup(file, column).stream().flatMap(List::stream).collect(Collectors.toList());
  }
}
