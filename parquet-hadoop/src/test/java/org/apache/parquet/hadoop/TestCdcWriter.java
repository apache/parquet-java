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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Content defined chunking through {@link ParquetWriter.Builder} and {@link ParquetOutputFormat}, into
 * a real file that is read back.
 */
public class TestCdcWriter {

  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message t { required int64 id; required binary name (STRING); }");

  private static final CdcOptions OPTIONS = CdcOptions.builder()
      .withMinChunkSize(16 * 1024)
      .withMaxChunkSize(64 * 1024)
      .build();

  private static final CdcOptions SMALL_OPTIONS = CdcOptions.builder()
      .withMinChunkSize(4 * 1024)
      .withMaxChunkSize(16 * 1024)
      .build();

  private static final int ROWS = 40_000;

  @TempDir
  private java.nio.file.Path tempDir;

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void roundTripsEveryValue(WriterVersion version) throws IOException {
    Path file = write("roundtrip-" + version, b -> b.withWriterVersion(version));
    assertThat(pageCounts(file)).hasSizeGreaterThan(1);
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()))) {
      DataPage page = reader.readNextRowGroup()
          .getPageReader(
              reader.getFileMetaData().getSchema().getColumns().get(1))
          .readPage();
      assertThat(page)
          .as("the writer version reaches the pages")
          .isInstanceOf(version == WriterVersion.PARQUET_2_0 ? DataPageV2.class : DataPageV1.class);
    }

    List<String> read = new ArrayList<>();
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(new Configuration())
        .build()) {
      Group g;
      while ((g = reader.read()) != null) {
        read.add(g.getLong("id", 0) + "|" + g.getString("name", 0));
      }
    }

    List<String> expected = new ArrayList<>(ROWS);
    for (int i = 0; i < ROWS; i++) {
      expected.add(i + "|" + name(i));
    }
    assertThat(read).containsExactlyElementsOf(expected);
  }

  /**
   * Chunking only moves page boundaries: every physical type, null, list and map reads back as it
   * does without, across row groups, page limits, dictionary fallback and column chunks of nulls.
   */
  @ParameterizedTest
  @CsvSource({"PARQUET_1_0, true", "PARQUET_1_0, false", "PARQUET_2_0, true", "PARQUET_2_0, false"})
  public void chunkingLosesNoData(WriterVersion version, boolean dictionary) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message t {"
        + " required int32 i32; optional int64 i64; optional float f; required double d; optional boolean b;"
        + " optional binary s (STRING); optional fixed_len_byte_array(8) x; optional int96 t;"
        + " optional group l (LIST) { repeated group list { optional int32 element; } }"
        + " optional group m (MAP) { repeated group key_value { required binary key (STRING); optional int64 value; } }"
        + " optional int64 sparse;"
        + " }");
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
      // Null in the first two row groups, and in the first pages of every later one.
      if (i >= 14_000 && i % 7_000 >= 5_000) {
        row.append("sparse", random.nextLong());
      }
      rows.add(row);
    }
    List<String> expected = rows.stream().map(Group::toString).collect(Collectors.toList());

    List<Path> files = new ArrayList<>();
    for (boolean chunking : new boolean[] {false, true}) {
      Path file = new Path(tempDir.resolve("data-" + version + "-" + dictionary + "-" + chunking + ".parquet")
          .toUri());
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
          .withType(schema)
          .withWriterVersion(version)
          .withDictionaryEncoding(dictionary)
          .withRowGroupRowCountLimit(7_000)
          .withPageSize(2 * 1024)
          .withPageRowCountLimit(2_000)
          .withDictionaryPageSize(16 * 1024)
          .withContentDefinedChunking(SMALL_OPTIONS)
          .withContentDefinedChunkingEnabled(chunking)
          .build()) {
        for (Group row : rows) {
          writer.write(row);
        }
      }
      List<String> read = new ArrayList<>();
      try (ParquetReader<Group> reader =
          ParquetReader.builder(new GroupReadSupport(), file).build()) {
        for (Group row = reader.read(); row != null; row = reader.read()) {
          read.add(row.toString());
        }
      }
      assertThat(read).as(chunking ? "chunked" : "unchunked").containsExactlyElementsOf(expected);
      files.add(file);
    }
    assertThat(pageCounts(files.get(1)))
        .as("chunking moves the page boundaries")
        .isNotEqualTo(pageCounts(files.get(0)));
  }

  /**
   * The chunker follows each column across row groups, as arrow-rs's does, so a row group boundary
   * adds a page break but moves no chunk boundary. After an edit the chunks realign once, rather than
   * again at the start of every later row group, whose position the edit has shifted. The first row
   * group ends a row before a chunk boundary, which a chunker restarting its hash, match run or size
   * there misses; a match pending across records needs a nested column, as TestCdcWrite has.
   */
  @Test
  public void chunkingContinuesAcrossRowGroups() throws IOException {
    List<List<Integer>> whole = pageCountsByRowGroup(write("one-group", UnaryOperator.identity()));
    assertThat(whole).hasSize(1);
    assertThat(whole.get(0)).hasSizeGreaterThan(4);
    int perGroup = whole.get(0).get(0) - 1;
    List<List<Integer>> groups = pageCountsByRowGroup(write("groups", b -> b.withRowGroupRowCountLimit(perGroup)));
    assertThat(groups).hasSizeGreaterThan(4);

    Set<Long> expected = new TreeSet<>(pageEnds(whole));
    for (long end = perGroup; end < ROWS; end += perGroup) {
      expected.add(end);
    }
    assertThat(pageEnds(groups)).containsExactlyElementsOf(expected);
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

  /** Through {@link ParquetOutputFormat}, the only reader of the configuration keys. */
  @Test
  public void anInvalidEnvelopeIsIgnoredWhileChunkingIsOff() throws Exception {
    Configuration conf = chunkingConf();
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, 8 * 1024 * 1024); // > the default max
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, false);

    Path off = new Path(tempDir.resolve("stale-config.parquet").toUri());
    assertThatCode(() -> new ParquetOutputFormat<Group>()
            .getRecordWriter(conf, off, CompressionCodecName.UNCOMPRESSED)
            .close(null))
        .as("a stale key must not fail a job that has the feature switched off")
        .doesNotThrowAnyException();

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    Path on = new Path(tempDir.resolve("stale-config-on.parquet").toUri());
    assertThatThrownBy(() ->
            new ParquetOutputFormat<Group>().getRecordWriter(conf, on, CompressionCodecName.UNCOMPRESSED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking size range: maximum chunk size (1048576) must be greater "
            + "than minimum chunk size (8388608)");
  }

  /**
   * Column chunks of nulls alone, of every dictionary encoded type, keep the dictionary with an empty
   * dictionary page, as in Arrow C++, and so does one that starts with pages of them. The file reads
   * back whatever the writer version and codec.
   */
  @ParameterizedTest
  @MethodSource("versionsAndCodecs")
  public void nullsReadBackWithTheDictionaryOn(WriterVersion version, CompressionCodecName codec) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message t { required int64 id; optional int32 late; optional binary b; optional int32 i;"
            + " optional int64 l; optional float f; optional double d; optional fixed_len_byte_array(4) x; }");
    Path file = new Path(
        tempDir.resolve("nulls-" + version + "-" + codec + ".parquet").toUri());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withType(schema)
        .withWriterVersion(version)
        .withCompressionCodec(codec)
        .withRowGroupRowCountLimit(ROWS / 4)
        .withContentDefinedChunking(SMALL_OPTIONS)
        .build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(schema);
      for (int i = 0; i < ROWS; i++) {
        Group row = f.newGroup().append("id", (long) i);
        if (i >= ROWS * 5 / 8) {
          row.append("late", i % 100);
        }
        writer.write(row);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      for (int i = 0; i < ROWS; i++) {
        Group row = reader.read();
        assertThat(row.getLong("id", 0)).isEqualTo(i);
        for (String empty : new String[] {"b", "i", "l", "f", "d", "x"}) {
          assertThat(row.getFieldRepetitionCount(empty)).as(empty).isZero();
        }
        if (i >= ROWS * 5 / 8) {
          assertThat(row.getInteger("late", 0)).isEqualTo(i % 100);
        } else {
          assertThat(row.getFieldRepetitionCount("late")).isZero();
        }
      }
      assertThat(reader.read()).isNull();
    }
  }

  static Stream<Arguments> versionsAndCodecs() {
    return Stream.of(WriterVersion.values()).flatMap(version -> Stream.of(
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP,
            CompressionCodecName.ZSTD,
            CompressionCodecName.LZ4_RAW)
        .map(codec -> Arguments.of(version, codec)));
  }

  @Test
  public void chunkingCanBeSwitchedOffAfterItsOptionsAreSet() throws IOException {
    // With the position-based limits lifted, an unchunked column chunk is one page.
    assertThat(pageCounts(write("switched-off", b -> b.withContentDefinedChunkingEnabled(false))))
        .hasSize(1);
  }

  @Test
  public void theConfigurationKeysActuallyChunk() throws Exception {
    Configuration conf = chunkingConf();
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, 16 * 1024);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MAX_SIZE, 64 * 1024);
    conf.setInt(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_NORM_LEVEL, 1);
    CdcOptions configured = CdcOptions.builder()
        .withMinChunkSize(16 * 1024)
        .withMaxChunkSize(64 * 1024)
        .withNormLevel(1)
        .build();
    List<Integer> configuredPages = pageCounts(writeThroughOutputFormat(conf, "configured"));
    assertThat(configuredPages).hasSizeGreaterThan(1);
    assertThat(configuredPages)
        .as("every key reaches the writer")
        .containsExactlyElementsOf(pageCounts(write("direct", b -> b.withContentDefinedChunking(configured))));

    conf.unset(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED);
    assertThat(pageCounts(writeThroughOutputFormat(conf, "unconfigured")))
        .as("and do nothing while the enabled key is unset")
        .hasSize(1);
  }

  private Configuration chunkingConf() {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    conf.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, GroupWriteSupport.class.getName());
    conf.setInt(ParquetOutputFormat.PAGE_ROW_COUNT_LIMIT, Integer.MAX_VALUE);
    conf.setInt(ParquetOutputFormat.PAGE_SIZE, 1 << 30);
    conf.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, false);
    return conf;
  }

  private Path writeThroughOutputFormat(Configuration conf, String label) throws Exception {
    Path file = new Path(tempDir.resolve(label + ".parquet").toUri());
    RecordWriter<Void, Group> writer =
        new ParquetOutputFormat<Group>().getRecordWriter(conf, file, CompressionCodecName.UNCOMPRESSED);
    SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
    for (int i = 0; i < ROWS; i++) {
      writer.write(null, f.newGroup().append("id", (long) i).append("name", name(i)));
    }
    writer.close(null);
    return file;
  }

  /**
   * A chunked first page is cut by content, not size, so it is no sample to judge a dictionary on:
   * as in Arrow C++, the dictionary falls back only when it outgrows its size limit.
   */
  @Test
  public void theDictionaryFallsBackOnItsSizeLimitAlone() throws IOException {
    assertThat(encodingsOf(writeHighCardinality("repeating", 12_000)))
        .as("a dictionary within its limit is kept")
        .contains(Encoding.PLAIN_DICTIONARY)
        .doesNotContain(Encoding.PLAIN);
    assertThat(encodingsOf(writeHighCardinality("unique", Integer.MAX_VALUE)))
        .as("one that outgrows it is dropped")
        .contains(Encoding.PLAIN);
  }

  private static Set<Encoding> encodingsOf(Path file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()))) {
      return reader.getFooter().getBlocks().get(0).getColumns().get(1).getEncodings();
    }
  }

  /** 60 000 chunked rows of ~45-byte values drawn from {@code distinct} different ones. */
  private Path writeHighCardinality(String label, int distinct) throws IOException {
    Path file = new Path(tempDir.resolve(label + ".parquet").toUri());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withType(SCHEMA)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withRowGroupSize(1L << 30)
        .withContentDefinedChunking(SMALL_OPTIONS)
        .build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
      Random random = new Random(7);
      String pad = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
      for (int i = 0; i < 60_000; i++) {
        writer.write(f.newGroup().append("id", (long) i).append("name", "v" + random.nextInt(distinct) + pad));
      }
    }
    return file;
  }

  private static String name(int i) {
    return "row-" + (i * 2654435761L % 1_000_000L);
  }

  /**
   * Writes {@code ROWS} rows with chunking on and the position-based page limits lifted, so every
   * page boundary is the chunker's.
   */
  private Path write(String label, UnaryOperator<ExampleParquetWriter.Builder> configure) throws IOException {
    Path file = new Path(tempDir.resolve(label + ".parquet").toUri());
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(file)
        .withType(SCHEMA)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withDictionaryEncoding(false)
        .withRowGroupSize(1L << 30)
        .withPageSize(1 << 30)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withContentDefinedChunking(OPTIONS);
    try (ParquetWriter<Group> writer = configure.apply(builder).build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
      for (int i = 0; i < ROWS; i++) {
        writer.write(f.newGroup().append("id", (long) i).append("name", name(i)));
      }
    }
    return file;
  }

  /** The page value counts of each row group, kept apart rather than concatenated. */
  private static List<List<Integer>> pageCountsByRowGroup(Path file) throws IOException {
    List<List<Integer>> groups = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()))) {
      ColumnDescriptor column =
          reader.getFileMetaData().getSchema().getColumns().get(1);
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        List<Integer> counts = new ArrayList<>();
        PageReader pages = rowGroup.getPageReader(column);
        for (long read = 0; read < pages.getTotalValueCount(); ) {
          DataPage page = pages.readPage();
          counts.add(page.getValueCount());
          read += page.getValueCount();
        }
        groups.add(counts);
      }
    }
    return groups;
  }

  private static List<Integer> pageCounts(Path file) throws IOException {
    return pageCountsByRowGroup(file).stream().flatMap(List::stream).collect(Collectors.toList());
  }
}
