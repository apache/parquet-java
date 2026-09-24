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

import static org.apache.parquet.column.impl.ChunkingTestSupport.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
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
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Content defined chunking through the writer the outside world actually uses.
 *
 * <p>The unit tests in parquet-column drive a {@code ColumnWriteStore} directly, so nothing there
 * covers {@link ParquetWriter.Builder}, a real file on disk, the footer, or reading the values back
 * out. These do.
 */
public class TestCdcWriter {

  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message t { required int64 id; required binary name (STRING); }");

  private static final CdcOptions OPTIONS = options(16 * 1024, 64 * 1024, 0);

  private static final int ROWS = 40_000;

  @TempDir
  private java.nio.file.Path tempDir;

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void roundTripsEveryValue(WriterVersion version) throws IOException {
    Path file = write("roundtrip-" + version, 0, ROWS, b -> b.withWriterVersion(version));
    // With the position-based limits lifted, more than one page means the builder reached the chunker.
    assertThat(pageCounts(file)).hasSizeGreaterThan(1);

    List<String> read = new ArrayList<>();
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(new Configuration())
        .build()) {
      Group g;
      while ((g = reader.read()) != null) {
        read.add(g.getLong("id", 0) + "|" + g.getString("name", 0));
      }
    }

    // Every value, not the first and last: a cut never lands on row 0, so a value corrupted at
    // each chunk start would slip past a check of the ends.
    List<String> expected = new ArrayList<>(ROWS);
    for (int i = 0; i < ROWS; i++) {
      expected.add(i + "|" + name(i));
    }
    assertThat(read).containsExactlyElementsOf(expected);
  }

  /**
   * A row group's pages fall exactly where they would if its rows were the whole file, because
   * {@code InternalParquetRecordWriter.initStore()} rebuilds the column writers -- and their
   * chunkers -- after every flush.
   *
   * <p>This is the one design decision the three implementations most easily part ways on. Arrow
   * C++ does the same, its column writers being members of a {@code RowGroupSerializer} that
   * {@code AppendRowGroup} builds fresh; arrow-rs keeps a single chunker per file and so diverges
   * from both after the first row group. Carry the state across here and every group but the first
   * would cut somewhere else.
   */
  @Test
  public void everyRowGroupChunksAsIfItWereTheWholeFile() throws IOException {
    int perGroup = ROWS / 4;
    List<List<Integer>> groups =
        pageCountsByRowGroup(write("four-groups", 0, ROWS, b -> b.withRowGroupRowCountLimit(perGroup)));

    assertThat(groups).hasSize(4);
    for (int group = 0; group < groups.size(); ++group) {
      List<Integer> alone = pageCountsByRowGroup(
              write("group-" + group, group * perGroup, perGroup, UnaryOperator.identity()))
          .get(0);
      assertThat(groups.get(group)).as("row group %d", group).containsExactlyElementsOf(alone);
    }
  }

  @Test
  public void anInvalidEnvelopeIsIgnoredWhileChunkingIsOff() throws Exception {
    // Through ParquetOutputFormat.getRecordWriter, which is the only code that reads these keys.
    // ParquetWriter.Builder builds its properties from encodingPropsBuilder and never consults the
    // configuration, so driving this through ExampleParquetWriter would exercise nothing at all.
    Configuration conf = chunkingConf();
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, 8 * 1024 * 1024); // > the default max
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, false);

    Path off = new Path(tempDir.resolve("stale-config.parquet").toUri());
    assertThatCode(() -> new ParquetOutputFormat<Group>()
            .getRecordWriter(conf, off, CompressionCodecName.UNCOMPRESSED)
            .close(null))
        .as("a stale key must not fail a job that has the feature switched off")
        .doesNotThrowAnyException();

    // ...and the same envelope is rejected as soon as the feature is actually asked for.
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    Path on = new Path(tempDir.resolve("stale-config-on.parquet").toUri());
    assertThatThrownBy(() ->
            new ParquetOutputFormat<Group>().getRecordWriter(conf, on, CompressionCodecName.UNCOMPRESSED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking size range: maximum chunk size (1048576) must be greater "
            + "than minimum chunk size (8388608)");
  }

  /**
   * Setting the options turns chunking on, so the flag has to be able to turn it back off, as a
   * framework passing its own settings through would need.
   */
  @Test
  public void chunkingCanBeSwitchedOffAfterItsOptionsAreSet() throws IOException {
    // With the position-based limits lifted, an unchunked column chunk is one page.
    assertThat(pageCounts(write("switched-off", 0, ROWS, b -> b.withContentDefinedChunking(false))))
        .hasSize(1);
  }

  @Test
  public void theConfigurationKeysActuallyChunk() throws Exception {
    // The keys are wired to nothing unless getRecordWriter reads them, so assert the pages a
    // configured writer really produces rather than round-tripping the getters.
    Configuration conf = chunkingConf();
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, 16 * 1024);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MAX_SIZE, 64 * 1024);
    conf.setInt(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_NORM_LEVEL, 1);
    CdcOptions configured = options(16 * 1024, 64 * 1024, 1);
    assertThat(pageCounts(writeThroughOutputFormat(conf, "configured")))
        .as("every key reaches the writer")
        .containsExactlyElementsOf(
            pageCounts(write("direct", 0, ROWS, b -> b.withContentDefinedChunking(configured))));

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, false);
    assertThat(pageCounts(writeThroughOutputFormat(conf, "unconfigured")))
        .as("and do nothing at all when the feature is off")
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

  @Test
  public void aSmallEnvelopeCanCostAColumnItsDictionary() throws IOException {
    // The limitation documented on withContentDefinedChunking: FallbackValuesWriter judges the
    // dictionary on the first page alone, a small envelope makes that page too short to be a fair
    // sample, and the whole column chunk falls back to PLAIN. Arrow C++ decides on dictionary size
    // alone and does not share this.
    assertThat(encodingsOf(writeHighCardinality("dict-plain", false)))
        .as("without chunking the dictionary is kept")
        .contains(Encoding.PLAIN_DICTIONARY);
    assertThat(encodingsOf(writeHighCardinality("dict-chunked", true)))
        .as("a small envelope costs it")
        .doesNotContain(Encoding.PLAIN_DICTIONARY);
  }

  private static Set<Encoding> encodingsOf(Path file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()))) {
      return reader.getFooter().getBlocks().get(0).getColumns().get(1).getEncodings();
    }
  }

  /** 60 000 rows of ~45-byte values over 12 000 distinct ones, so the dictionary is worth keeping. */
  private Path writeHighCardinality(String label, boolean chunking) throws IOException {
    Path file = new Path(tempDir.resolve(label + ".parquet").toUri());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withType(SCHEMA)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withRowGroupSize(1L << 30)
        .withContentDefinedChunking(options(4 * 1024, 16 * 1024, 0))
        .withContentDefinedChunking(chunking)
        .build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
      Random random = new Random(7);
      String pad = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
      for (int i = 0; i < 60_000; i++) {
        writer.write(f.newGroup().append("id", (long) i).append("name", "v" + random.nextInt(12_000) + pad));
      }
    }
    return file;
  }

  // ----------------------------------------------------------- helpers

  private static String name(int i) {
    return "row-" + (i * 2654435761L % 1_000_000L);
  }

  /**
   * Writes rows {@code firstRow} to {@code firstRow + rows - 1} with chunking on and the
   * position-based page limits lifted, so every page boundary is the chunker's.
   */
  private Path write(String label, int firstRow, int rows, UnaryOperator<ExampleParquetWriter.Builder> configure)
      throws IOException {
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
      for (int i = firstRow; i < firstRow + rows; i++) {
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
