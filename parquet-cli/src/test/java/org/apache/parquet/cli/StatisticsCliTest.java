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
package org.apache.parquet.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StatisticsCliTest extends CliTestBase {
  private Path input;

  @BeforeEach
  void writeStatisticsFixture() throws IOException {
    input = getTempFolder().toPath().resolve("statistics data.parquet");
    writeFixture(input, true);
    try (ParquetFileReader reader = reader(input)) {
      assertThat(reader.getRowGroups())
          .extracting(BlockMetaData::getRowCount)
          .containsExactly(3L, 3L);
    }
  }

  @Test
  void columnIndexShowsBothIndexesByDefault() throws Exception {
    CliResult result = cli("column-index", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).contains("row-group 0:", "row-group 1:");
    for (String column : List.of("id", "payload.text", "payload.number", "missing")) {
      assertThat(result.output())
          .contains("column index for column " + column + ":", "offset index for column " + column + ":");
    }
    assertThat(result.output()).doesNotContain("NONE");
  }

  @Test
  void columnIndexSelectsColumnAndRowGroup() throws Exception {
    CliResult result = cli("column-index", input.toString(), "--column", "payload.text", "--row-group", "1");

    assertThat(result.exitCode()).isZero();
    assertThat(result.output())
        .contains(
            "row-group 1:",
            "column index for column payload.text:",
            "offset index for column payload.text:")
        .doesNotContain("row-group 0:", "column id:", "column payload.number:", "column missing:");
    try (ParquetFileReader reader = reader(input)) {
      ColumnChunkMetaData column =
          reader.getRowGroups().get(1).getColumns().get(1);
      assertThat(result.output())
          .contains(
              reader.readColumnIndex(column).toString(),
              reader.readOffsetIndex(column).toString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"--column-index", "--offset-index"})
  void columnIndexModeSelectsOnlyRequestedIndex(String mode) throws Exception {
    CliResult result = cli("column-index", input.toString(), "--column", "id", mode);

    String requested = mode.equals("--column-index") ? "column index" : "offset index";
    String excluded = mode.equals("--column-index") ? "offset index" : "column index";
    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).contains(requested + " for column id:").doesNotContain(excluded + " for column");
  }

  @Test
  void columnIndexPreservesRequestedColumnOrder() throws Exception {
    CliResult result = cli("column-index", input.toString(), "--column", "payload.text,id", "--column-index");

    assertThat(result.exitCode()).isZero();
    assertThat(result.output())
        .contains("column index for column payload.text:", "column index for column id:")
        .doesNotContain("column payload.number:", "column missing:");
    assertThat(result.output().indexOf("column payload.text:"))
        .isLessThan(result.output().indexOf("column id:"));
  }

  @Test
  void columnSizeReportsFooterSizesAndRatios() throws Exception {
    CliResult result = cli("column-size", input.toString());
    Map<String, Long> expected = footerSizes();
    List<SizeRow> rows = sizeRows(result);

    assertThat(rows).extracting(SizeRow::column).containsExactlyInAnyOrderElementsOf(expected.keySet());
    long total = expected.values().stream().mapToLong(Long::longValue).sum();
    for (SizeRow row : rows) {
      assertThat(row.bytes()).isEqualTo(expected.get(row.column()));
      assertThat(row.ratio()).isCloseTo((double) expected.get(row.column()) / total, offset(0.000001));
      assertThat(row.percentage()).isFalse();
    }
  }

  @Test
  void columnSizeAggregatesSelectedParentAndFormatsPercentages() throws Exception {
    CliResult result = cli("column-size", input.toString(), "--columns", "payload,id", "--percentage");
    List<SizeRow> rows = sizeRows(result);
    Map<String, Long> sizes = footerSizes();
    long payload = sizes.get("payload.text") + sizes.get("payload.number");
    long total = sizes.values().stream().mapToLong(Long::longValue).sum();

    assertThat(rows).extracting(SizeRow::column).containsExactly("payload", "id");
    assertThat(rows.get(0).bytes()).isEqualTo(payload);
    assertThat(rows.get(1).bytes()).isEqualTo(sizes.get("id"));
    assertThat(rows).extracting(SizeRow::percentage).containsOnly(true);
    assertThat(rows.get(0).ratio()).isCloseTo(100.0 * payload / total, offset(0.0001));
    assertThat(rows.get(1).ratio()).isCloseTo(100.0 * sizes.get("id") / total, offset(0.0001));
  }

  @Test
  void columnSizeSortsAllColumnsByDescendingSize() throws Exception {
    List<SizeRow> rows = sizeRows(cli("column-size", input.toString(), "--sort"));

    assertThat(rows).hasSize(4);
    assertThat(rows).extracting(SizeRow::bytes).isSortedAccordingTo(java.util.Comparator.reverseOrder());
  }

  @Test
  void columnSizeSortsSelectedColumns() throws Exception {
    List<SizeRow> rows = sizeRows(cli("column-size", input.toString(), "--columns", "id,payload", "--sort"));

    assertThat(rows).extracting(SizeRow::column).containsExactly("payload", "id");
    assertThat(rows.get(0).bytes()).isGreaterThan(rows.get(1).bytes());
  }

  @ParameterizedTest
  @ValueSource(strings = {"id", "payload.text"})
  void bloomFilterTestsTypedValues(String column) throws Exception {
    String present = column.equals("id") ? "1" : "one";
    String absent = column.equals("id") ? "99999" : "not-in-fixture";
    CliResult result =
        cli("bloom-filter", input.toString(), "--column", column, "--values", present + "," + absent);

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).contains("Row group 0:", "Row group 1:", "value " + present + " maybe exists.");
    assertThat(result.output()
            .lines()
            .filter(line -> line.equals("value " + absent + " NOT exists."))
            .count())
        .isEqualTo(2);
    assertThat(result.output()).doesNotContain("value " + absent + " maybe exists.");
  }

  @Test
  void bloomFilterReportsColumnWithoutFilter() throws Exception {
    CliResult result = cli("bloom-filter", input.toString(), "--column", "missing", "--values", "2");

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()
            .lines()
            .filter(line -> line.equals("column missing has no bloom filter"))
            .count())
        .isEqualTo(2);
    assertThat(result.output()).doesNotContain("value 2 maybe exists.", "value 2 NOT exists.");
  }

  @Test
  void sizeStatsReportsBytesAndLevelHistograms() throws Exception {
    CliResult result = cli("size-stats", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).contains("File path: " + input, "Row group 0", "Row group 1");
    String compact = result.output().replaceAll("[ \\t]+", " ");
    assertThat(compact).contains("[payload, text] 11 B [3, 0] [0, 0, 3]", "[missing] - - -");
  }

  @Test
  void sizeStatsSelectsColumnsAndRowGroup() throws Exception {
    CliResult result = cli("size-stats", input.toString(), "--columns", "id,payload.text", "--row-groups", "1");

    assertThat(result.exitCode()).isZero();
    assertThat(result.output())
        .contains("Row group 1", "[id]", "[payload, text]")
        .doesNotContain("Row group 0", "[payload, number]", "[missing]");
    assertThat(result.output().replaceAll("[ \\t]+", " ")).contains("[payload, text] 11 B [3, 0] [0, 0, 3]");
  }

  @Test
  void sizeStatsReportsUnavailableMetadata() throws Exception {
    Path withoutStats = getTempFolder().toPath().resolve("no size statistics.parquet");
    writeFixture(withoutStats, false);
    CliResult result = cli("size-stats", withoutStats.toString(), "--column", "payload.text");

    assertThat(result.exitCode()).isZero();
    assertThat(result.output().replaceAll("[ \\t]+", " ")).contains("[payload, text] - - -");
    assertThat(result.output()).doesNotContain("11 B");
  }

  @Test
  void checkStatsChecksEveryInputFile() throws Exception {
    Path second = getTempFolder().toPath().resolve("second data.parquet");
    Files.copy(input, second);
    CliResult result = cli("check-stats", input.toString(), second.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).contains(input + " has no corrupt stats", second + " has no corrupt stats");
    assertThat(result.output()
            .lines()
            .filter(line -> line.endsWith(" has no corrupt stats"))
            .count())
        .isEqualTo(2);
  }

  @Test
  void checkStatsFailsWhenLaterInputCannotBeRead() throws Exception {
    Path missing = getTempFolder().toPath().resolve("absent data.parquet");
    CliResult result = cli("check-stats", input.toString(), missing.toString());

    assertThat(result.exitCode()).isNotZero();
    assertThat(result.output())
        .contains(input + " has no corrupt stats", "absent data.parquet")
        .doesNotContain(missing + " has no corrupt stats");
  }

  private static void writeFixture(Path path, boolean sizeStatistics) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message stats { required int32 id; "
            + "repeated group payload { optional binary text (UTF8); required int64 number; } optional int64 missing; }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    List<String> text = List.of("one", "two", "three", "four", "five", "six");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toUri()))
        .withType(schema)
        .withConf(new Configuration())
        .withDictionaryEncoding(false)
        .withRowGroupRowCountLimit(3)
        .withPageRowCountLimit(2)
        .withMinRowCountForPageSizeCheck(1)
        .withMaxRowCountForPageSizeCheck(1)
        .withBloomFilterEnabled("id", true)
        .withBloomFilterEnabled("payload.text", true)
        .withSizeStatisticsEnabled(sizeStatistics)
        .build()) {
      for (int i = 0; i < text.size(); i++) {
        Group row = factory.newGroup().append("id", i + 1);
        row.addGroup("payload").append("text", text.get(i)).append("number", 100L + i);
        if (i % 2 == 1) row.append("missing", (long) i + 1);
        writer.write(row);
      }
    }
  }

  private static ParquetFileReader reader(Path path) throws IOException {
    return ParquetFileReader.open(
        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), new Configuration()));
  }

  private Map<String, Long> footerSizes() throws IOException {
    Map<String, Long> sizes = new LinkedHashMap<>();
    try (ParquetFileReader reader = reader(input)) {
      for (BlockMetaData group : reader.getRowGroups()) {
        for (ColumnChunkMetaData column : group.getColumns()) {
          sizes.merge(column.getPath().toDotString(), column.getTotalSize(), Long::sum);
        }
      }
    }
    return sizes;
  }

  private static List<SizeRow> sizeRows(CliResult result) {
    assertThat(result.exitCode()).isZero();
    Pattern pattern = Pattern.compile("(?m)^([\\w.]+)-> Size In Bytes: (\\d+) Size In Ratio: ([0-9.]+)(%)?$");
    Matcher matcher = pattern.matcher(result.output());
    List<SizeRow> rows = new ArrayList<>();
    while (matcher.find()) {
      rows.add(new SizeRow(
          matcher.group(1),
          Long.parseLong(matcher.group(2)),
          Double.parseDouble(matcher.group(3)),
          matcher.group(4) != null));
    }
    assertThat(rows).isNotEmpty();
    return rows;
  }

  private record SizeRow(String column, long bytes, double ratio, boolean percentage) {}
}
