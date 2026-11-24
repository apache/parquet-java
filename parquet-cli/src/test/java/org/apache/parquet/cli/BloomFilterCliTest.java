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

import java.nio.file.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

class BloomFilterCliTest extends CliTestBase {

  private static final String BLOOM_METADATA =
      "Hash strategy: xxhash\nAlgorithm: block\nCompression: uncompressed\nBitset size: 1024";

  @Test
  void showsMetadataForEveryColumnAndRowGroup() throws Exception {
    CliResult result = cli("bloom-filter", writeBloomFile().toString());

    assertThat(result.exitCode()).as(result.output()).isZero();
    String columns = "bloom filter for column id:\nNONE\nbloom filter for column uuid:\n"
        + BLOOM_METADATA
        + "\nbloom filter for column user.name:\n"
        + BLOOM_METADATA;
    assertThat(result.output())
        .isEqualTo(rowGroup(0, columns) + "\n" + rowGroup(1, columns) + "\n" + rowGroup(2, columns));
  }

  @Test
  void showsMetadataForSelectedNestedColumn() throws Exception {
    CliResult result =
        cli("bloom-filter", "--column", "user.name", writeBloomFile().toString());

    assertThat(result.exitCode()).as(result.output()).isZero();
    String column = "bloom filter for column user.name:\n" + BLOOM_METADATA;
    assertThat(result.output())
        .isEqualTo(rowGroup(0, column) + "\n" + rowGroup(1, column) + "\n" + rowGroup(2, column));
  }

  @Test
  void showsNoneForColumnWithoutBloomFilter() throws Exception {
    CliResult result = cli("bloom-filter", "-c", "id", writeBloomFile().toString());

    assertThat(result.exitCode()).as(result.output()).isZero();
    String column = "bloom filter for column id:\nNONE";
    assertThat(result.output())
        .isEqualTo(rowGroup(0, column) + "\n" + rowGroup(1, column) + "\n" + rowGroup(2, column));
  }

  @Test
  void keepsValueMembershipChecks() throws Exception {
    CliResult result = cli(
        "bloom-filter",
        "-c",
        "uuid",
        "-v",
        "record-0,missing",
        writeBloomFile().toString());

    assertThat(result.exitCode()).as(result.output()).isZero();
    assertThat(result.output())
        .isEqualTo(rowGroup(0, "value record-0 maybe exists.\nvalue missing NOT exists.")
            + "\n"
            + rowGroup(1, "value record-0 NOT exists.\nvalue missing NOT exists.")
            + "\n"
            + rowGroup(2, "value record-0 NOT exists.\nvalue missing NOT exists."));
  }

  @Test
  void requiresColumnWhenCheckingValues() throws Exception {
    CliResult result =
        cli("bloom-filter", "-v", "record-0", writeBloomFile().toString());

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("A column is required to check values.");
  }

  @Test
  void rejectsUnknownColumn() throws Exception {
    CliResult result = cli("bloom-filter", "-c", "missing", writeBloomFile().toString());

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("Schema doesn't have column: missing");
  }

  private Path writeBloomFile() throws Exception {
    Path file = getTempFolder().toPath().resolve("input with spaces.parquet");
    MessageType schema = MessageTypeParser.parseMessageType("message record {"
        + " required int32 id; required binary uuid (STRING);"
        + " required group user { required binary name (STRING); } }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(file.toUri()))
        .withType(schema)
        .withDictionaryEncoding(false)
        .withBloomFilterEnabled("uuid", true)
        .withBloomFilterEnabled("user.name", true)
        .withMaxBloomFilterBytes(1024)
        .withRowGroupRowCountLimit(2)
        .withMinRowCountForPageSizeCheck(1)
        .withMaxRowCountForPageSizeCheck(1)
        .build()) {
      for (int i = 0; i < 5; i++) {
        Group row = factory.newGroup().append("id", i).append("uuid", "record-" + i);
        row.addGroup("user").append("name", "name-" + i);
        writer.write(row);
      }
    }
    return file;
  }

  private static String rowGroup(int index, String contents) {
    return "\nRow group " + index + ": \n" + "-".repeat(80) + "\n" + contents;
  }
}
