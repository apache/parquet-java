/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.hadoop;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that files are written with EncodingStats, the stats are readable, and generally correct.
 */
public class TestReadWriteEncodingStats {
  @TempDir
  private java.nio.file.Path tempDir;

  private static final Configuration CONF = new Configuration();
  private static final int NUM_RECORDS = 1000;
  private static final MessageType SCHEMA = parseMessageType("message test { "
      + "required binary dict_binary_field; "
      + "required int32 plain_int32_field; "
      + "required binary fallback_binary_field; "
      + "} ");

  private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

  private static void writeData(ParquetWriter<Group> writer) throws IOException {
    SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
    for (int i = 0; i < NUM_RECORDS; i += 1) {
      int index = i % ALPHABET.length();

      Group group = f.newGroup()
          .append("dict_binary_field", ALPHABET.substring(index, index + 1))
          .append("plain_int32_field", i)
          .append(
              "fallback_binary_field",
              i < (NUM_RECORDS / 2)
                  ? ALPHABET.substring(index, index + 1)
                  : UUID.randomUUID().toString());

      writer.write(group);
    }
  }

  @Test
  public void testReadWrite() throws Exception {
    Path path = new Path(tempDir.resolve("encoding-stats.parquet").toUri());

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withWriterVersion(PARQUET_1_0)
        .withPageSize(1024) // ensure multiple pages are written
        .enableDictionaryEncoding()
        .withDictionaryPageSize(2 * 1024)
        .withConf(CONF)
        .withType(SCHEMA)
        .build();
    writeData(writer);
    writer.close();

    try (ParquetFileReader reader = ParquetFileReader.open(CONF, path)) {
      assertThat(reader.getRowGroups()).as("Should have one row group").hasSize(1);
      BlockMetaData rowGroup = reader.getRowGroups().get(0);

      ColumnChunkMetaData dictColumn = rowGroup.getColumns().get(0);
      EncodingStats dictStats = dictColumn.getEncodingStats();
      assertThat(dictStats)
          .as("Dict column should have non-null encoding stats")
          .isNotNull();
      assertThat(dictStats.hasDictionaryPages())
          .as("Dict column should have a dict page")
          .isTrue();
      assertThat(dictStats.hasDictionaryEncodedPages())
          .as("Dict column should have dict-encoded pages")
          .isTrue();
      assertThat(dictStats.hasNonDictionaryEncodedPages())
          .as("Dict column should not have non-dict pages")
          .isFalse();

      ColumnChunkMetaData plainColumn = rowGroup.getColumns().get(1);
      EncodingStats plainStats = plainColumn.getEncodingStats();
      assertThat(plainStats)
          .as("Plain column should have non-null encoding stats")
          .isNotNull();
      assertThat(plainStats.hasDictionaryPages())
          .as("Plain column should not have a dict page")
          .isFalse();
      assertThat(plainStats.hasDictionaryEncodedPages())
          .as("Plain column should not have dict-encoded pages")
          .isFalse();
      assertThat(plainStats.hasNonDictionaryEncodedPages())
          .as("Plain column should have non-dict pages")
          .isTrue();

      ColumnChunkMetaData fallbackColumn = rowGroup.getColumns().get(2);
      EncodingStats fallbackStats = fallbackColumn.getEncodingStats();
      assertThat(fallbackStats)
          .as("Fallback column should have non-null encoding stats")
          .isNotNull();
      assertThat(fallbackStats.hasDictionaryPages())
          .as("Fallback column should have a dict page")
          .isTrue();
      assertThat(fallbackStats.hasDictionaryEncodedPages())
          .as("Fallback column should have dict-encoded pages")
          .isTrue();
      assertThat(fallbackStats.hasNonDictionaryEncodedPages())
          .as("Fallback column should have non-dict pages")
          .isTrue();
    }
  }
}
