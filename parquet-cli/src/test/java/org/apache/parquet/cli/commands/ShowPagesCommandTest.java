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
package org.apache.parquet.cli.commands;

import static org.apache.parquet.cli.Util.humanReadable;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.event.LoggingEvent;

public class ShowPagesCommandTest extends ParquetFileTest {
  @Test
  public void testShowPagesCommand() throws IOException {
    File file = parquetFile();
    ShowPagesCommand command = new ShowPagesCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    command.setConf(new Configuration());
    assertThat(command.run()).isZero();
  }

  @Test
  public void testUnusedDictionaryPageSizes() throws IOException {
    Path path = new Path(randomParquetFile().toURI());
    Configuration conf = new Configuration();
    PrimitiveType type = Types.required(INT32).named("value");
    MessageType schema = new MessageType("record", type);
    try (ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, conf),
        schema,
        ParquetFileWriter.Mode.CREATE,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.MAX_PADDING_SIZE_DEFAULT)) {
      writer.start();
      writer.startBlock(2);
      writer.startColumn(
          schema.getColumnDescription(new String[] {"value"}), 2, CompressionCodecName.UNCOMPRESSED);
      writer.writeDictionaryPage(new DictionaryPage(
          BytesInput.concat(BytesInput.fromInt(10), BytesInput.fromInt(20), BytesInput.fromInt(30)),
          3,
          PLAIN));
      writer.writeDataPage(
          2,
          2 * Integer.BYTES,
          BytesInput.concat(BytesInput.fromInt(41), BytesInput.fromInt(42)),
          Statistics.createStats(type),
          RLE,
          RLE,
          PLAIN);
      writer.endColumn();
      writer.endBlock();
      writer.end(Map.of());
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      ColumnChunkMetaData column =
          reader.getRowGroups().get(0).getColumns().get(0);
      assertThat(column.getEncodingStats().hasDictionaryPages()).isTrue();
      assertThat(column.hasDictionaryPage()).isFalse();
      assertThat(column.getDictionaryPageOffset()).isPositive().isLessThan(column.getFirstDataPageOffset());
    }
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
        .withConf(conf)
        .build()) {
      assertThat(reader.read().getInteger("value", 0)).isEqualTo(41);
      assertThat(reader.read().getInteger("value", 0)).isEqualTo(42);
      assertThat(reader.read()).isNull();
    }

    withLogger((console, events) -> {
      ShowPagesCommand command = new ShowPagesCommand(console);
      command.targets = List.of(path.toString());
      command.setConf(conf);
      assertThat(command.run()).isZero();
      List<String> pageLines = events.stream()
          .map(LoggingEvent::getMessage)
          .filter(line -> line.trim().startsWith("0-"))
          .toList();
      assertThat(pageLines).hasSize(2);
      assertThat(pageLines.get(0))
          .contains("dict")
          .contains(String.format("%-7d %-10s %-10s", 3, humanReadable(4.0f), humanReadable(12L)));
      assertThat(pageLines.get(1))
          .contains("data")
          .contains(String.format("%-7d %-10s %-10s", 2, humanReadable(4.0f), humanReadable(8L)));
    });
  }

  @ParameterizedTest
  @CsvSource({
    "GZIP, PARQUET_1_0, true",
    "GZIP, PARQUET_2_0, true",
    "UNCOMPRESSED, PARQUET_1_0, true",
    "UNCOMPRESSED, PARQUET_2_0, true",
    "GZIP, PARQUET_1_0, false",
    "GZIP, PARQUET_2_0, false",
    "UNCOMPRESSED, PARQUET_1_0, false",
    "UNCOMPRESSED, PARQUET_2_0, false"
  })
  public void testPageSizesMatchHeaders(CompressionCodecName codec, WriterVersion version, boolean dictionaryEnabled)
      throws IOException {
    Path path = new Path(randomParquetFile().toURI());
    Configuration conf = new Configuration();
    MessageType schema =
        Types.buildMessage().required(BINARY).named("color").named("schema");
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withType(schema)
        .withCompressionCodec(codec)
        .withWriterVersion(version)
        .withDictionaryEncoding(dictionaryEnabled)
        .build()) {
      for (int i = 0; i < 200; i++) {
        writer.write(
            groups.newGroup().append("color", String.valueOf(i % 2).repeat(120)));
      }
    }

    withLogger((console, events) -> {
      ShowPagesCommand command = new ShowPagesCommand(console);
      command.targets = Arrays.asList(path.toString());
      command.setConf(conf);
      assertThat(command.run()).isZero();
      List<String> pageLines = events.stream()
          .map(LoggingEvent::getMessage)
          .filter(line -> line.trim().startsWith("0-"))
          .toList();
      assertThat(pageLines).hasSize(dictionaryEnabled ? 2 : 1);

      HadoopInputFile file = HadoopInputFile.fromPath(path, conf);
      try (ParquetFileReader reader = ParquetFileReader.open(file);
          SeekableInputStream input = file.newStream()) {
        ColumnChunkMetaData column =
            reader.getRowGroups().get(0).getColumns().get(0);
        assertThat(column.hasDictionaryPage()).isEqualTo(dictionaryEnabled);
        input.seek(column.getStartingPos());
        for (int i = 0; i < pageLines.size(); i++) {
          PageHeader header = Util.readPageHeader(input);
          int count = 200;
          if (dictionaryEnabled && i == 0) {
            assertThat(header.getType()).isEqualTo(PageType.DICTIONARY_PAGE);
            count = header.getDictionary_page_header().getNum_values();
            if (codec == CompressionCodecName.GZIP) {
              assertThat(header.getCompressed_page_size()).isLessThan(header.getUncompressed_page_size());
            }
          } else {
            assertThat(header.getType())
                .isEqualTo(
                    version == WriterVersion.PARQUET_1_0
                        ? PageType.DATA_PAGE
                        : PageType.DATA_PAGE_V2);
          }
          long size = header.getCompressed_page_size();
          assertThat(pageLines.get(i))
              .contains(String.format(
                  "%-7d %-10s %-10s",
                  count, humanReadable((float) size / count), humanReadable(size)));
          input.seek(input.getPos() + size);
        }
      }
    });
  }
}
