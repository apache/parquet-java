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
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
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
