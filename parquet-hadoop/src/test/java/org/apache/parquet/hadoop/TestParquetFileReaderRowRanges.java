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

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.columnindex.RowRanges;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests {@link ParquetFileReader#getRowRanges(int)}.
 */
public class TestParquetFileReaderRowRanges {

  private static final int ROW_COUNT = 10_000;
  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message test { required int64 id; required int64 grp; }");

  @TempDir
  private java.nio.file.Path tempDir;

  private Path file;

  @BeforeEach
  public void writeFile() throws IOException {
    file = new Path(tempDir.resolve("row-ranges.parquet").toUri());

    // Small page size produces many pages per column chunk.
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withType(SCHEMA)
        .withWriteMode(OVERWRITE)
        .withRowGroupSize(64L * 1024 * 1024)
        .withPageSize(4 * 1024)
        .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
      for (int i = 0; i < ROW_COUNT; i++) {
        writer.write(factory.newGroup().append("id", (long) i).append("grp", (long) (i % 8)));
      }
    }
  }

  private ParquetFileReader openReader() throws IOException {
    Configuration conf = new Configuration();
    ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
    return ParquetFileReader.open(HadoopInputFile.fromPath(file, conf), options);
  }

  @Test
  public void getRowRangesWithoutFilterCoversAllRows() throws IOException {
    try (ParquetFileReader reader = openReader()) {
      assertThat(reader.getRowGroups()).hasSize(1);
      BlockMetaData block = reader.getRowGroups().get(0);

      RowRanges ranges = reader.getRowRanges(0);

      assertThat(ranges.rowCount()).isEqualTo(block.getRowCount());
      assertThat(ranges.isOverlapping(0L, block.getRowCount() - 1)).isTrue();
    }
  }

  @Test
  public void getRowRangesRejectsOutOfRangeBlockIndex() throws IOException {
    try (ParquetFileReader reader = openReader()) {
      int blockCount = reader.getRowGroups().size();
      assertThatThrownBy(() -> reader.getRowRanges(-1))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Invalid block index");
      assertThatThrownBy(() -> reader.getRowRanges(blockCount))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Invalid block index");
    }
  }
}
