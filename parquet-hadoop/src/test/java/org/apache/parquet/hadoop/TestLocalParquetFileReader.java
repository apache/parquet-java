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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestLocalParquetFileReader {

  @TempDir
  private Path tempDir;

  @ParameterizedTest
  @ValueSource(ints = {0, 10, 12})
  void invalidFileIncludesPath(int length) throws IOException {
    Path path = Files.write(tempDir.resolve("invalid.parquet"), new byte[length]);

    assertThatThrownBy(() -> {
          try (ParquetFileReader ignored = ParquetFileReader.open(new LocalInputFile(path))) {}
        })
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith(path + " is not a Parquet file");
  }

  @Test
  void validFileReportsPath() throws IOException {
    Path path = tempDir.resolve("valid.parquet");
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 value; }");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
        .withType(schema)
        .build()) {
      writer.write(new SimpleGroupFactory(schema).newGroup().append("value", 7));
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
      assertThat(reader.getFile()).isEqualTo(path.toString());
      assertThat(reader.getRecordCount()).isEqualTo(1);
      assertThat(reader.getFooter().getFileMetaData().getSchema()).isEqualTo(schema);
    }
  }
}
