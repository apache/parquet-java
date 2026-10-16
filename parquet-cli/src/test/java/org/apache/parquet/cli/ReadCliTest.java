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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReadCliTest extends CliTestBase {

  private java.nio.file.Path input;

  @BeforeEach
  void createInput() throws IOException {
    input = writeRecords("read input.parquet", 0, 12);
  }

  @Test
  void headPrintsFirstTenRecordsByDefault() throws Exception {
    CliResult result = cli("head", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readRecords(result.output())).containsExactlyElementsOf(expectedRecords(10));
  }

  @Test
  void headUsesRequestedRecordLimit() throws Exception {
    CliResult result = cli("head", "--num-records", "3", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readRecords(result.output())).containsExactlyElementsOf(expectedRecords(3));
  }

  @Test
  void catPrintsAllRecordsByDefault() throws Exception {
    CliResult result = cli("cat", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readRecords(result.output())).containsExactlyElementsOf(expectedRecords(12));
  }

  @Test
  void catPrintsOnlyRequestedColumns() throws Exception {
    CliResult result = cli("cat", "--columns", "id,label", input.toString());

    List<Map<String, Object>> expected = IntStream.range(0, 12)
        .mapToObj(i -> Map.<String, Object>of("id", i, "label", "row-" + i))
        .toList();
    assertThat(result.exitCode()).isZero();
    assertThat(readRecords(result.output())).containsExactlyElementsOf(expected);
  }

  @Test
  void scanCountsRecordsAcrossInputFiles() throws Exception {
    java.nio.file.Path second = writeRecords("second input.parquet", 100, 3);

    CliResult result = cli("scan", input.toString(), second.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output())
        .contains(
            "Scanned 12 records from " + input,
            "Scanned 3 records from " + second,
            "Scanned 15 records from 2 file(s)");
  }

  @Test
  void scanRejectsColumnMissingFromInputSchema() throws Exception {
    CliResult result = cli("scan", "--columns", "missing_field", input.toString());

    assertThat(result.exitCode()).isNotZero();
    assertThat(result.output()).contains("Cannot find field", "missing_field");
  }

  private java.nio.file.Path writeRecords(String name, int firstId, int count) throws IOException {
    java.nio.file.Path path = getTempFolder().toPath().resolve(name);
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.INT32)
        .named("id")
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("label")
        .required(PrimitiveTypeName.INT32)
        .named("unused")
        .named("record");
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(path.toUri()))
        .withType(schema)
        .build()) {
      for (int i = firstId; i < firstId + count; i++) {
        writer.write(groups.newGroup()
            .append("id", i)
            .append("label", "row-" + i)
            .append("unused", 1000 + i));
      }
    }
    return path;
  }

  private static List<Map<String, Object>> readRecords(String output) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> records = new ArrayList<>();
    for (String line : output.lines().toList()) {
      records.add(mapper.readValue(line, new TypeReference<Map<String, Object>>() {}));
    }
    return records;
  }

  private static List<Map<String, Object>> expectedRecords(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> Map.<String, Object>of("id", i, "label", "row-" + i, "unused", 1000 + i))
        .toList();
  }
}
