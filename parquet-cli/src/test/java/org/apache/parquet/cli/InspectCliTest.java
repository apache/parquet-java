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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class InspectCliTest extends CliTestBase {

  private static final int RECORD_COUNT = 200;
  private static final Pattern DICTIONARY_ENTRY = Pattern.compile("(?m)^\\s*\\d+: \"([^\"]*)\"$");

  private java.nio.file.Path input;

  @BeforeEach
  void createInput() throws IOException {
    input = getTempFolder().toPath().resolve("inspection input.parquet");
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("fruit")
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("color")
        .named("inspection");
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(input.toUri()))
        .withType(schema)
        .withDictionaryEncoding(true)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .build()) {
      for (int i = 0; i < RECORD_COUNT; i++) {
        writer.write(groups.newGroup()
            .append("fruit", i % 2 == 0 ? "apple" : "pear")
            .append("color", i % 2 == 0 ? "red" : "green"));
      }
    }
  }

  @Test
  void footerPrintsInterpretedMetadataByDefault() throws Exception {
    CliResult result = cli("footer", input.toString());

    assertThat(result.exitCode()).isZero();
    JsonNode footer = new ObjectMapper().readTree(result.output());
    assertThat(footer.path("blocks")).hasSize(1);
    assertThat(footer.at("/blocks/0/rowCount").asLong()).isEqualTo(RECORD_COUNT);
    assertThat(footer.at("/fileMetaData/schema/name").asText()).isEqualTo("inspection");
    assertThat(footer.at("/fileMetaData/schema/fields").findValuesAsText("name"))
        .containsExactly("fruit", "color");
  }

  @Test
  void footerRawPrintsThriftMetadata() throws Exception {
    CliResult result = cli("footer", "--raw", input.toString());

    assertThat(result.exitCode()).isZero();
    JsonNode footer = new ObjectMapper().readTree(result.output());
    assertThat(footer.path("num_rows").asLong()).isEqualTo(RECORD_COUNT);
    assertThat(footer.path("row_groups")).hasSize(1);
    assertThat(footer.at("/row_groups/0/num_rows").asLong()).isEqualTo(RECORD_COUNT);
    assertThat(footer.path("schema").findValuesAsText("name")).containsExactly("inspection", "fruit", "color");
  }

  @Test
  void pagesPrintsAllColumnsByDefault() throws Exception {
    CliResult result = cli("pages", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(pageColumns(result.output())).containsExactly("fruit", "color");
  }

  @Test
  void pagesPrintsOnlyRequestedColumn() throws Exception {
    CliResult result = cli("pages", "--columns", "color", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(pageColumns(result.output())).containsExactly("color");
  }

  @Test
  void rawPagesPrintsHeadersOnlyForRequestedColumn() throws Exception {
    CliResult result = cli("pages", "--raw", "--columns", "color", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output())
        .contains("columnName: color,", "\"dictionary_page_header\"", "\"data_page_header_v2\"")
        .doesNotContain("columnName: fruit,")
        .containsPattern("\"num_values\"\\s*:\\s*" + RECORD_COUNT + "\\b");
  }

  @ParameterizedTest
  @CsvSource({"fruit, apple, pear", "color, red, green"})
  void dictionaryPrintsValuesOfRequestedColumn(String column, String first, String second) throws Exception {
    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(new Path(input.toUri()), new Configuration()))) {
      assertThat(reader.getRowGroups()).hasSize(1);
      assertThat(reader.getRowGroups().get(0).getColumns()).hasSize(2).allSatisfy(metadata -> assertThat(
              metadata.getEncodingStats().hasDictionaryEncodedPages())
          .isTrue());
    }

    CliResult result = cli("dictionary", "--column", column, input.toString());

    assertThat(result.exitCode()).isZero();
    List<String> values = DICTIONARY_ENTRY
        .matcher(result.output())
        .results()
        .map(match -> match.group(1))
        .toList();
    assertThat(values).containsExactlyInAnyOrder(first, second);
  }

  private static List<String> pageColumns(String output) {
    return output.lines()
        .filter(line -> line.startsWith("Column: "))
        .map(line -> line.substring("Column: ".length()))
        .toList();
  }
}
