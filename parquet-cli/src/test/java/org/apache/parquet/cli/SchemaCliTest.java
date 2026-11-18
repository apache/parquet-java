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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.avro.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

class SchemaCliTest extends CliTestBase {

  private static final String PARQUET_SCHEMA = String.join(
      "\n",
      "message schema {",
      "  required int32 int32_field;",
      "  required int64 int64_field;",
      "  required float float_field;",
      "  required double double_field;",
      "  required binary binary_field;",
      "  required fixed_len_byte_array(12) flba_field;",
      "  required int32 date_field (DATE);",
      "}",
      "");

  @Test
  void printsAvroSchemaByDefault() throws Exception {
    CliResult result = cli("schema", parquetFile().getAbsolutePath());

    assertThat(result.exitCode()).isZero();
    Schema schema = new Schema.Parser().parse(result.output());
    assertThat(schema.getName()).isEqualTo("schema");
    assertThat(schema.getFields())
        .extracting(Schema.Field::name)
        .containsExactly(
            "int32_field",
            "int64_field",
            "float_field",
            "double_field",
            "binary_field",
            "flba_field",
            "date_field");
    assertThat(schema.getField("int32_field").schema().getType()).isEqualTo(Schema.Type.INT);
    assertThat(schema.getField("flba_field").schema().getFixedSize()).isEqualTo(12);
    assertThat(schema.getField("date_field").schema().getLogicalType().getName())
        .isEqualTo("date");
  }

  @Test
  void printsStandardListsWithoutAvroSchemaAsArrays() throws Exception {
    CliResult result = cli("schema", writeStandardListFile().toString());

    assertThat(result.exitCode()).as(result.output()).isZero();
    Schema tags = listSchema(result.output(), "tags");
    assertThat(tags.getType()).isEqualTo(Schema.Type.ARRAY);
    assertThat(tags.getElementType().toString()).isEqualTo("[\"null\",\"string\"]");
  }

  @Test
  void keepsExplicitAddListElementRecordsSetting() throws Exception {
    CliResult result = cli(
        "-D",
        "parquet.avro.add-list-element-records=true",
        "schema",
        writeStandardListFile().toString());

    assertThat(result.exitCode()).as(result.output()).isZero();
    Schema element = listSchema(result.output(), "tags").getElementType();
    assertThat(element.getType()).isEqualTo(Schema.Type.RECORD);
    assertThat(element.getName()).isEqualTo("list");
    assertThat(element.getFields()).extracting(Schema.Field::name).containsExactly("element");
  }

  // Written without an Avro schema in the file metadata, like files from Spark or PyArrow
  private Path writeStandardListFile() throws Exception {
    Path input = getTempFolder().toPath().resolve("lists.parquet");
    MessageType parquetSchema = Types.buildMessage()
        .optionalList()
        .optionalElement(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("tags")
        .named("record");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(input.toUri()))
        .withType(parquetSchema)
        .build()) {
      // only the schema is needed
    }
    return input;
  }

  private static Schema listSchema(String avroSchema, String field) {
    return new Schema.Parser()
        .parse(avroSchema)
        .getField(field)
        .schema()
        .getTypes()
        .get(1);
  }

  @Test
  void printsParquetSchemaForPathContainingSpaces() throws Exception {
    Path input = getTempFolder().toPath().resolve("input with spaces.parquet");
    Files.copy(parquetFile().toPath(), input);

    CliResult result = cli("schema", "--parquet", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).isEqualTo(PARQUET_SCHEMA);
  }

  @Test
  void writesSchemaToOutputFile() throws Exception {
    Path output = getTempFolder().toPath().resolve("schema output.txt");

    CliResult result = cli(
        "schema",
        "--parquet",
        "--output",
        output.toString(),
        parquetFile().getAbsolutePath());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).isEmpty();
    assertThat(Files.readString(output, StandardCharsets.UTF_8)).isEqualTo(PARQUET_SCHEMA);
  }

  @Test
  void refusesToOverwriteOutputWithoutFlag() throws Exception {
    Path output = getTempFolder().toPath().resolve("schema.txt");
    Files.writeString(output, "existing contents", StandardCharsets.UTF_8);

    CliResult result =
        cli("schema", "--output", output.toString(), parquetFile().getAbsolutePath());

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("File already exists", output.toString());
    assertThat(Files.readString(output, StandardCharsets.UTF_8)).isEqualTo("existing contents");
  }

  @Test
  void overwritesOutputWithFlag() throws Exception {
    Path output = getTempFolder().toPath().resolve("schema.txt");
    Files.writeString(output, "existing contents", StandardCharsets.UTF_8);

    CliResult result = cli(
        "schema",
        "--parquet",
        "-o",
        output.toString(),
        "--overwrite",
        parquetFile().getAbsolutePath());

    assertThat(result.exitCode()).isZero();
    assertThat(Files.readString(output, StandardCharsets.UTF_8)).isEqualTo(PARQUET_SCHEMA);
  }
}
