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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.jupiter.api.Test;

class CsvCliTest extends CliTestBase {

  @Test
  void csvSchemaInfersNullableFieldsWithRequestedRecordName() throws Exception {
    Path input = csv("sample.csv", "id,label\n1,alpha\n2,beta\n");

    CliResult result = cli("csv-schema", "--record-name", "CsvRecord", input.toString());

    assertThat(result.exitCode()).isZero();
    Schema schema = new Schema.Parser().parse(result.output());
    assertThat(schema.getName()).isEqualTo("CsvRecord");
    assertThat(schema.getFields()).extracting(Schema.Field::name).containsExactly("id", "label");
    assertThat(schema.getField("id").schema().getTypes())
        .extracting(Schema::getType)
        .containsExactly(Schema.Type.NULL, Schema.Type.LONG);
    assertThat(schema.getField("label").schema().getTypes())
        .extracting(Schema::getType)
        .containsExactly(Schema.Type.NULL, Schema.Type.STRING);
  }

  @Test
  void csvSchemaWritesOutputUsingHeaderDelimiterSkipAndRequiredField() throws Exception {
    Path input = csv("sample.csv", "preamble\n1;alpha\n2;beta\n");
    Path output = getTempFolder().toPath().resolve("schema output.avsc");

    CliResult result = cli(
        "csv-schema",
        "--record-name",
        "RequiredId",
        "--delimiter",
        ";",
        "--skip-lines",
        "1",
        "--header",
        "id;label",
        "--require",
        "id",
        "--output",
        output.toString(),
        input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).isEmpty();
    Schema schema = new Schema.Parser().parse(Files.readString(output));
    assertThat(schema.getName()).isEqualTo("RequiredId");
    assertThat(schema.getFields()).extracting(Schema.Field::name).containsExactly("id", "label");
    assertThat(schema.getField("id").schema().getType()).isEqualTo(Schema.Type.LONG);
    assertThat(schema.getField("label").schema().getTypes())
        .extracting(Schema::getType)
        .containsExactly(Schema.Type.NULL, Schema.Type.STRING);
  }

  @Test
  void csvSchemaGeneratesNamesWithoutHeader() throws Exception {
    Path input = csv("sample.csv", "1,alpha\n2,beta\n");

    CliResult result = cli("csv-schema", "--record-name", "NoHeader", "--no-header", input.toString());

    assertThat(result.exitCode()).isZero();
    Schema schema = new Schema.Parser().parse(result.output());
    assertThat(schema.getFields()).extracting(Schema.Field::name).containsExactly("field_0", "field_1");
    assertThat(schema.getField("field_0").schema().getTypes())
        .extracting(Schema::getType)
        .containsExactly(Schema.Type.NULL, Schema.Type.LONG);
  }

  @Test
  void convertCsvWritesAllRecordsWithDefaultCompression() throws Exception {
    Path input = csv("sample.csv", "id,label\n1,alpha\n2,beta\n");
    Path output = output();

    CliResult result = cli("convert-csv", "--output", output.toString(), input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(records(output)).containsExactly(row(1L, "alpha"), row(2L, "beta"));
    assertCodec(output, CompressionCodecName.GZIP);
  }

  @Test
  void convertCsvUsesHeaderDelimiterSkipAndRequiredField() throws Exception {
    Path input = csv("sample.csv", "preamble\n1;alpha\n2;beta\n");
    Path output = output();

    CliResult result = cli(
        "convert-csv",
        "--delimiter",
        ";",
        "--skip-lines",
        "1",
        "--header",
        "id;label",
        "--require",
        "id",
        "--output",
        output.toString(),
        input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(records(output)).containsExactly(row(1L, "alpha"), row(2L, "beta"));
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile(output))) {
      assertThat(reader.getFileMetaData().getSchema().getType("id").getRepetition())
          .isEqualTo(Repetition.REQUIRED);
    }
  }

  @Test
  void convertCsvUsesSuppliedSchemaInsteadOfInferredTypes() throws Exception {
    Path input = csv("sample.csv", "id,label\n1,alpha\n2,beta\n");
    Path schemaFile = getTempFolder().toPath().resolve("string id.avsc");
    Schema schema = SchemaBuilder.record("StringId")
        .fields()
        .requiredString("id")
        .requiredString("label")
        .endRecord();
    Files.writeString(schemaFile, schema.toString());
    Path output = output();

    CliResult result =
        cli("convert-csv", "--schema", schemaFile.toString(), "--output", output.toString(), input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(records(output)).containsExactly(row("1", "alpha"), row("2", "beta"));
  }

  @Test
  void convertCsvCombinesInputFilesAndUsesRequestedCodec() throws Exception {
    Path first = csv("first.csv", "id,label\n1,alpha\n2,beta\n");
    Path second = csv("second.csv", "id,label\n3,gamma\n4,delta\n");
    Path output = output();

    CliResult result = cli(
        "convert-csv",
        "--compression-codec",
        "UNCOMPRESSED",
        "--output",
        output.toString(),
        first.toString(),
        second.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(records(output))
        .containsExactly(row(1L, "alpha"), row(2L, "beta"), row(3L, "gamma"), row(4L, "delta"));
    assertCodec(output, CompressionCodecName.UNCOMPRESSED);
  }

  @Test
  void convertCsvPreservesExistingOutputWithoutOverwriteFlag() throws Exception {
    Path input = csv("sample.csv", "id,label\n1,alpha\n");
    Path output = output();
    Files.writeString(output, "existing contents");

    CliResult result = cli("convert-csv", "--output", output.toString(), input.toString());

    assertThat(result.exitCode()).isNotZero();
    assertThat(Files.readString(output)).isEqualTo("existing contents");
  }

  @Test
  void convertCsvReplacesExistingOutputWithOverwriteFlag() throws Exception {
    Path input = csv("sample.csv", "id,label\n1,alpha\n2,beta\n");
    Path output = output();
    Files.writeString(output, "existing contents");

    CliResult result = cli("convert-csv", "--overwrite", "--output", output.toString(), input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(records(output)).containsExactly(row(1L, "alpha"), row(2L, "beta"));
  }

  private Path csv(String name, String contents) throws IOException {
    Path directory = Files.createDirectories(getTempFolder().toPath().resolve("csv inputs"));
    return Files.writeString(directory.resolve(name), contents);
  }

  private Path output() {
    return getTempFolder().toPath().resolve("converted output.parquet");
  }

  private static HadoopInputFile inputFile(Path path) throws IOException {
    return HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), new Configuration());
  }

  private static List<Map<String, Object>> records(Path path) throws IOException {
    List<Map<String, Object>> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(inputFile(path)).build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        Object id = record.get("id");
        records.add(row(
            id instanceof CharSequence ? id.toString() : id,
            record.get("label").toString()));
      }
    }
    return records;
  }

  private static Map<String, Object> row(Object id, String label) {
    return Map.of("id", id, "label", label);
  }

  private static void assertCodec(Path output, CompressionCodecName codec) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile(output))) {
      assertThat(reader.getRowGroups()).isNotEmpty();
      assertThat(reader.getRowGroups().get(0).getColumns())
          .extracting(column -> column.getCodec())
          .containsOnly(codec);
    }
  }
}
