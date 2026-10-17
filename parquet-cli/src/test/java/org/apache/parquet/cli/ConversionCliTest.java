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

import static org.apache.parquet.avro.AvroWriteSupport.WRITE_PARQUET_UUID;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ConversionCliTest extends CliTestBase {

  private static final String FIRST_UUID = "123e4567-e89b-12d3-a456-426614174000";
  private static final String SECOND_UUID = "123e4567-e89b-12d3-a456-426614174001";
  private static final Schema SCHEMA = SchemaBuilder.record("record")
      .fields()
      .requiredLong("id")
      .requiredString("label")
      .name("uuid")
      .type(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)))
      .noDefault()
      .endRecord();

  private Path avroInput;
  private Path parquetInput;

  @BeforeEach
  void createInputs() throws IOException {
    avroInput = getTempFolder().toPath().resolve("input data.avro");
    parquetInput = getTempFolder().toPath().resolve("input data.parquet");
    List<GenericRecord> records = new ArrayList<>();
    for (Map<String, Object> values : expectedRecords()) {
      GenericRecord record = new GenericData.Record(SCHEMA);
      values.forEach(record::put);
      records.add(record);
    }
    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
      writer.create(SCHEMA, avroInput.toFile());
      for (GenericRecord record : records) {
        writer.append(record);
      }
    }
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
            new org.apache.hadoop.fs.Path(parquetInput.toUri()))
        .withSchema(SCHEMA)
        .withDataModel(GenericData.get())
        .build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
  }

  @Test
  void convertPreservesRecordsAndUsesDefaultCodec() throws Exception {
    Path output = output("convert");

    CliResult result = cli("convert", "--output", output.toString(), avroInput.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readParquet(output)).containsExactlyElementsOf(expectedRecords());
    assertParquetCodec(output, CompressionCodecName.GZIP);
    assertUuidStorage(output, false);
  }

  @Test
  void convertAppliesColumnProjectionAndCodec() throws Exception {
    Path output = output("convert");

    CliResult result = cli(
        "convert",
        "--columns",
        "id,label",
        "--compression-codec",
        "UNCOMPRESSED",
        "--output",
        output.toString(),
        avroInput.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readParquet(output))
        .containsExactly(Map.of("id", 1L, "label", "alpha"), Map.of("id", 2L, "label", "beta"));
    assertParquetCodec(output, CompressionCodecName.UNCOMPRESSED);
  }

  @Test
  void convertUsesSuppliedSchema() throws Exception {
    Path output = output("convert");
    Path schema = labelSchema();

    CliResult result =
        cli("convert", "--schema", schema.toString(), "--output", output.toString(), avroInput.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readParquet(output)).containsExactly(Map.of("label", "alpha"), Map.of("label", "beta"));
  }

  @Test
  void toAvroPreservesRecordsAndUsesDefaultCodec() throws Exception {
    Path output = output("to-avro");

    CliResult result = cli("to-avro", "--output", output.toString(), parquetInput.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readAvro(output)).containsExactlyElementsOf(expectedRecords());
    assertAvroCodec(output, "deflate");
  }

  @Test
  void toAvroAppliesColumnProjectionAndCodec() throws Exception {
    Path output = output("to-avro");

    CliResult result = cli(
        "to-avro",
        "--columns",
        "id,label",
        "--compression-codec",
        "UNCOMPRESSED",
        "--output",
        output.toString(),
        parquetInput.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readAvro(output))
        .containsExactly(Map.of("id", 1L, "label", "alpha"), Map.of("id", 2L, "label", "beta"));
    assertAvroCodec(output, "null");
  }

  @Test
  void toAvroUsesSuppliedSchema() throws Exception {
    Path output = output("to-avro");
    Path schema = labelSchema();

    CliResult result =
        cli("to-avro", "--schema", schema.toString(), "--output", output.toString(), parquetInput.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(readAvro(output)).containsExactly(Map.of("label", "alpha"), Map.of("label", "beta"));
  }

  @ParameterizedTest
  @CsvSource({"convert, false", "convert, true", "to-avro, false", "to-avro, true"})
  void conversionHonorsOverwrite(String command, boolean overwrite) throws Exception {
    Path output = output(command);
    Files.writeString(output, "existing contents");
    List<String> args = new ArrayList<>(List.of(command, "--output", output.toString()));
    if (overwrite) {
      args.add("--overwrite");
    }
    args.add((command.equals("convert") ? avroInput : parquetInput).toString());

    CliResult result = cli(args.toArray(String[]::new));

    if (overwrite) {
      assertThat(result.exitCode()).isZero();
      assertThat(command.equals("convert") ? readParquet(output) : readAvro(output))
          .containsExactlyElementsOf(expectedRecords());
    } else {
      assertThat(result.exitCode()).isNotZero();
      assertThat(Files.readString(output)).isEqualTo("existing contents");
    }
  }

  @Test
  void explicitConfigurationReachesParquetWriter() throws Exception {
    Path output = output("convert");

    CliResult result = cli(
        "--property",
        WRITE_PARQUET_UUID + "=true",
        "convert",
        "--output",
        output.toString(),
        avroInput.toString());

    assertThat(result.exitCode()).isZero();
    assertUuidStorage(output, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"properties", "xml"})
  void configurationFileReachesParquetWriter(String extension) throws Exception {
    Path config = configFile(extension, true);
    Path output = output("convert");

    CliResult result =
        cli("--config-file", config.toString(), "convert", "--output", output.toString(), avroInput.toString());

    assertThat(result.exitCode()).isZero();
    assertUuidStorage(output, true);
  }

  @Test
  void explicitConfigurationOverridesFileSetting() throws Exception {
    Path config = configFile("properties", true);
    Path output = output("convert");

    CliResult result = cli(
        "--config-file",
        config.toString(),
        "--conf",
        WRITE_PARQUET_UUID + "=false",
        "convert",
        "--output",
        output.toString(),
        avroInput.toString());

    assertThat(result.exitCode()).isZero();
    assertUuidStorage(output, false);
  }

  private Path labelSchema() throws IOException {
    Schema schema =
        SchemaBuilder.record("record").fields().requiredString("label").endRecord();
    return Files.writeString(getTempFolder().toPath().resolve("label schema.avsc"), schema.toString());
  }

  private Path configFile(String extension, boolean uuid) throws IOException {
    String content = extension.equals("xml")
        ? "<configuration><property><name>" + WRITE_PARQUET_UUID + "</name><value>" + uuid
            + "</value></property></configuration>"
        : WRITE_PARQUET_UUID + "=" + uuid + "\n";
    return Files.writeString(getTempFolder().toPath().resolve("writer settings." + extension), content);
  }

  private Path output(String command) {
    return getTempFolder().toPath().resolve(command.equals("convert") ? "output data.parquet" : "output data.avro");
  }

  private static List<Map<String, Object>> expectedRecords() {
    return List.of(
        Map.of("id", 1L, "label", "alpha", "uuid", FIRST_UUID),
        Map.of("id", 2L, "label", "beta", "uuid", SECOND_UUID));
  }

  private static Map<String, Object> values(GenericRecord record) {
    Map<String, Object> values = new LinkedHashMap<>();
    for (Schema.Field field : record.getSchema().getFields()) {
      Object value = record.get(field.name());
      values.put(field.name(), value instanceof Number ? value : value.toString());
    }
    return values;
  }

  private static HadoopInputFile inputFile(Path path) throws IOException {
    return HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), new Configuration());
  }

  private static List<Map<String, Object>> readParquet(Path path) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(inputFile(path)).build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        rows.add(values(record));
      }
    }
    return rows;
  }

  private static List<Map<String, Object>> readAvro(Path path) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (DataFileReader<GenericRecord> reader = new DataFileReader<>(path.toFile(), new GenericDatumReader<>())) {
      while (reader.hasNext()) {
        rows.add(values(reader.next()));
      }
    }
    return rows;
  }

  private static void assertUuidStorage(Path path, boolean uuid) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile(path))) {
      PrimitiveType type =
          reader.getFileMetaData().getSchema().getType("uuid").asPrimitiveType();
      assertThat(type.getPrimitiveTypeName())
          .isEqualTo(uuid ? PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY : PrimitiveTypeName.BINARY);
      if (uuid) {
        assertThat(type.getTypeLength()).isEqualTo(16);
        assertThat(type.getLogicalTypeAnnotation()).isEqualTo(LogicalTypeAnnotation.uuidType());
      } else {
        assertThat(type.getLogicalTypeAnnotation()).isEqualTo(LogicalTypeAnnotation.stringType());
      }
      assertThat(reader.getRowGroups()).hasSize(1);
      assertThat(reader.getRowGroups().get(0).getRowCount()).isEqualTo(2);
    }
  }

  private static void assertParquetCodec(Path path, CompressionCodecName codec) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile(path))) {
      assertThat(reader.getRowGroups().get(0).getColumns())
          .extracting(column -> column.getCodec())
          .containsOnly(codec);
    }
  }

  private static void assertAvroCodec(Path path, String codec) throws IOException {
    try (DataFileReader<GenericRecord> reader = new DataFileReader<>(path.toFile(), new GenericDatumReader<>())) {
      assertThat(reader.getMetaString("avro.codec")).isEqualTo(codec);
    }
  }
}
