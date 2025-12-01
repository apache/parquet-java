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

package org.apache.parquet.variant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestVariantReadsFromFile {
  private static final String CASE_LOCATION = null;

  private static Stream<JsonNode> cases() throws IOException {
    if (CASE_LOCATION == null) {
      return Stream.of(JsonUtil.mapper().readValue("{\"case_number\": -1}", JsonNode.class));
    }

    InputFile caseJsonInput = new LocalInputFile(Paths.get(CASE_LOCATION + "/cases.json"));
    JsonNode cases = JsonUtil.mapper().readValue(caseJsonInput.newStream(), JsonNode.class);
    Preconditions.checkArgument(
        cases != null && cases.isArray(), "Invalid case JSON, not an array: %s", caseJsonInput);

    return Streams.stream(cases);
  }

  private static Stream<Arguments> errorCases() throws IOException {
    return cases().filter(caseNode -> caseNode.has("error_message") || !caseNode.has("parquet_file"))
        .map(caseNode -> {
          int caseNumber = JsonUtil.getInt("case_number", caseNode);
          String testName = JsonUtil.getStringOrNull("test", caseNode);
          String parquetFile = JsonUtil.getStringOrNull("parquet_file", caseNode);
          String errorMessage = JsonUtil.getStringOrNull("error_message", caseNode);
          return Arguments.of(caseNumber, testName, parquetFile, errorMessage);
        });
  }

  private static Stream<Arguments> singleVariantCases() throws IOException {
    return cases().filter(caseNode -> caseNode.has("variant_file") || !caseNode.has("parquet_file"))
        .map(caseNode -> {
          int caseNumber = JsonUtil.getInt("case_number", caseNode);
          String testName = JsonUtil.getStringOrNull("test", caseNode);
          String variant = JsonUtil.getStringOrNull("variant", caseNode);
          String parquetFile = JsonUtil.getStringOrNull("parquet_file", caseNode);
          String variantFile = JsonUtil.getStringOrNull("variant_file", caseNode);
          return Arguments.of(caseNumber, testName, variant, parquetFile, variantFile);
        });
  }

  private static Stream<Arguments> multiVariantCases() throws IOException {
    return cases().filter(caseNode -> caseNode.has("variant_files") || !caseNode.has("parquet_file"))
        .map(caseNode -> {
          int caseNumber = JsonUtil.getInt("case_number", caseNode);
          String testName = JsonUtil.getStringOrNull("test", caseNode);
          String parquetFile = JsonUtil.getStringOrNull("parquet_file", caseNode);
          List<String> variantFiles = caseNode.has("variant_files")
              ? Lists.newArrayList(Iterables.transform(
                  caseNode.get("variant_files"),
                  node -> node == null || node.isNull() ? null : node.asText()))
              : null;
          String variants = JsonUtil.getStringOrNull("variants", caseNode);
          return Arguments.of(caseNumber, testName, variants, parquetFile, variantFiles);
        });
  }

  @ParameterizedTest
  @MethodSource("errorCases")
  public void testError(int caseNumber, String testName, String parquetFile, String errorMessage) {
    if (parquetFile == null) {
      return;
    }

    Assertions.assertThatThrownBy(() -> readParquet(parquetFile)).as("Test case %s: %s", caseNumber, testName);
    // .hasMessage(errorMessage);
  }

  @ParameterizedTest
  @MethodSource("singleVariantCases")
  public void testSingleVariant(
      int caseNumber, String testName, String variant, String parquetFile, String variantFile)
      throws IOException {
    if (parquetFile == null) {
      return;
    }

    Variant expected = readVariant(variantFile);

    GenericRecord record = readParquetRecord(parquetFile);
    Assertions.assertThat(record.get("var")).isInstanceOf(GenericData.Record.class);
    GenericData.Record actual = (GenericData.Record) record.get("var");
    assertEqual(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("multiVariantCases")
  public void testMultiVariant(
      int caseNumber, String testName, String variants, String parquetFile, List<String> variantFiles)
      throws IOException {
    if (parquetFile == null) {
      return;
    }

    List<GenericRecord> records = readParquet(parquetFile);

    for (int i = 0; i < records.size(); i += 1) {
      GenericRecord record = records.get(i);
      String variantFile = variantFiles.get(i);

      if (variantFile != null) {
        Variant expected = readVariant(variantFile);
        Assertions.assertThat(record.get("var")).isInstanceOf(GenericData.Record.class);
        GenericData.Record actual = (GenericData.Record) record.get("var");
        assertEqual(expected, actual);
      } else {
        Assertions.assertThat(record.get("var")).isNull();
      }
    }
  }

  private static byte[] readAllBytes(InputStream in) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte[] data = new byte[4096];
    int nRead;
    while ((nRead = in.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    return buffer.toByteArray();
  }

  private Variant readVariant(String variantFile) throws IOException {
    try (InputStream in = new LocalInputFile(Paths.get(CASE_LOCATION + "/" + variantFile)).newStream()) {
      byte[] variantBytes = readAllBytes(in);
      ByteBuffer variantBuffer = ByteBuffer.wrap(variantBytes);

      byte header = variantBytes[0];
      int offsetSize = 1 + ((header & 0b11000000) >> 6);
      int dictSize = VariantUtil.readUnsigned(variantBuffer, 1, offsetSize);
      int offsetListOffset = 1 + offsetSize;
      int dataOffset = offsetListOffset + ((1 + dictSize) * offsetSize);
      int endOffset = dataOffset
          + VariantUtil.readUnsigned(variantBuffer, offsetListOffset + (offsetSize * dictSize), offsetSize);

      return new Variant(VariantUtil.slice(variantBuffer, endOffset), variantBuffer);
    }
  }

  private GenericRecord readParquetRecord(String parquetFile) throws IOException {
    return Iterables.getOnlyElement(readParquet(parquetFile));
  }

  private List<GenericRecord> readParquet(String parquetFile) throws IOException {
    org.apache.parquet.io.InputFile inputFile = new LocalInputFile(Paths.get(CASE_LOCATION + "/" + parquetFile));
    List<GenericRecord> records = Lists.newArrayList();

    // Set Avro read schema converted from the Parquet schema
    Configuration conf = new Configuration(false);
    try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
      final MessageType fileSchema = fileReader.getFileMetaData().getSchema();
      Schema avroReadSchema = new AvroSchemaConverter().convert(fileSchema);
      AvroReadSupport.setAvroReadSchema(conf, avroReadSchema);
    }

    try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(inputFile).withConf(conf).build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        records.add(record);
      }
    }
    return records;
  }

  private static void assertEqual(Variant expected, GenericData.Record actualRecord) {
    assertThat(actualRecord).isNotNull();
    assertThat(expected).isNotNull();
    Variant actual = new Variant((ByteBuffer) actualRecord.get("value"), (ByteBuffer) actualRecord.get("metadata"));

    assertEqual(expected, actual);
  }

  private static void assertEqual(Variant expected, Variant actual) {
    assertThat(actual).isNotNull();
    assertThat(expected).isNotNull();

    assertThat(actual.getType()).isEqualTo(expected.getType());

    switch (expected.getType()) {
      case NULL:
        // nothing to compare
        break;
      case BOOLEAN:
        assertThat(actual.getBoolean()).isEqualTo(expected.getBoolean());
        break;
      case BYTE:
        assertThat(actual.getByte()).isEqualTo(expected.getByte());
        break;
      case SHORT:
        assertThat(actual.getShort()).isEqualTo(expected.getShort());
        break;
      case INT:
      case DATE:
        assertThat(actual.getInt()).isEqualTo(expected.getInt());
        break;
      case LONG:
      case TIMESTAMP_TZ:
      case TIMESTAMP_NTZ:
      case TIME:
      case TIMESTAMP_NANOS_TZ:
      case TIMESTAMP_NANOS_NTZ:
        assertThat(actual.getLong()).isEqualTo(expected.getLong());
        break;
      case FLOAT:
        assertThat(actual.getFloat()).isEqualTo(expected.getFloat());
        break;
      case DOUBLE:
        assertThat(actual.getDouble()).isEqualTo(expected.getDouble());
        break;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        assertThat(actual.getDecimal()).isEqualTo(expected.getDecimal());
        break;
      case STRING:
        assertThat(actual.getString()).isEqualTo(expected.getString());
        break;
      case BINARY:
        assertThat(actual.getBinary()).isEqualTo(expected.getBinary());
        break;
      case UUID:
        assertThat(actual.getUUID()).isEqualTo(expected.getUUID());
        break;
      case OBJECT:
        assertThat(actual.numObjectElements()).isEqualTo(expected.numObjectElements());
        for (int i = 0; i < expected.numObjectElements(); ++i) {
          Variant.ObjectField expectedField = expected.getFieldAtIndex(i);
          Variant.ObjectField actualField = actual.getFieldAtIndex(i);

          assertThat(actualField.key).isEqualTo(expectedField.key);
          assertEqual(expectedField.value, actualField.value);
        }
        break;
      case ARRAY:
        assertThat(actual.numArrayElements()).isEqualTo(expected.numArrayElements());
        for (int i = 0; i < expected.numArrayElements(); ++i) {
          assertEqual(expected.getElementAtIndex(i), actual.getElementAtIndex(i));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unknown Variant type: " + expected.getType());
    }
  }
}
