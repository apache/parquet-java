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

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.filter.ColumnPredicates.equalTo;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;
import static org.apache.parquet.hadoop.VectorInteropFileGenerator.readFooter;
import static org.apache.parquet.hadoop.VectorInteropFileGenerator.rewriteFooter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.columnindex.RowRanges;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.LogicalTypes;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestVectorReadWrite {
  @TempDir
  private java.nio.file.Path tempDir;

  private static MessageType schema(String outer, int length, boolean nested) {
    String vector = outer + " group v (VECTOR(" + length + ")) { repeated group list { required float element; } }";
    return MessageTypeParser.parseMessageType(
        nested ? "message m { repeated group outer { " + vector + " } }" : "message m { " + vector + " }");
  }

  private Path writeVectors(MessageType schema, WriterVersion version, boolean dictionary, List<Group> rows)
      throws IOException {
    Path path = new Path(tempDir.resolve(UUID.randomUUID() + ".parquet").toUri());
    VectorInteropFileGenerator.writeRows(path, schema, version, dictionary, rows);
    return path;
  }

  private static List<Group> listRows(MessageType schema, int... lengths) {
    List<Group> rows = new ArrayList<>();
    int value = 0;
    for (int length : lengths) {
      Group row = new SimpleGroupFactory(schema).newGroup();
      if (length >= 0) {
        Group parent = schema.containsField("outer") ? row.addGroup("outer") : row;
        Group vector = parent.addGroup("v");
        for (int i = 0; i < length; i++) {
          vector.addGroup("list").append("element", (float) value++);
        }
      }
      rows.add(row);
    }
    return rows;
  }

  private static List<Group> readGroups(Path path, MessageType requested) throws IOException {
    try (ParquetFileReader reader =
        ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())))) {
      reader.setRequestedSchema(requested);
      try (PageReadStore pages = reader.readRowGroup(0)) {
        MessageType fileSchema = reader.getFileMetaData().getSchema();
        MessageColumnIO columns = new ColumnIOFactory().getColumnIO(requested, fileSchema);
        RecordReader<Group> records = columns.getRecordReader(pages, new GroupRecordConverter(requested));
        List<Group> result = new ArrayList<>();
        for (long i = 0; i < pages.getRowCount(); i++) result.add(records.read());
        return result;
      }
    }
  }

  private static void assertVectorRows(List<Group> actual, int... lengths) {
    assertThat(actual).hasSize(lengths.length);
    int value = 0;
    for (int row = 0; row < lengths.length; row++) {
      Group vector = actual.get(row).getGroup("v", 0);
      assertThat(vector.getFieldRepetitionCount("list")).isEqualTo(lengths[row]);
      for (int i = 0; i < lengths[row]; i++) {
        assertThat(vector.getGroup("list", i).getFloat("element", 0)).isEqualTo((float) value++);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"PARQUET_1_0,false", "PARQUET_1_0,true", "PARQUET_2_0,false", "PARQUET_2_0,true"})
  public void testVectorRoundTrip(WriterVersion version, boolean dictionary) throws IOException {
    Path path = new Path(tempDir.resolve(UUID.randomUUID() + ".parquet").toUri());
    VectorInteropFileGenerator.writeMixed(path, version, dictionary);
    verifyMixedFile(path, version, dictionary);
    if (!dictionary) verifyFilteredRows(path);
    if (version == WriterVersion.PARQUET_1_0 && !dictionary) {
      Path legacy = new Path(tempDir.resolve("legacy-list.parquet").toUri());
      Files.copy(java.nio.file.Path.of(path.toUri()), java.nio.file.Path.of(legacy.toUri()));
      rewriteFooter(legacy, footer -> footer.getSchema().stream()
          .filter(element -> element.isSetLogicalType()
              && element.getLogicalType().isSetVECTOR())
          .forEach(org.apache.parquet.format.SchemaElement::unsetLogicalType));
      List<org.apache.parquet.format.SchemaElement> vectors = readFooter(legacy).getSchema().stream()
          .filter(element -> element.getConverted_type() == org.apache.parquet.format.ConvertedType.LIST)
          .toList();
      assertThat(vectors).hasSize(10);
      try (ParquetReader<Group> reader =
          ParquetReader.builder(new GroupReadSupport(), legacy).build()) {
        for (Group expected : VectorInteropFileGenerator.mixedRows()) assertRow(reader.read(), expected);
        assertThat(reader.read()).isNull();
      }
    }
  }

  private static void assertRow(Group actual, Group expected) {
    assertThat(actual).isNotNull();
    assertThat(actual.getType().getFieldCount())
        .isEqualTo(expected.getType().getFieldCount());
    for (int field = 0; field < expected.getType().getFieldCount(); field++) {
      assertThat(actual.getType().getFieldName(field))
          .isEqualTo(expected.getType().getFieldName(field));
      assertThat(actual.getFieldRepetitionCount(field)).isEqualTo(expected.getFieldRepetitionCount(field));
      for (int index = 0; index < expected.getFieldRepetitionCount(field); index++) {
        if (!expected.getType().getType(field).isPrimitive()) {
          assertRow(actual.getGroup(field, index), expected.getGroup(field, index));
        } else {
          assertThat(((SimpleGroup) actual).getObject(field, index))
              .isEqualTo(((SimpleGroup) expected).getObject(field, index));
        }
      }
    }
  }

  private static void verifyMixedFile(Path path, WriterVersion version, boolean dictionary) throws IOException {
    MessageType schema = VectorInteropFileGenerator.mixedSchema();
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), path).build()) {
      for (Group expected : VectorInteropFileGenerator.mixedRows()) assertRow(reader.read(), expected);
      assertThat(reader.read()).isNull();
    }
    try (ParquetFileReader reader =
        ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())))) {
      assertThat(reader.getFooter().getFileMetaData().getSchema()).isEqualTo(schema);
      assertThat(reader.getFooter().getBlocks()).hasSize(1);
      assertThat(reader.getFooter().getBlocks().get(0).getRowCount()).isEqualTo(6);
      List<ColumnChunkMetaData> columns =
          reader.getFooter().getBlocks().get(0).getColumns();
      for (ColumnChunkMetaData column : columns) {
        assertThat(column.getCodec()).isEqualTo(CompressionCodecName.UNCOMPRESSED);
      }
      assertThat(reader.readColumnIndex(columns.get(0))).isNotNull();
      assertThat(reader.readOffsetIndex(columns.get(0)).getPageCount()).isEqualTo(3);
      try (PageReadStore pages = reader.readRowGroup(0)) {
        PageReader f = pages.getPageReader(schema.getColumns().get(1));
        if (dictionary) {
          DictionaryPage page = f.readDictionaryPage();
          assertThat(page.getDictionarySize()).isEqualTo(10);
          assertThat(page.getEncoding())
              .isEqualTo(version == WriterVersion.PARQUET_1_0 ? PLAIN_DICTIONARY : PLAIN);
        } else {
          assertThat(f.readDictionaryPage()).isNull();
        }
        for (int i = 0; i < 3; i++) {
          DataPage page = f.readPage();
          assertThat(page.getValueCount()).isEqualTo(10);
          if (version == WriterVersion.PARQUET_1_0) {
            DataPageV1 v1 = (DataPageV1) page;
            assertThat(v1.getRlEncoding()).isEqualTo(RLE);
            assertThat(v1.getDlEncoding()).isEqualTo(RLE);
            assertThat(v1.getValueEncoding()).isEqualTo(dictionary ? PLAIN_DICTIONARY : PLAIN);
          } else {
            DataPageV2 v2 = (DataPageV2) page;
            assertThat(v2.getRowCount()).isEqualTo(2);
            assertThat(v2.getNullCount()).isZero();
            assertThat(v2.getDataEncoding()).isEqualTo(dictionary ? RLE_DICTIONARY : PLAIN);
          }
        }
        assertThat(f.readPage()).isNull();
      }
    }
    List<org.apache.parquet.format.SchemaElement> vectors = readFooter(path).getSchema().stream()
        .filter(element ->
            element.isSetLogicalType() && element.getLogicalType().isSetVECTOR())
        .toList();
    assertThat(vectors)
        .hasSize(10)
        .extracting(element -> element.getConverted_type())
        .containsOnly(ConvertedType.LIST);
    assertThat((int) vectors.get(0).getLogicalType().getSetField().getThriftFieldId())
        .isEqualTo(20);
  }

  private static void verifyFilteredRows(Path path) throws IOException {
    MessageType schema = VectorInteropFileGenerator.mixedSchema();
    List<Group> expected = VectorInteropFileGenerator.mixedRows();
    var predicate = and(gtEq(intColumn("id"), 2), ltEq(intColumn("id"), 3));
    try (ParquetFileReader reader = ParquetFileReader.open(
        new LocalInputFile(java.nio.file.Path.of(path.toUri())),
        ParquetReadOptions.builder()
            .withRecordFilter(FilterCompat.get(predicate))
            .useColumnIndexFilter(true)
            .build())) {
      assertThat(reader.getFilteredRecordCount()).isEqualTo(2);
    }
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
        .withFilter(FilterCompat.get(predicate))
        .useColumnIndexFilter(true)
        .build()) {
      assertRow(reader.read(), expected.get(2));
      assertRow(reader.read(), expected.get(3));
      assertThat(reader.read()).isNull();
    }
    try (ParquetFileReader reader =
            ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())));
        PageReadStore pages = reader.readFilteredRowGroup(
            0, RowRanges.builder().addSelectedRange(1, 4).build())) {
      assertThat(pages.getRowCount()).isEqualTo(4);
      assertThat(pages.getRowIndexes()).isPresent();
      assertThat(pages.getRowIndexes().get()).toIterable().containsExactly(1L, 2L, 3L, 4L);
      MessageColumnIO columns = new ColumnIOFactory().getColumnIO(schema, schema);
      RecordReader<Group> records = columns.getRecordReader(pages, new GroupRecordConverter(schema));
      for (int i = 1; i <= 4; i++) assertRow(records.read(), expected.get(i));
    }
    try (ParquetFileReader reader =
            ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())));
        PageReadStore pages = reader.readRowGroup(0)) {
      MessageColumnIO columns = new ColumnIOFactory().getColumnIO(schema, schema);
      RecordReader<Group> records = columns.getRecordReader(
          pages,
          new GroupRecordConverter(schema),
          FilterCompat.get(org.apache.parquet.filter.ColumnRecordFilter.column("id", equalTo(3))));
      assertRow(records.read(), expected.get(3));
      assertThat(records.read()).isNull();
    }
  }

  private static Stream<Arguments> invalidVectorWrites() {
    return Stream.of(
        Arguments.of("float element", 2, List.of(), "Invalid VECTOR length for v: expected 2, got 0"),
        Arguments.of("float element", 2, List.of(1.0f), "Invalid VECTOR length for v: expected 2, got 1"),
        Arguments.of(
            "float element",
            2,
            List.of(1.0f, 2.0f, 3.0f),
            "Invalid VECTOR length for v: expected 2, got 3"),
        Arguments.of("float element", 1, List.of(Float.NaN), "Non-finite VECTOR element in v"),
        Arguments.of("float element", 1, List.of(Float.POSITIVE_INFINITY), "Non-finite VECTOR element in v"),
        Arguments.of("float element", 1, List.of(Float.NEGATIVE_INFINITY), "Non-finite VECTOR element in v"),
        Arguments.of("double element", 1, List.of(Double.NaN), "Non-finite VECTOR element in v"),
        Arguments.of("double element", 1, List.of(Double.POSITIVE_INFINITY), "Non-finite VECTOR element in v"),
        Arguments.of("double element", 1, List.of(Double.NEGATIVE_INFINITY), "Non-finite VECTOR element in v"),
        Arguments.of(
            "fixed_len_byte_array(2) element (FLOAT16)",
            1,
            List.of(Binary.fromConstantByteArray(new byte[] {0, 0x7e})),
            "Non-finite VECTOR element in v"),
        Arguments.of(
            "fixed_len_byte_array(2) element (FLOAT16)",
            1,
            List.of(Binary.fromConstantByteArray(new byte[] {0, 0x7c})),
            "Non-finite VECTOR element in v"),
        Arguments.of(
            "fixed_len_byte_array(2) element (FLOAT16)",
            1,
            List.of(Binary.fromConstantByteArray(new byte[] {0, (byte) 0xfc})),
            "Non-finite VECTOR element in v"),
        Arguments.of(
            "fixed_len_byte_array(2) element (FLOAT16)",
            1,
            List.of(Binary.fromConstantByteArray(new byte[] {0})),
            "Invalid FLOAT16 VECTOR element length: 1"));
  }

  private void assertInvalidWrite(MessageType schema, Group row, String message) throws IOException {
    Path path = new Path(tempDir.resolve(UUID.randomUUID() + ".parquet").toUri());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withValidation(false)
        .build()) {
      assertThatThrownBy(() -> writer.write(row))
          .isInstanceOf(InvalidRecordException.class)
          .hasMessage(message);
    }
  }

  @ParameterizedTest
  @MethodSource("invalidVectorWrites")
  public void testInvalidVectorWrite(String element, int length, List<?> values, String message) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required group v (VECTOR(" + length
        + ")) { repeated group list { required " + element + "; } } }");
    Group row = new SimpleGroupFactory(schema).newGroup();
    Group vector = row.addGroup("v");
    for (Object value : values) {
      Group entry = vector.addGroup("list");
      if (value instanceof Float) {
        entry.append("element", (Float) value);
      } else if (value instanceof Double) {
        entry.append("element", (Double) value);
      } else {
        entry.append("element", (Binary) value);
      }
    }
    assertInvalidWrite(schema, row, message);
  }

  @org.junit.jupiter.api.Test
  public void testMandatoryVectorValidation() throws IOException {
    MessageType missingId = MessageTypeParser.parseMessageType("message m { required int32 id;"
        + " required group v (VECTOR(2)) { repeated group list { required float element; } } }");
    Group missingRow = new SimpleGroupFactory(missingId).newGroup();
    Group v = missingRow.addGroup("v");
    v.addGroup("list").append("element", 1.0f);
    v.addGroup("list").append("element", 2.0f);
    assertInvalidWrite(missingId, missingRow, "required field is missing required int32 id");

    MessageType duplicate = schema("required", 2, false);
    Group duplicateRow = new SimpleGroupFactory(duplicate).newGroup();
    for (int i = 0; i < 2; i++) {
      Group entry = duplicateRow.addGroup("v");
      entry.addGroup("list").append("element", i == 0 ? 1.0f : 3.0f);
      entry.addGroup("list").append("element", i == 0 ? 2.0f : 4.0f);
    }
    assertInvalidWrite(
        duplicate, duplicateRow, "repeated value when the type is not repeated in " + duplicate.getType("v"));
  }

  @org.junit.jupiter.api.Test
  public void testVectorReadProjection() throws Exception {
    MessageType list = MessageTypeParser.parseMessageType(
        schema("required", 5, false).toString().replace("VECTOR(5)", "LIST"));
    MessageType vector = schema("required", 5, false);
    Path original = writeVectors(list, WriterVersion.PARQUET_1_0, false, listRows(list, 4, 6));
    try (ParquetFileReader reader =
        ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(original.toUri())))) {
      assertThat(reader.getFooter().getBlocks()).hasSize(1);
      assertThat(reader.readOffsetIndex(reader.getFooter()
                  .getBlocks()
                  .get(0)
                  .getColumns()
                  .get(0))
              .getPageCount())
          .isEqualTo(1);
    }
    assertVectorRows(readGroups(original, vector), 4, 6);

    Path relabeled = new Path(tempDir.resolve("relabeled.parquet").toUri());
    Files.copy(java.nio.file.Path.of(original.toUri()), java.nio.file.Path.of(relabeled.toUri()));
    rewriteFooter(relabeled, footer -> footer.getSchema().get(1).setLogicalType(LogicalTypes.VECTOR(5)));
    assertThatThrownBy(() -> readGroups(relabeled, vector))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid VECTOR data for [v, list, element]: expected 5 elements, got 4");
    assertThatThrownBy(() -> readGroups(relabeled, list))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid VECTOR data for [v, list, element]: expected 5 elements, got 4");

    assertStoredLevels(relabeled, 4);

    Path generated = new Path(tempDir.resolve("invalid-generated.parquet").toUri());
    VectorInteropFileGenerator.writeInvalid(generated);
    Path reference = new Path(java.nio.file.Path.of(TestVectorReadWrite.class
            .getResource("/vector/vector_invalid_v1_dictionary.parquet")
            .toURI())
        .toUri());
    assertThat(readFooter(generated).getSchema())
        .isEqualTo(readFooter(reference).getSchema());
    verifyInvalidFixture(generated);
    verifyInvalidFixture(reference);
  }

  private void verifyInvalidFixture(Path path) throws IOException {
    MessageType schema = VectorInteropFileGenerator.invalidSchema();
    try (ParquetFileReader reader =
            ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())));
        PageReadStore pages = reader.readRowGroup(0)) {
      assertThat(reader.getFooter().getBlocks()).hasSize(1);
      assertThat(pages.getRowCount()).isEqualTo(4);
      for (ColumnChunkMetaData chunk :
          reader.getFooter().getBlocks().get(0).getColumns()) {
        assertThat(chunk.getCodec()).isEqualTo(CompressionCodecName.UNCOMPRESSED);
        assertThat(chunk.getColumnIndexReference()).isNull();
        assertThat(chunk.getOffsetIndexReference()).isNull();
        assertThat(chunk.getStatistics().isNumNullsSet()).isFalse();
      }
      for (String name : List.of("nf_f", "nf_d", "nf_h")) {
        PageReader column =
            pages.getPageReader(schema.getColumnDescription(new String[] {name, "list", "element"}));
        boolean dictionary = !name.equals("nf_h");
        if (dictionary) {
          assertThat(column.readDictionaryPage()).as(name).isNotNull();
        } else {
          assertThat(column.readDictionaryPage()).as(name).isNull();
        }
        for (int i = 0; i < 2; i++) {
          DataPageV1 page = (DataPageV1) column.readPage();
          assertThat(page.getValueCount()).isEqualTo(16);
          assertThat(page.getValueEncoding()).isEqualTo(dictionary ? PLAIN_DICTIONARY : PLAIN);
        }
        assertThat(column.readPage()).isNull();
      }
    }
    Path copy = new Path(tempDir.resolve(UUID.randomUUID() + ".parquet").toUri());
    Files.copy(java.nio.file.Path.of(path.toUri()), java.nio.file.Path.of(copy.toUri()));
    rewriteFooter(copy, footer -> footer.getSchema().stream()
        .filter(element ->
            element.isSetLogicalType() && element.getLogicalType().isSetVECTOR())
        .forEach(org.apache.parquet.format.SchemaElement::unsetLogicalType));
    List<Group> actual = readGroups(copy, schema);
    List<Group> expected = VectorInteropFileGenerator.invalidRows();
    assertThat(actual).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i++) assertRow(actual.get(i), expected.get(i));
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "short_v|expected 5 elements, got 4",
        "long_v|expected 5 elements, got 6",
        "empty_v|empty vector",
        "nested|expected 5 elements, got 4",
        "partial|expected 5 elements, got 3",
        "nf_f|non-finite element",
        "nf_d|non-finite element",
        "nf_h|non-finite element"
      },
      delimiter = '|')
  public void testInvalidVectorReferenceFile(String field, String reason) throws Exception {
    Path path = new Path(java.nio.file.Path.of(TestVectorReadWrite.class
            .getResource("/vector/vector_invalid_v1_dictionary.parquet")
            .toURI())
        .toUri());
    MessageType file;
    try (ParquetFileReader reader =
        ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())))) {
      file = reader.getFileMetaData().getSchema();
    }
    MessageType projected = new MessageType("m", file.getType("id"), file.getType(field));
    String name = field.equals("nested") || field.equals("partial") ? field + ", v" : field;
    assertThatThrownBy(() -> readGroups(path, projected))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid VECTOR data for [" + name + ", list, element]: " + reason);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"filter2", "legacy", "indexed"})
  public void testInvalidFilteredVector(String mode) throws Exception {
    Path path;
    MessageType requested;
    if (mode.equals("indexed")) {
      MessageType list = MessageTypeParser.parseMessageType(
          schema("required", 5, false).toString().replace("VECTOR(5)", "LIST"));
      path = writeVectors(list, WriterVersion.PARQUET_1_0, false, listRows(list, 4, 6));
      rewriteFooter(path, footer -> footer.getSchema().get(1).setLogicalType(LogicalTypes.VECTOR(5)));
      requested = schema("required", 5, false);
    } else {
      path = new Path(java.nio.file.Path.of(TestVectorReadWrite.class
              .getResource("/vector/vector_invalid_v1_dictionary.parquet")
              .toURI())
          .toUri());
      MessageType file;
      try (ParquetFileReader reader =
          ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())))) {
        file = reader.getFileMetaData().getSchema();
      }
      requested = new MessageType("m", file.getType("id"), file.getType("short_v"));
    }
    try (ParquetFileReader reader =
            ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())));
        PageReadStore pages = mode.equals("indexed")
            ? reader.readFilteredRowGroup(0, RowRanges.createSingle(1))
            : reader.readRowGroup(0)) {
      MessageColumnIO columns = new ColumnIOFactory()
          .getColumnIO(requested, reader.getFileMetaData().getSchema());
      RecordReader<Group> records = mode.equals("indexed")
          ? columns.getRecordReader(pages, new GroupRecordConverter(requested))
          : columns.getRecordReader(
              pages,
              new GroupRecordConverter(requested),
              mode.equals("filter2")
                  ? FilterCompat.get(eq(intColumn("id"), 1))
                  : FilterCompat.get(
                      org.apache.parquet.filter.ColumnRecordFilter.column("id", equalTo(1))));
      String name = mode.equals("indexed") ? "v" : "short_v";
      assertThatThrownBy(records::read)
          .isInstanceOf(ParquetDecodingException.class)
          .hasMessage("Invalid VECTOR data for [" + name + ", list, element]: expected 5 elements, got 4");
    }
  }

  private static org.apache.parquet.format.ColumnMetaData column(org.apache.parquet.format.FileMetaData footer) {
    return footer.getRow_groups().get(0).getColumns().get(0).getMeta_data();
  }

  @ParameterizedTest
  @CsvSource({"vector_mixed_v1_plain.parquet,PARQUET_1_0,false", "vector_mixed_v2_dictionary.parquet,PARQUET_2_0,true"
  })
  public void testVectorReferenceFile(String name, WriterVersion version, boolean dictionary) throws Exception {
    java.nio.file.Path local = java.nio.file.Path.of(
        TestVectorReadWrite.class.getResource("/vector/" + name).toURI());
    verifyMixedFile(new Path(local.toUri()), version, dictionary);
  }

  @org.junit.jupiter.api.Test
  public void testSplitVectorFile() throws Exception {
    Path generated = new Path(tempDir.resolve("split.parquet").toUri());
    VectorInteropFileGenerator.writeSplit(generated);
    Path reference = new Path(java.nio.file.Path.of(TestVectorReadWrite.class
            .getResource("/vector/vector_split_v1.parquet")
            .toURI())
        .toUri());
    for (Path path : List.of(generated, reference)) verifySplitFile(path);
  }

  private static void verifySplitFile(Path path) throws IOException {
    MessageType schema = schema("required", 5, false);
    try (ParquetFileReader reader =
        ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())))) {
      assertThat(reader.getFileMetaData().getSchema()).isEqualTo(schema);
      assertThat(reader.getFooter().getBlocks()).hasSize(1);
      assertThat(reader.getFooter().getBlocks().get(0).getRowCount()).isEqualTo(2);
      ColumnChunkMetaData chunk =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);
      assertThat(chunk.getValueCount()).isEqualTo(10);
      assertThat(chunk.getCodec()).isEqualTo(CompressionCodecName.UNCOMPRESSED);
      assertThat(chunk.getColumnIndexReference()).isNull();
      assertThat(chunk.getOffsetIndexReference()).isNull();
      try (PageReadStore pages = reader.readRowGroup(0)) {
        PageReader column = pages.getPageReader(schema.getColumns().get(0));
        assertThat(column.readDictionaryPage()).isNull();
        int[] counts = {3, 4, 3};
        for (int i = 0; i < counts.length; i++) {
          DataPageV1 page = (DataPageV1) column.readPage();
          assertThat(page.getValueCount()).isEqualTo(counts[i]);
          assertThat(page.getValueEncoding()).isEqualTo(PLAIN);
          assertThat(page.getRlEncoding()).isEqualTo(i == 1 ? BIT_PACKED : RLE);
          assertThat(page.getDlEncoding()).isEqualTo(i == 1 ? BIT_PACKED : RLE);
        }
        assertThat(column.readPage()).isNull();
      }
    }
    assertVectorRows(readGroups(path, schema), 5, 5);
    assertStoredLevels(path, 5);
  }

  private static void assertStoredLevels(Path path, int secondStart) throws IOException {
    try (ParquetFileReader reader =
            ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())));
        PageReadStore pages = reader.readRowGroup(0)) {
      MessageType schema = reader.getFileMetaData().getSchema();
      ColumnReadStoreImpl store = new ColumnReadStoreImpl(
          pages,
          new GroupRecordConverter(schema).getRootConverter(),
          schema,
          reader.getFileMetaData().getCreatedBy());
      ColumnReader column = store.getColumnReader(schema.getColumns().get(0));
      for (int i = 0; i < 10; i++) {
        assertThat(column.getFloat()).isEqualTo((float) i);
        assertThat(column.getCurrentRepetitionLevel()).isEqualTo(i == 0 || i == secondStart ? 0 : 1);
        assertThat(column.getCurrentDefinitionLevel()).isEqualTo(1);
        column.consume();
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
    "required,5,false,4,18,0,false,false",
    "required,5,false,4,15,0,true,true",
    "optional,5,false,3,11,0,false,true",
    "optional,5,false,3,10,-1,false,false",
    "required,1,false,3,2,0,false,false",
    "required,5,false,9223372036854775807,15,0,false,false",
    "required,2,true,3,5,0,false,false"
  })
  public void testInvalidVectorFooter(
      String outer,
      int length,
      boolean nested,
      long rowCount,
      long values,
      long nulls,
      boolean projectList,
      boolean filtered)
      throws IOException {
    MessageType fileSchema = schema(outer, length, nested);
    int count = rowCount == Long.MAX_VALUE ? 3 : (int) rowCount;
    int[] lengths = new int[count];
    Arrays.fill(lengths, length);
    if (outer.equals("optional")) lengths[0] = -1;
    Path path = writeVectors(fileSchema, PARQUET_2_0, false, listRows(fileSchema, lengths));
    rewriteFooter(path, footer -> {
      footer.setNum_rows(rowCount);
      footer.getRow_groups().get(0).setNum_rows(rowCount);
      column(footer).setNum_values(values);
      if (nulls == -1) {
        column(footer).getStatistics().unsetNull_count();
      } else {
        column(footer).getStatistics().setNull_count(nulls);
      }
    });
    String expected = (nested ? "[outer, v, list, element]" : "[v, list, element]") + ": rows=" + rowCount
        + ", values=" + values + ", nulls=" + nulls + ", length=" + length;
    for (boolean select : filtered ? new boolean[] {false, true} : new boolean[] {false}) {
      assertThatThrownBy(() -> {
            try (ParquetFileReader reader =
                ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())))) {
              if (projectList) {
                reader.setRequestedSchema(MessageTypeParser.parseMessageType(
                    "message m { repeated group v (LIST) { repeated group list {"
                        + " required float element; } } }"));
              }
              assertThat(reader.readOffsetIndex(reader.getFooter()
                      .getBlocks()
                      .get(0)
                      .getColumns()
                      .get(0)))
                  .isNotNull();
              if (select) {
                reader.readFilteredRowGroup(0, RowRanges.createSingle(1));
              } else {
                reader.readRowGroup(0);
              }
            }
          })
          .isInstanceOf(ParquetDecodingException.class)
          .hasMessageContaining("Invalid VECTOR counts for " + expected);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,3,10,0", "false,2,10,1", "false,-1,10,0", "false,2,10,-1", "true,2,9,0"})
  public void testInvalidVectorPageHeader(boolean nested, int headerRows, int headerValues, int headerNulls)
      throws IOException {
    MessageType fileSchema = schema("required", nested ? 2 : 5, nested);
    Path path = new Path(tempDir.resolve(UUID.randomUUID() + ".parquet").toUri());
    try (ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, new Configuration()),
        fileSchema,
        CREATE,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        null,
        ParquetProperties.builder().build())) {
      FloatStatistics stats = new FloatStatistics();
      stats.updateStats(1.0f);
      stats.setNumNulls(0);
      writer.start();
      writer.startBlock(2);
      writer.startColumn(fileSchema.getColumns().get(0), 10, CompressionCodecName.UNCOMPRESSED);
      writer.writeDataPageV2(
          headerRows,
          headerNulls,
          headerValues,
          BytesInput.empty(),
          BytesInput.empty(),
          PLAIN,
          BytesInput.empty(),
          false,
          0,
          stats);
      writer.endColumn();
      writer.endBlock();
      writer.end(Collections.emptyMap());
    }
    assertThatThrownBy(() -> {
          try (ParquetFileReader reader =
              ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())))) {
            reader.readRowGroup(0);
          }
        })
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid VECTOR counts for "
            + (nested ? "[outer, v, list, element]" : "[v, list, element]")
            + ": rows=" + headerRows + ", values=" + headerValues + ", nulls=" + headerNulls
            + ", length=" + (nested ? 2 : 5));
  }

  @org.junit.jupiter.api.Test
  public void testInvalidIndexedVectorPage() throws IOException {
    MessageType fileSchema = schema("required", 5, false);
    Path path = new Path(tempDir.resolve("invalid-indexed.parquet").toUri());
    FloatStatistics stats = new FloatStatistics();
    stats.updateStats(1.0f);
    stats.setNumNulls(0);
    try (ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, new Configuration()),
        fileSchema,
        CREATE,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        null,
        ParquetProperties.builder().build())) {
      writer.start();
      writer.startBlock(4);
      writer.startColumn(fileSchema.getColumns().get(0), 20, CompressionCodecName.UNCOMPRESSED);
      writer.writeDataPage(9, 0, BytesInput.empty(), stats, 2, RLE, RLE, PLAIN);
      writer.writeDataPage(11, 0, BytesInput.empty(), stats, 2, RLE, RLE, PLAIN);
      writer.endColumn();
      writer.endBlock();
      writer.end(Collections.emptyMap());
    }
    assertThatThrownBy(() -> {
          try (ParquetFileReader reader =
              ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())))) {
            assertThat(reader.readOffsetIndex(reader.getFooter()
                    .getBlocks()
                    .get(0)
                    .getColumns()
                    .get(0)))
                .isNotNull();
            reader.readFilteredRowGroup(0, RowRanges.createSingle(1));
          }
        })
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid VECTOR counts for [v, list, element]: rows=2, values=9, nulls=-1, length=5");
  }
}
