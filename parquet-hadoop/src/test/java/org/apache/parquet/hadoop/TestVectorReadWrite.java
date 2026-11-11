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

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.filter.ColumnPredicates.equalTo;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;
import static org.apache.parquet.hadoop.VectorInteropFileGenerator.readFooter;
import static org.apache.parquet.hadoop.VectorInteropFileGenerator.rewriteFooter;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
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
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestVectorReadWrite {
  @TempDir
  private java.nio.file.Path tempDir;

  private static MessageType schema(String outer, int length, boolean nested) {
    String vector =
        outer + " group v (FINITE_VECTOR(" + length + ")) { repeated group list { required float element; } }";
    return MessageTypeParser.parseMessageType(
        nested ? "message m { repeated group outer { " + vector + " } }" : "message m { " + vector + " }");
  }

  private Path writeVectors(MessageType schema, WriterVersion version, boolean dictionary, List<Group> rows)
      throws IOException {
    Path path = new Path(tempDir.resolve(UUID.randomUUID() + ".parquet").toUri());
    VectorInteropFileGenerator.writeRows(path, schema, version, dictionary, rows);
    return path;
  }

  private static Path referenceFile(String name) throws java.net.URISyntaxException {
    return new Path(java.nio.file.Path.of(
            TestVectorReadWrite.class.getResource("/vector/" + name).toURI())
        .toUri());
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
      rewriteFooter(
          legacy,
          footer -> footer.getSchema().stream()
              .filter(element -> element.isSetLogicalType()
                  && element.getLogicalType().isSetFINITE_VECTOR())
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
      try (PageReadStore pages = reader.readRowGroup(0)) {
        PageReader f = pages.getPageReader(schema.getColumns().get(1));
        if (dictionary) {
          assertThat(f.readDictionaryPage()).isNotNull();
        } else {
          assertThat(f.readDictionaryPage()).isNull();
        }
        DataPage page = f.readPage();
        assertThat(page)
            .isInstanceOf(version == WriterVersion.PARQUET_1_0 ? DataPageV1.class : DataPageV2.class);
      }
    }
    List<org.apache.parquet.format.SchemaElement> vectors = readFooter(path).getSchema().stream()
        .filter(element ->
            element.isSetLogicalType() && element.getLogicalType().isSetFINITE_VECTOR())
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
      assertThat(pages.getRowIndexes()).isPresent();
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

  @org.junit.jupiter.api.Test
  public void testInvalidVectorWriteSchema() {
    GroupType vector = schema("required", 2, false).getType("v").asGroupType();
    GroupType invalid = vector.withNewFields(
        Types.repeatedGroup().optional(FLOAT).named("element").named("list"));
    MessageType schema = Types.buildMessage()
        .optionalGroup()
        .addField(invalid)
        .named("parent")
        .named("m");
    assertThatThrownBy(() -> writeVectors(schema, WriterVersion.PARQUET_1_0, false, List.of()))
        .isInstanceOf(InvalidSchemaException.class)
        .hasMessage("Invalid FINITE_VECTOR schema: " + invalid);
  }

  @org.junit.jupiter.api.Test
  public void testVectorReadProjection() throws Exception {
    MessageType list = MessageTypeParser.parseMessageType(
        schema("required", 5, false).toString().replace("FINITE_VECTOR(5)", "LIST"));
    MessageType vector = schema("required", 5, false);
    Path path = writeVectors(list, WriterVersion.PARQUET_1_0, false, listRows(list, 4, 6));
    assertVectorRows(readGroups(path, vector), 4, 6);

    Path vectorPath = writeVectors(vector, WriterVersion.PARQUET_1_0, false, listRows(vector, 4, 6));
    for (MessageType requested : List.of(vector, list)) {
      assertVectorRows(readGroups(vectorPath, requested), 4, 6);
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  public void testVectorValuesAreNotValidated(boolean validation) throws IOException {
    MessageType schema = schema("required", 3, false);
    Group row = new SimpleGroupFactory(schema).newGroup();
    Group vector = row.addGroup("v");
    for (float value : new float[] {Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY}) {
      vector.addGroup("list").append("element", value);
    }
    Path path = new Path(tempDir.resolve("non-finite.parquet").toUri());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withValidation(validation)
        .build()) {
      writer.write(row);
    }
    assertRow(readGroups(path, schema).get(0), row);
  }

  private static org.apache.parquet.format.ColumnMetaData column(org.apache.parquet.format.FileMetaData footer) {
    return footer.getRow_groups().get(0).getColumns().get(0).getMeta_data();
  }

  @ParameterizedTest
  @CsvSource({"vector_mixed_v1_plain.parquet,PARQUET_1_0,false", "vector_mixed_v2_dictionary.parquet,PARQUET_2_0,true"
  })
  public void testVectorReferenceFile(String name, WriterVersion version, boolean dictionary) throws Exception {
    verifyMixedFile(referenceFile(name), version, dictionary);
  }

  @org.junit.jupiter.api.Test
  public void testSplitVectorFile() throws Exception {
    Path path = referenceFile("vector_split_v1.parquet");
    MessageType schema = schema("required", 5, false);
    assertVectorRows(readGroups(path, schema), 5, 5);
    try (ParquetFileReader reader =
            ParquetFileReader.open(new LocalInputFile(java.nio.file.Path.of(path.toUri())));
        PageReadStore pages = reader.readRowGroup(0)) {
      PageReader column = pages.getPageReader(schema.getColumns().get(0));
      for (int count : new int[] {3, 4, 3}) {
        DataPageV1 page = (DataPageV1) column.readPage();
        assertThat(page.getValueCount()).isEqualTo(count);
      }
      assertThat(column.readPage()).isNull();
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
          .hasMessageContaining("Invalid FINITE_VECTOR counts for " + expected);
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
        .hasMessage("Invalid FINITE_VECTOR counts for "
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
        .hasMessage(
            "Invalid FINITE_VECTOR counts for [v, list, element]: rows=2, values=9, nulls=-1, length=5");
  }
}
