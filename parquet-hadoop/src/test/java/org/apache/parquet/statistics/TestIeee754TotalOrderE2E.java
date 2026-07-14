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
package org.apache.parquet.statistics;

import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIeee754TotalOrderE2E {

  private static final Binary FLOAT16_NAN_A = Binary.fromConstantByteArray(new byte[] {0x01, 0x7e});
  private static final Binary FLOAT16_NAN_B = Binary.fromConstantByteArray(new byte[] {0x02, 0x7e});
  private static final Binary FLOAT16_ONE = Binary.fromConstantByteArray(new byte[] {0x00, 0x3c});

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final MessageType FLOAT_SCHEMA = Types.buildMessage()
      .required(PrimitiveTypeName.FLOAT)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("float_col")
      .named("msg");

  private static final MessageType DOUBLE_SCHEMA = Types.buildMessage()
      .required(PrimitiveTypeName.DOUBLE)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("double_col")
      .named("msg");

  private static final MessageType FLOAT_DOUBLE_SCHEMA = Types.buildMessage()
      .required(PrimitiveTypeName.FLOAT)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("float_col")
      .required(PrimitiveTypeName.DOUBLE)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("double_col")
      .named("msg");

  private static final MessageType FLOAT16_SCHEMA = Types.buildMessage()
      .required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
      .length(LogicalTypeAnnotation.Float16LogicalTypeAnnotation.BYTES)
      .as(LogicalTypeAnnotation.float16Type())
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("float16_col")
      .named("msg");

  private static final MessageType TYPE_DEFINED_FLOAT_SCHEMA = Types.buildMessage()
      .required(PrimitiveTypeName.FLOAT)
      .columnOrder(ColumnOrder.typeDefined())
      .named("float_col")
      .named("msg");

  private static final MessageType TYPE_DEFINED_DOUBLE_SCHEMA = Types.buildMessage()
      .required(PrimitiveTypeName.DOUBLE)
      .columnOrder(ColumnOrder.typeDefined())
      .named("double_col")
      .named("msg");

  private static final MessageType TYPE_DEFINED_FLOAT16_SCHEMA = Types.buildMessage()
      .required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
      .length(LogicalTypeAnnotation.Float16LogicalTypeAnnotation.BYTES)
      .as(LogicalTypeAnnotation.float16Type())
      .columnOrder(ColumnOrder.typeDefined())
      .named("float16_col")
      .named("msg");

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }

  private Path writeFloatFile(float... values) throws IOException {
    return writeFloatFile(FLOAT_SCHEMA, values);
  }

  private Path writeFloatFile(MessageType schema, float... values) throws IOException {
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withDictionaryEncoding(false)
        .build()) {
      GroupFactory factory = new SimpleGroupFactory(schema);
      for (float v : values) {
        writer.write(factory.newGroup().append("float_col", v));
      }
    }
    return path;
  }

  private Path writeDoubleFile(double... values) throws IOException {
    return writeDoubleFile(DOUBLE_SCHEMA, values);
  }

  private Path writeDoubleFile(MessageType schema, double... values) throws IOException {
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withDictionaryEncoding(false)
        .build()) {
      GroupFactory factory = new SimpleGroupFactory(schema);
      for (double v : values) {
        writer.write(factory.newGroup().append("double_col", v));
      }
    }
    return path;
  }

  private Path writeFloat16File(MessageType schema, Binary... values) throws IOException {
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withDictionaryEncoding(false)
        .build()) {
      GroupFactory factory = new SimpleGroupFactory(schema);
      for (Binary v : values) {
        writer.write(factory.newGroup().append("float16_col", v));
      }
    }
    return path;
  }

  private Path writeFloatDoubleFile(float[] floatValues, double[] doubleValues) throws IOException {
    Preconditions.checkArgument(floatValues.length == doubleValues.length, "Arrays must have same length");
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(FLOAT_DOUBLE_SCHEMA)
        .withDictionaryEncoding(false)
        .build()) {
      GroupFactory factory = new SimpleGroupFactory(FLOAT_DOUBLE_SCHEMA);
      for (int i = 0; i < floatValues.length; i++) {
        writer.write(
            factory.newGroup().append("float_col", floatValues[i]).append("double_col", doubleValues[i]));
      }
    }
    return path;
  }

  private List<Float> readFloatValues(Path path) throws IOException {
    return readFloatValues(path, null);
  }

  private List<Float> readFloatValues(Path path, FilterCompat.Filter filter) throws IOException {
    List<Float> result = new ArrayList<>();
    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), path);
    if (filter != null) {
      builder.withFilter(filter);
    }
    try (ParquetReader<Group> reader = builder.build()) {
      Group group;
      while ((group = reader.read()) != null) {
        result.add(group.getFloat("float_col", 0));
      }
    }
    return result;
  }

  private List<Float> readFloatValuesWithRecordFilter(Path path, FilterCompat.Filter filter) throws IOException {
    List<Float> result = new ArrayList<>();
    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), path)
        .useBloomFilter(false)
        .useDictionaryFilter(false)
        .useStatsFilter(false)
        .useColumnIndexFilter(false)
        .withFilter(filter);
    try (ParquetReader<Group> reader = builder.build()) {
      Group group;
      while ((group = reader.read()) != null) {
        result.add(group.getFloat("float_col", 0));
      }
    }
    return result;
  }

  private List<Double> readDoubleValues(Path path) throws IOException {
    return readDoubleValues(path, null);
  }

  private List<Double> readDoubleValues(Path path, FilterCompat.Filter filter) throws IOException {
    List<Double> result = new ArrayList<>();
    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), path);
    if (filter != null) {
      builder.withFilter(filter);
    }
    try (ParquetReader<Group> reader = builder.build()) {
      Group group;
      while ((group = reader.read()) != null) {
        result.add(group.getDouble("double_col", 0));
      }
    }
    return result;
  }

  private List<Double> readDoubleValuesWithRecordFilter(Path path, FilterCompat.Filter filter) throws IOException {
    List<Double> result = new ArrayList<>();
    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), path)
        .useBloomFilter(false)
        .useDictionaryFilter(false)
        .useStatsFilter(false)
        .useColumnIndexFilter(false)
        .withFilter(filter);
    try (ParquetReader<Group> reader = builder.build()) {
      Group group;
      while ((group = reader.read()) != null) {
        result.add(group.getDouble("double_col", 0));
      }
    }
    return result;
  }

  private List<Binary> readFloat16ValuesWithRecordFilter(Path path, FilterCompat.Filter filter) throws IOException {
    List<Binary> result = new ArrayList<>();
    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), path)
        .useBloomFilter(false)
        .useDictionaryFilter(false)
        .useStatsFilter(false)
        .useColumnIndexFilter(false)
        .withFilter(filter);
    try (ParquetReader<Group> reader = builder.build()) {
      Group group;
      while ((group = reader.read()) != null) {
        result.add(group.getBinary("float16_col", 0));
      }
    }
    return result;
  }

  private static void assertFloatBits(List<Float> values, int... expectedBits) {
    assertThat(values).hasSameSizeAs(expectedBits);
    for (int i = 0; i < expectedBits.length; i++) {
      assertThat(Float.floatToRawIntBits(values.get(i))).isEqualTo(expectedBits[i]);
    }
  }

  private static void assertDoubleBits(List<Double> values, long... expectedBits) {
    assertThat(values).hasSameSizeAs(expectedBits);
    for (int i = 0; i < expectedBits.length; i++) {
      assertThat(Double.doubleToRawLongBits(values.get(i))).isEqualTo(expectedBits[i]);
    }
  }

  private static void assertFloat16Bits(List<Binary> values, int... expectedBits) {
    assertThat(values).hasSameSizeAs(expectedBits);
    for (int i = 0; i < expectedBits.length; i++) {
      assertThat(values.get(i).get2BytesLittleEndian()).isEqualTo((short) expectedBits[i]);
    }
  }

  @Test
  public void testFloatStatisticsWithNaN() throws IOException {
    Path path = writeFloatFile(1.0f, Float.NaN, 3.0f, Float.NaN, 5.0f);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);
      ColumnChunkMetaData column = block.getColumns().get(0);

      // Verify column order is IEEE 754
      assertThat(column.getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.ieee754TotalOrder());

      // Verify statistics
      Statistics<?> stats = column.getStatistics();
      assertThat(stats).isNotNull();
      assertThat(stats.isNanCountSet()).as("nan_count should be set").isTrue();
      assertThat(stats.getNanCount()).as("nan_count should be 2").isEqualTo(2);

      // Min/max should exclude NaN
      FloatStatistics floatStats = (FloatStatistics) stats;
      assertThat(floatStats.getMin()).isEqualTo(1.0f);
      assertThat(floatStats.getMax()).isEqualTo(5.0f);

      // Verify column index
      ColumnIndex columnIndex = reader.readColumnIndex(column);
      assertThat(columnIndex)
          .as("ColumnIndex should not be null for IEEE 754 with NaN")
          .isNotNull();
      List<Long> nanCounts = columnIndex.getNanCounts();
      assertThat(nanCounts).as("nan_counts should be set in column index").isNotNull();
      // All values in one page, so one entry with nan_count = 2
      assertThat(nanCounts).hasSize(1);
      assertThat(nanCounts.get(0)).isEqualTo(Long.valueOf(2));

      // Verify min/max in column index exclude NaN
      List<ByteBuffer> minValues = columnIndex.getMinValues();
      List<ByteBuffer> maxValues = columnIndex.getMaxValues();
      assertThat(minValues).hasSize(1);
      float ciMin = minValues.get(0).order(ByteOrder.LITTLE_ENDIAN).getFloat(0);
      float ciMax = maxValues.get(0).order(ByteOrder.LITTLE_ENDIAN).getFloat(0);
      assertThat(ciMin).isEqualTo(1.0f);
      assertThat(ciMax).isEqualTo(5.0f);
    }
  }

  @Test
  public void testDoubleStatisticsWithNaN() throws IOException {
    // Write: -10.0, NaN, 20.0, NaN, NaN
    Path path = writeDoubleFile(-10.0, Double.NaN, 20.0, Double.NaN, Double.NaN);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);
      ColumnChunkMetaData column = block.getColumns().get(0);

      assertThat(column.getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.ieee754TotalOrder());

      Statistics<?> stats = column.getStatistics();
      assertThat(stats.isNanCountSet()).isTrue();
      assertThat(stats.getNanCount()).isEqualTo(3);

      DoubleStatistics doubleStats = (DoubleStatistics) stats;
      assertThat(doubleStats.getMin()).isEqualTo(-10.0);
      assertThat(doubleStats.getMax()).isEqualTo(20.0);

      ColumnIndex columnIndex = reader.readColumnIndex(column);
      assertThat(columnIndex).isNotNull();
      List<Long> nanCounts = columnIndex.getNanCounts();
      assertThat(nanCounts).isNotNull();
      assertThat(nanCounts).hasSize(1);
      assertThat(nanCounts.get(0)).isEqualTo(Long.valueOf(3));
    }
  }

  @Test
  public void testFloatStatisticsNoNaN() throws IOException {
    Path path = writeFloatFile(1.0f, 2.0f, 3.0f);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);
      Statistics<?> stats = column.getStatistics();

      assertThat(stats.isNanCountSet()).isTrue();
      assertThat(stats.getNanCount()).isEqualTo(0);

      FloatStatistics floatStats = (FloatStatistics) stats;
      assertThat(floatStats.getMin()).isEqualTo(1.0f);
      assertThat(floatStats.getMax()).isEqualTo(3.0f);
    }
  }

  @Test
  public void testFloatStatisticsAllNaN() throws IOException {
    Path path = writeFloatFile(Float.NaN, Float.NaN, Float.NaN);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);
      Statistics<?> stats = column.getStatistics();

      assertThat(stats.isNanCountSet()).isTrue();
      assertThat(stats.getNanCount()).isEqualTo(3);

      assertThat(stats.hasNonNullValue())
          .as("All-NaN column should have min/max values")
          .isTrue();
      FloatStatistics floatStats = (FloatStatistics) stats;
      assertThat(Float.isNaN(floatStats.getMin()))
          .as("All-NaN min should be NaN")
          .isTrue();
      assertThat(Float.isNaN(floatStats.getMax()))
          .as("All-NaN max should be NaN")
          .isTrue();
    }
  }

  @Test
  public void testRecordLevelFloatFiltersDistinguishNaNRawBitsWithIeee754TotalOrder() throws IOException {
    float nanA = Float.intBitsToFloat(0x7fc00001);
    float nanB = Float.intBitsToFloat(0x7fc00002);
    Path path = writeFloatFile(nanA, nanB, 1.0f);

    assertFloatBits(
        readFloatValuesWithRecordFilter(path, FilterCompat.get(eq(floatColumn("float_col"), nanA))),
        0x7fc00001);
    assertFloatBits(
        readFloatValuesWithRecordFilter(path, FilterCompat.get(notEq(floatColumn("float_col"), nanA))),
        0x7fc00002,
        Float.floatToRawIntBits(1.0f));
  }

  @Test
  public void testRecordLevelDoubleFiltersDistinguishNaNRawBitsWithIeee754TotalOrder() throws IOException {
    double nanA = Double.longBitsToDouble(0x7ff8000000000001L);
    double nanB = Double.longBitsToDouble(0x7ff8000000000002L);
    Path path = writeDoubleFile(nanA, nanB, 1.0);

    assertDoubleBits(
        readDoubleValuesWithRecordFilter(path, FilterCompat.get(eq(doubleColumn("double_col"), nanA))),
        0x7ff8000000000001L);
    assertDoubleBits(
        readDoubleValuesWithRecordFilter(path, FilterCompat.get(notEq(doubleColumn("double_col"), nanA))),
        0x7ff8000000000002L,
        Double.doubleToRawLongBits(1.0));
  }

  @Test
  public void testRecordLevelFloat16FiltersDistinguishNaNRawBitsWithIeee754TotalOrder() throws IOException {
    Path path = writeFloat16File(FLOAT16_SCHEMA, FLOAT16_NAN_A, FLOAT16_NAN_B, FLOAT16_ONE);

    assertFloat16Bits(
        readFloat16ValuesWithRecordFilter(
            path, FilterCompat.get(eq(binaryColumn("float16_col"), FLOAT16_NAN_A))),
        0x7e01);
    assertFloat16Bits(
        readFloat16ValuesWithRecordFilter(
            path, FilterCompat.get(notEq(binaryColumn("float16_col"), FLOAT16_NAN_A))),
        0x7e02,
        0x3c00);
  }

  @Test
  public void testRecordLevelFloatFiltersTreatNaNsEqualWithTypeDefinedOrder() throws IOException {
    float nanA = Float.intBitsToFloat(0x7fc00001);
    float nanB = Float.intBitsToFloat(0x7fc00002);
    Path path = writeFloatFile(TYPE_DEFINED_FLOAT_SCHEMA, nanA, nanB, 1.0f);

    assertFloatBits(
        readFloatValuesWithRecordFilter(path, FilterCompat.get(eq(floatColumn("float_col"), nanA))),
        0x7fc00001,
        0x7fc00002);
    assertFloatBits(
        readFloatValuesWithRecordFilter(path, FilterCompat.get(notEq(floatColumn("float_col"), nanA))),
        Float.floatToRawIntBits(1.0f));
  }

  @Test
  public void testRecordLevelDoubleFiltersTreatNaNsEqualWithTypeDefinedOrder() throws IOException {
    double nanA = Double.longBitsToDouble(0x7ff8000000000001L);
    double nanB = Double.longBitsToDouble(0x7ff8000000000002L);
    Path path = writeDoubleFile(TYPE_DEFINED_DOUBLE_SCHEMA, nanA, nanB, 1.0);

    assertDoubleBits(
        readDoubleValuesWithRecordFilter(path, FilterCompat.get(eq(doubleColumn("double_col"), nanA))),
        0x7ff8000000000001L,
        0x7ff8000000000002L);
    assertDoubleBits(
        readDoubleValuesWithRecordFilter(path, FilterCompat.get(notEq(doubleColumn("double_col"), nanA))),
        Double.doubleToRawLongBits(1.0));
  }

  @Test
  public void testRecordLevelFloat16FiltersTreatNaNsEqualWithTypeDefinedOrder() throws IOException {
    Path path = writeFloat16File(TYPE_DEFINED_FLOAT16_SCHEMA, FLOAT16_NAN_A, FLOAT16_NAN_B, FLOAT16_ONE);

    assertFloat16Bits(
        readFloat16ValuesWithRecordFilter(
            path, FilterCompat.get(eq(binaryColumn("float16_col"), FLOAT16_NAN_A))),
        0x7e01,
        0x7e02);
    assertFloat16Bits(
        readFloat16ValuesWithRecordFilter(
            path, FilterCompat.get(notEq(binaryColumn("float16_col"), FLOAT16_NAN_A))),
        0x3c00);
  }

  @Test
  public void testFloatDoubleColumnsWithNaN() throws IOException {
    float[] floatValues = {1.0f, Float.NaN, 3.0f};
    double[] doubleValues = {-5.0, Double.NaN, 10.0};
    Path path = writeFloatDoubleFile(floatValues, doubleValues);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);

      // Verify float column
      ColumnChunkMetaData floatCol = block.getColumns().get(0);
      assertThat(floatCol.getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.ieee754TotalOrder());
      Statistics<?> floatStats = floatCol.getStatistics();
      assertThat(floatStats.isNanCountSet()).isTrue();
      assertThat(floatStats.getNanCount()).isEqualTo(1);
      assertThat(((FloatStatistics) floatStats).getMin()).isEqualTo(1.0f);
      assertThat(((FloatStatistics) floatStats).getMax()).isEqualTo(3.0f);

      // Verify double column
      ColumnChunkMetaData doubleCol = block.getColumns().get(1);
      assertThat(doubleCol.getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.ieee754TotalOrder());
      Statistics<?> doubleStats = doubleCol.getStatistics();
      assertThat(doubleStats.isNanCountSet()).isTrue();
      assertThat(doubleStats.getNanCount()).isEqualTo(1);
      assertThat(((DoubleStatistics) doubleStats).getMin()).isEqualTo(-5.0);
      assertThat(((DoubleStatistics) doubleStats).getMax()).isEqualTo(10.0);

      // Verify column indexes for both
      ColumnIndex floatCI = reader.readColumnIndex(floatCol);
      assertThat(floatCI).isNotNull();
      assertThat(floatCI.getNanCounts()).isNotNull();
      assertThat(floatCI.getNanCounts().get(0)).isEqualTo(Long.valueOf(1));

      ColumnIndex doubleCI = reader.readColumnIndex(doubleCol);
      assertThat(doubleCI).isNotNull();
      assertThat(doubleCI.getNanCounts()).isNotNull();
      assertThat(doubleCI.getNanCounts().get(0)).isEqualTo(Long.valueOf(1));
    }
  }
}
