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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIeee754TotalOrderEndToEnd {

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

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }

  private Path writeFloatFile(float... values) throws IOException {
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(FLOAT_SCHEMA)
        .withDictionaryEncoding(false)
        .build()) {
      GroupFactory factory = new SimpleGroupFactory(FLOAT_SCHEMA);
      for (float v : values) {
        writer.write(factory.newGroup().append("float_col", v));
      }
    }
    return path;
  }

  private Path writeDoubleFile(double... values) throws IOException {
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(DOUBLE_SCHEMA)
        .withDictionaryEncoding(false)
        .build()) {
      GroupFactory factory = new SimpleGroupFactory(DOUBLE_SCHEMA);
      for (double v : values) {
        writer.write(factory.newGroup().append("double_col", v));
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

  @Test
  public void testFloatStatisticsWithNaN() throws IOException {
    Path path = writeFloatFile(1.0f, Float.NaN, 3.0f, Float.NaN, 5.0f);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);
      ColumnChunkMetaData column = block.getColumns().get(0);

      // Verify column order is IEEE 754
      assertEquals(
          ColumnOrder.ieee754TotalOrder(), column.getPrimitiveType().columnOrder());

      // Verify statistics
      Statistics<?> stats = column.getStatistics();
      assertNotNull(stats);
      assertTrue("nan_count should be set", stats.isNanCountSet());
      assertEquals("nan_count should be 2", 2, stats.getNanCount());

      // Min/max should exclude NaN
      FloatStatistics floatStats = (FloatStatistics) stats;
      assertEquals(1.0f, floatStats.getMin(), 0.0f);
      assertEquals(5.0f, floatStats.getMax(), 0.0f);

      // Verify column index
      ColumnIndex columnIndex = reader.readColumnIndex(column);
      assertNotNull("ColumnIndex should not be null for IEEE 754 with NaN", columnIndex);
      List<Long> nanCounts = columnIndex.getNanCounts();
      assertNotNull("nan_counts should be set in column index", nanCounts);
      // All values in one page, so one entry with nan_count = 2
      assertEquals(1, nanCounts.size());
      assertEquals(Long.valueOf(2), nanCounts.get(0));

      // Verify min/max in column index exclude NaN
      List<ByteBuffer> minValues = columnIndex.getMinValues();
      List<ByteBuffer> maxValues = columnIndex.getMaxValues();
      assertEquals(1, minValues.size());
      float ciMin = minValues.get(0).order(ByteOrder.LITTLE_ENDIAN).getFloat(0);
      float ciMax = maxValues.get(0).order(ByteOrder.LITTLE_ENDIAN).getFloat(0);
      assertEquals(1.0f, ciMin, 0.0f);
      assertEquals(5.0f, ciMax, 0.0f);
    }
  }

  @Test
  public void testDoubleStatisticsWithNaN() throws IOException {
    // Write: -10.0, NaN, 20.0, NaN, NaN
    Path path = writeDoubleFile(-10.0, Double.NaN, 20.0, Double.NaN, Double.NaN);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);
      ColumnChunkMetaData column = block.getColumns().get(0);

      assertEquals(
          ColumnOrder.ieee754TotalOrder(), column.getPrimitiveType().columnOrder());

      Statistics<?> stats = column.getStatistics();
      assertTrue(stats.isNanCountSet());
      assertEquals(3, stats.getNanCount());

      DoubleStatistics doubleStats = (DoubleStatistics) stats;
      assertEquals(-10.0, doubleStats.getMin(), 0.0);
      assertEquals(20.0, doubleStats.getMax(), 0.0);

      ColumnIndex columnIndex = reader.readColumnIndex(column);
      assertNotNull(columnIndex);
      List<Long> nanCounts = columnIndex.getNanCounts();
      assertNotNull(nanCounts);
      assertEquals(1, nanCounts.size());
      assertEquals(Long.valueOf(3), nanCounts.get(0));
    }
  }

  @Test
  public void testFloatStatisticsNoNaN() throws IOException {
    Path path = writeFloatFile(1.0f, 2.0f, 3.0f);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);
      Statistics<?> stats = column.getStatistics();

      assertTrue(stats.isNanCountSet());
      assertEquals(0, stats.getNanCount());

      FloatStatistics floatStats = (FloatStatistics) stats;
      assertEquals(1.0f, floatStats.getMin(), 0.0f);
      assertEquals(3.0f, floatStats.getMax(), 0.0f);
    }
  }

  @Test
  public void testFloatStatisticsAllNaN() throws IOException {
    Path path = writeFloatFile(Float.NaN, Float.NaN, Float.NaN);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);
      Statistics<?> stats = column.getStatistics();

      assertTrue(stats.isNanCountSet());
      assertEquals(3, stats.getNanCount());

      assertTrue("All-NaN column should have non-null min/max values", stats.hasNonNullValue());
      assertTrue("All-NaN min should be NaN", Float.isNaN(((FloatStatistics) stats).getMin()));
      assertTrue("All-NaN max should be NaN", Float.isNaN(((FloatStatistics) stats).getMax()));
    }
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
      assertEquals(
          ColumnOrder.ieee754TotalOrder(), floatCol.getPrimitiveType().columnOrder());
      Statistics<?> floatStats = floatCol.getStatistics();
      assertTrue(floatStats.isNanCountSet());
      assertEquals(1, floatStats.getNanCount());
      assertEquals(1.0f, ((FloatStatistics) floatStats).getMin(), 0.0f);
      assertEquals(3.0f, ((FloatStatistics) floatStats).getMax(), 0.0f);

      // Verify double column
      ColumnChunkMetaData doubleCol = block.getColumns().get(1);
      assertEquals(
          ColumnOrder.ieee754TotalOrder(),
          doubleCol.getPrimitiveType().columnOrder());
      Statistics<?> doubleStats = doubleCol.getStatistics();
      assertTrue(doubleStats.isNanCountSet());
      assertEquals(1, doubleStats.getNanCount());
      assertEquals(-5.0, ((DoubleStatistics) doubleStats).getMin(), 0.0);
      assertEquals(10.0, ((DoubleStatistics) doubleStats).getMax(), 0.0);

      // Verify column indexes for both
      ColumnIndex floatCI = reader.readColumnIndex(floatCol);
      assertNotNull(floatCI);
      assertNotNull(floatCI.getNanCounts());
      assertEquals(Long.valueOf(1), floatCI.getNanCounts().get(0));

      ColumnIndex doubleCI = reader.readColumnIndex(doubleCol);
      assertNotNull(doubleCI);
      assertNotNull(doubleCI.getNanCounts());
      assertEquals(Long.valueOf(1), doubleCI.getNanCounts().get(0));
    }
  }
}
