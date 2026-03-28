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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.FloatingPointNanInteropFileGenerator.GenerationResult;
import org.apache.parquet.hadoop.FloatingPointNanInteropFileGenerator.Scenario;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Float16;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestInterOpReadFloatingPointNanCount {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testReadStatsAndColumnIndex() throws IOException {
    Configuration conf = new Configuration();
    GenerationResult generated = FloatingPointNanInteropFileGenerator.generateAndMerge(
        conf, new Path(temp.newFolder().getAbsolutePath()));

    verifyFile(generated.getNoNanFile(), conf, Scenario.NO_NAN);
    verifyFile(generated.getMixedNanFile(), conf, Scenario.MIXED_NAN);
    verifyFile(generated.getAllNanFile(), conf, Scenario.ALL_NAN);
    verifyFile(generated.getMergedFile(), conf, Scenario.NO_NAN, Scenario.MIXED_NAN, Scenario.ALL_NAN);
  }

  private static void verifyFile(Path file, Configuration conf, Scenario... scenarios) throws IOException {
    List<Float> expectFloats = concatFloatValues(scenarios);
    List<Double> expectDoubles = concatDoubleValues(scenarios);
    List<Binary> expectFloat16s = concatFloat16Values(scenarios);

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf));
        ParquetReader<Group> recordReader = ParquetReader.builder(new GroupReadSupport(), file)
            .withConf(conf)
            .build()) {
      List<BlockMetaData> blocks = reader.getFooter().getBlocks();
      assertEquals(scenarios.length, blocks.size());

      // Verify read values
      int rowId = 0;
      Group group;
      while ((group = recordReader.read()) != null) {
        assertFloatValue(expectFloats.get(rowId), group.getFloat("float_ieee754", 0));
        assertFloatValue(expectFloats.get(rowId), group.getFloat("float_typedef", 0));

        assertDoubleValue(expectDoubles.get(rowId), group.getDouble("double_ieee754", 0));
        assertDoubleValue(expectDoubles.get(rowId), group.getDouble("double_typedef", 0));

        assertEquals(expectFloat16s.get(rowId), group.getBinary("float16_ieee754", 0));
        assertEquals(expectFloat16s.get(rowId), group.getBinary("float16_typedef", 0));
        rowId++;
      }
      assertEquals(10 * scenarios.length, rowId);

      // Verify stats
      for (int rg = 0; rg < blocks.size(); rg++) {
        BlockMetaData block = blocks.get(rg);
        assertEquals(10L, block.getRowCount());
        for (ColumnChunkMetaData column : block.getColumns()) {
          verifyStats(scenarios[rg], column);
          verifyColumnIndex(reader, scenarios[rg], column);
        }
      }
    }
  }

  private static void verifyStats(Scenario scenario, ColumnChunkMetaData column) {
    String name = column.getPath().toDotString();
    Statistics<?> stats = column.getStatistics();
    assertTrue(stats.isNanCountSet());

    if (name.contains("float16")) {
      verifyFloat16Stats(scenario, name, (BinaryStatistics) stats);
    } else if (name.contains("double")) {
      verifyDoubleStats(scenario, name, (DoubleStatistics) stats);
    } else {
      verifyFloatStats(scenario, name, (FloatStatistics) stats);
    }
  }

  private static void verifyFloatStats(Scenario scenario, String name, FloatStatistics stats) {
    if (scenario == Scenario.NO_NAN) {
      assertEquals(0L, stats.getNanCount());
      assertEquals(-2f, stats.getMin(), 0f);
      assertEquals(5f, stats.getMax(), 0f);
      return;
    }

    if (scenario == Scenario.MIXED_NAN) {
      assertEquals(2L, stats.getNanCount());
      if (isIeee(name)) {
        assertEquals(-2f, stats.getMin(), 0f);
        assertEquals(5f, stats.getMax(), 0f);
      }
      return;
    }

    // ALL_NAN
    assertEquals(10L, stats.getNanCount());
    if (isIeee(name)) {
      assertTrue(Float.isNaN(stats.getMin()));
      assertTrue(Float.isNaN(stats.getMax()));
    }
  }

  private static void verifyDoubleStats(Scenario scenario, String name, DoubleStatistics stats) {
    if (scenario == Scenario.NO_NAN) {
      assertEquals(0L, stats.getNanCount());
      assertEquals(-2d, stats.getMin(), 0d);
      assertEquals(5d, stats.getMax(), 0d);
      return;
    }

    if (scenario == Scenario.MIXED_NAN) {
      assertEquals(2L, stats.getNanCount());
      if (isIeee(name)) {
        assertEquals(-2d, stats.getMin(), 0d);
        assertEquals(5d, stats.getMax(), 0d);
      }
      return;
    }

    assertEquals(10L, stats.getNanCount());
    if (isIeee(name)) {
      assertTrue(Double.isNaN(stats.getMin()));
      assertTrue(Double.isNaN(stats.getMax()));
    }
  }

  private static void verifyFloat16Stats(Scenario scenario, String name, BinaryStatistics stats) {
    if (scenario == Scenario.NO_NAN) {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertEquals(0L, stats.getNanCount());
      assertEquals((short) 0xc000, min);
      assertEquals((short) 0x4500, max);
      return;
    }

    if (scenario == Scenario.MIXED_NAN) {
      assertEquals(2L, stats.getNanCount());
      if (isIeee(name)) {
        short min = stats.genericGetMin().get2BytesLittleEndian();
        short max = stats.genericGetMax().get2BytesLittleEndian();
        assertEquals((short) 0xc000, min);
        assertEquals((short) 0x4500, max);
      }
      return;
    }

    assertEquals(10L, stats.getNanCount());
    if (isIeee(name)) {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertTrue(Float16.isNaN(min));
      assertTrue(Float16.isNaN(max));
    }
  }

  private static void verifyColumnIndex(ParquetFileReader reader, Scenario scenario, ColumnChunkMetaData column)
      throws IOException {
    String name = column.getPath().toDotString();
    boolean ieee = isIeee(name);
    boolean hasNaN = scenario != Scenario.NO_NAN;

    ColumnIndex columnIndex = reader.readColumnIndex(column);
    if (!ieee && hasNaN) {
      // TypeDefinedOrder + NaN => page index is intentionally invalidated.
      assertNull(columnIndex);
      return;
    }

    assertNotNull(columnIndex);
    assertNotNull(columnIndex.getNanCounts());
    assertEquals(1, columnIndex.getNanCounts().size());
    assertEquals(
        expectedNanCount(scenario), (long) columnIndex.getNanCounts().get(0));

    assertEquals(1, columnIndex.getMinValues().size());
    assertEquals(1, columnIndex.getMaxValues().size());
    ByteBuffer min = columnIndex.getMinValues().get(0).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer max = columnIndex.getMaxValues().get(0).order(ByteOrder.LITTLE_ENDIAN);

    if (name.contains("float16")) {
      verifyFloat16ColumnIndexBounds(scenario, min, max);
      return;
    }

    if (name.contains("double")) {
      verifyDoubleColumnIndexBounds(scenario, min, max);
      return;
    }

    verifyFloatColumnIndexBounds(scenario, min, max);
  }

  private static long expectedNanCount(Scenario scenario) {
    return scenario == Scenario.NO_NAN ? 0L : (scenario == Scenario.MIXED_NAN ? 2L : 10L);
  }

  private static void verifyFloat16ColumnIndexBounds(Scenario scenario, ByteBuffer min, ByteBuffer max) {
    short minV = min.getShort(0);
    short maxV = max.getShort(0);
    if (scenario == Scenario.ALL_NAN) {
      assertEquals((short) 0x7c01, minV);
      assertEquals((short) 0x7fff, maxV);
    } else {
      assertEquals((short) 0xc000, minV);
      assertEquals((short) 0x4500, maxV);
    }
  }

  private static void verifyDoubleColumnIndexBounds(Scenario scenario, ByteBuffer min, ByteBuffer max) {
    long minV = min.getLong(0);
    long maxV = max.getLong(0);
    if (scenario == Scenario.ALL_NAN) {
      assertEquals(0x7ff0000000000001L, minV);
      assertEquals(0x7fffffffffffffffL, maxV);
    } else {
      assertEquals(Double.doubleToRawLongBits(-2d), minV);
      assertEquals(Double.doubleToRawLongBits(5d), maxV);
    }
  }

  private static void verifyFloatColumnIndexBounds(Scenario scenario, ByteBuffer min, ByteBuffer max) {
    int minV = min.getInt(0);
    int maxV = max.getInt(0);
    if (scenario == Scenario.ALL_NAN) {
      assertEquals(0x7fc00001, minV);
      assertEquals(0x7fffffff, maxV);
    } else {
      assertEquals(Float.floatToRawIntBits(-2f), minV);
      assertEquals(Float.floatToRawIntBits(5f), maxV);
    }
  }

  private static boolean isIeee(String columnName) {
    return columnName.endsWith("ieee754");
  }

  private static void assertFloatValue(float expected, float actual) {
    if (Float.isNaN(expected)) {
      assertTrue(Float.isNaN(actual));
    } else {
      assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(actual));
    }
  }

  private static void assertDoubleValue(double expected, double actual) {
    if (Double.isNaN(expected)) {
      assertTrue(Double.isNaN(actual));
    } else {
      assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(actual));
    }
  }

  private static List<Float> concatFloatValues(Scenario... scenarios) {
    List<Float> values = new ArrayList<>();
    for (Scenario scenario : scenarios) {
      for (float value : FloatingPointNanInteropFileGenerator.floatValues(scenario)) {
        values.add(value);
      }
    }
    return values;
  }

  private static List<Double> concatDoubleValues(Scenario... scenarios) {
    List<Double> values = new ArrayList<>();
    for (Scenario scenario : scenarios) {
      for (double value : FloatingPointNanInteropFileGenerator.doubleValues(scenario)) {
        values.add(value);
      }
    }
    return values;
  }

  private static List<Binary> concatFloat16Values(Scenario... scenarios) {
    List<Binary> values = new ArrayList<>();
    for (Scenario scenario : scenarios) {
      for (Binary value : FloatingPointNanInteropFileGenerator.float16Values(scenario)) {
        values.add(value);
      }
    }
    return values;
  }
}
