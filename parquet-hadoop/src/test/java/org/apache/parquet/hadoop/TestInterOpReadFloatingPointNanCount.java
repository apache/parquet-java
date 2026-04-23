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
    verifyFile(generated.getZeroMinFile(), conf, Scenario.ZERO_MIN);
    verifyFile(generated.getZeroMaxFile(), conf, Scenario.ZERO_MAX);
    verifyFile(
        generated.getMergedFile(),
        conf,
        Scenario.NO_NAN,
        Scenario.MIXED_NAN,
        Scenario.ALL_NAN,
        Scenario.ZERO_MIN,
        Scenario.ZERO_MAX);
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
      assertEquals(4L, stats.getNanCount());
      if (isIeee(name)) {
        assertEquals(-2f, stats.getMin(), 0f);
        assertEquals(3f, stats.getMax(), 0f);
      }
      return;
    }

    if (scenario == Scenario.ALL_NAN) {
      assertEquals(10L, stats.getNanCount());
      if (isIeee(name)) {
        assertEquals(0xffffffff, Float.floatToRawIntBits(stats.getMin()));
        assertEquals(0x7fffffff, Float.floatToRawIntBits(stats.getMax()));
      }
      return;
    }

    if (scenario == Scenario.ZERO_MIN) {
      assertEquals(0L, stats.getNanCount());
      assertEquals(isIeee(name) ? 0x00000000 : 0x80000000, Float.floatToRawIntBits(stats.getMin()));
      assertEquals(5f, stats.getMax(), 0f);
      return;
    }

    assertEquals(0L, stats.getNanCount());
    assertEquals(-5f, stats.getMin(), 0f);
    assertEquals(isIeee(name) ? 0x80000000 : 0x00000000, Float.floatToRawIntBits(stats.getMax()));
  }

  private static void verifyDoubleStats(Scenario scenario, String name, DoubleStatistics stats) {
    if (scenario == Scenario.NO_NAN) {
      assertEquals(0L, stats.getNanCount());
      assertEquals(-2d, stats.getMin(), 0d);
      assertEquals(5d, stats.getMax(), 0d);
      return;
    }

    if (scenario == Scenario.MIXED_NAN) {
      assertEquals(4L, stats.getNanCount());
      if (isIeee(name)) {
        assertEquals(-2d, stats.getMin(), 0d);
        assertEquals(3d, stats.getMax(), 0d);
      }
      return;
    }

    if (scenario == Scenario.ALL_NAN) {
      assertEquals(10L, stats.getNanCount());
      if (isIeee(name)) {
        assertEquals(0xffffffffffffffffL, Double.doubleToRawLongBits(stats.getMin()));
        assertEquals(0x7fffffffffffffffL, Double.doubleToRawLongBits(stats.getMax()));
      }
      return;
    }

    if (scenario == Scenario.ZERO_MIN) {
      assertEquals(0L, stats.getNanCount());
      assertEquals(
          isIeee(name) ? 0x0000000000000000L : 0x8000000000000000L,
          Double.doubleToRawLongBits(stats.getMin()));
      assertEquals(5d, stats.getMax(), 0d);
      return;
    }

    assertEquals(0L, stats.getNanCount());
    assertEquals(-5d, stats.getMin(), 0d);
    assertEquals(
        isIeee(name) ? 0x8000000000000000L : 0x0000000000000000L, Double.doubleToRawLongBits(stats.getMax()));
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
      assertEquals(4L, stats.getNanCount());
      if (isIeee(name)) {
        short min = stats.genericGetMin().get2BytesLittleEndian();
        short max = stats.genericGetMax().get2BytesLittleEndian();
        assertEquals((short) 0xc000, min);
        assertEquals((short) 0x4200, max);
      }
      return;
    }

    if (scenario == Scenario.ALL_NAN) {
      assertEquals(10L, stats.getNanCount());
      if (isIeee(name)) {
        short min = stats.genericGetMin().get2BytesLittleEndian();
        short max = stats.genericGetMax().get2BytesLittleEndian();
        assertEquals((short) 0xffff, min);
        assertEquals((short) 0x7fff, max);
      }
      return;
    }

    if (scenario == Scenario.ZERO_MIN) {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertEquals(0L, stats.getNanCount());
      assertEquals((short) (isIeee(name) ? 0x0000 : 0x8000), min);
      assertEquals((short) 0x4500, max);
      return;
    }

    assertEquals(0L, stats.getNanCount());
    if (isIeee(name)) {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertEquals((short) 0xc500, min);
      assertEquals((short) 0x8000, max);
    } else {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertEquals((short) 0xc500, min);
      assertEquals((short) 0x0000, max);
    }
  }

  private static void verifyColumnIndex(ParquetFileReader reader, Scenario scenario, ColumnChunkMetaData column)
      throws IOException {
    String name = column.getPath().toDotString();
    boolean ieee = isIeee(name);
    boolean hasNaN = scenario == Scenario.MIXED_NAN || scenario == Scenario.ALL_NAN;

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
      verifyFloat16ColumnIndexBounds(scenario, ieee, min, max);
      return;
    }

    if (name.contains("double")) {
      verifyDoubleColumnIndexBounds(scenario, ieee, min, max);
      return;
    }

    verifyFloatColumnIndexBounds(scenario, ieee, min, max);
  }

  private static long expectedNanCount(Scenario scenario) {
    if (scenario == Scenario.MIXED_NAN) {
      return 4L;
    }
    if (scenario == Scenario.ALL_NAN) {
      return 10L;
    }
    return 0L;
  }

  private static void verifyFloat16ColumnIndexBounds(
      Scenario scenario, boolean ieee, ByteBuffer min, ByteBuffer max) {
    short minV = min.getShort(0);
    short maxV = max.getShort(0);
    if (scenario == Scenario.ALL_NAN) {
      assertEquals((short) 0xffff, minV);
      assertEquals((short) 0x7fff, maxV);
    } else if (scenario == Scenario.MIXED_NAN || scenario == Scenario.NO_NAN) {
      assertEquals((short) 0xc000, minV);
      assertEquals((short) (scenario == Scenario.MIXED_NAN ? 0x4200 : 0x4500), maxV);
    } else if (scenario == Scenario.ZERO_MIN) {
      assertEquals((short) (ieee ? 0x0000 : 0x8000), minV);
      assertEquals((short) 0x4500, maxV);
    } else {
      assertEquals((short) 0xc500, minV);
      assertEquals((short) (ieee ? 0x8000 : 0x0000), maxV);
    }
  }

  private static void verifyDoubleColumnIndexBounds(Scenario scenario, boolean ieee, ByteBuffer min, ByteBuffer max) {
    long minV = min.getLong(0);
    long maxV = max.getLong(0);
    if (scenario == Scenario.ALL_NAN) {
      assertEquals(0xffffffffffffffffL, minV);
      assertEquals(0x7fffffffffffffffL, maxV);
    } else if (scenario == Scenario.MIXED_NAN || scenario == Scenario.NO_NAN) {
      assertEquals(Double.doubleToRawLongBits(-2d), minV);
      assertEquals(Double.doubleToRawLongBits(scenario == Scenario.MIXED_NAN ? 3d : 5d), maxV);
    } else if (scenario == Scenario.ZERO_MIN) {
      assertEquals(Double.doubleToRawLongBits(ieee ? 0d : -0d), minV);
      assertEquals(Double.doubleToRawLongBits(5d), maxV);
    } else {
      assertEquals(Double.doubleToRawLongBits(-5d), minV);
      assertEquals(Double.doubleToRawLongBits(ieee ? -0d : 0d), maxV);
    }
  }

  private static void verifyFloatColumnIndexBounds(Scenario scenario, boolean ieee, ByteBuffer min, ByteBuffer max) {
    int minV = min.getInt(0);
    int maxV = max.getInt(0);
    if (scenario == Scenario.ALL_NAN) {
      assertEquals(0xffffffff, minV);
      assertEquals(0x7fffffff, maxV);
    } else if (scenario == Scenario.MIXED_NAN || scenario == Scenario.NO_NAN) {
      assertEquals(Float.floatToRawIntBits(-2f), minV);
      assertEquals(Float.floatToRawIntBits(scenario == Scenario.MIXED_NAN ? 3f : 5f), maxV);
    } else if (scenario == Scenario.ZERO_MIN) {
      assertEquals(Float.floatToRawIntBits(ieee ? 0f : -0f), minV);
      assertEquals(Float.floatToRawIntBits(5f), maxV);
    } else {
      assertEquals(Float.floatToRawIntBits(-5f), minV);
      assertEquals(Float.floatToRawIntBits(ieee ? -0f : 0f), maxV);
    }
  }

  private static boolean isIeee(String columnName) {
    return columnName.endsWith("ieee754");
  }

  private static void assertFloatValue(float expected, float actual) {
    assertEquals(Float.floatToRawIntBits(expected), Float.floatToRawIntBits(actual));
  }

  private static void assertDoubleValue(double expected, double actual) {
    assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(actual));
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
