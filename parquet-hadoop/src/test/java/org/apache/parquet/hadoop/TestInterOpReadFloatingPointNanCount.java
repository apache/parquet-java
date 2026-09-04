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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestInterOpReadFloatingPointNanCount {

  // Canonical parquet-testing fixture exercising the same scenarios as the local generator, as a
  // single file with five row groups. parquet-java's writer produces the same statistics and
  // column index layout, so the reference file is verified with the exact same expectations as the
  // locally generated merged file.
  private static final String REFERENCE_FILE = "floating_orders_nan_count.parquet";
  private static final String REFERENCE_CHANGESET = "ffdcbb5e22828186c7461e56dbd26a0fe3caee56";

  private final InterOpTester interop = new InterOpTester();

  @TempDir
  private java.nio.file.Path tempDir;

  @Test
  public void testReadStatsAndColumnIndex() throws IOException {
    Configuration conf = new Configuration();
    GenerationResult generated = FloatingPointNanInteropFileGenerator.generateAndMerge(
        conf, new Path(Files.createTempDirectory(tempDir, "folder").toUri()));

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

  /**
   * Reads the canonical {@code floating_orders_nan_count.parquet} from the parquet-testing repo and
   * verifies it against the same expectations as the locally generated files, confirming that
   * parquet-java reads the reference file (produced by another writer) consistently.
   */
  @Test
  public void testReadReferenceFile() throws IOException {
    Configuration conf = new Configuration();
    Path file = interop.GetInterOpFile(REFERENCE_FILE, REFERENCE_CHANGESET);
    verifyFile(
        file,
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
      assertThat(blocks).hasSameSizeAs(scenarios);

      // Verify read values
      int rowId = 0;
      Group group;
      while ((group = recordReader.read()) != null) {
        assertFloatValue(expectFloats.get(rowId), group.getFloat("float_ieee754", 0));
        assertFloatValue(expectFloats.get(rowId), group.getFloat("float_typedef", 0));

        assertDoubleValue(expectDoubles.get(rowId), group.getDouble("double_ieee754", 0));
        assertDoubleValue(expectDoubles.get(rowId), group.getDouble("double_typedef", 0));

        assertThat(group.getBinary("float16_ieee754", 0)).isEqualTo(expectFloat16s.get(rowId));
        assertThat(group.getBinary("float16_typedef", 0)).isEqualTo(expectFloat16s.get(rowId));
        rowId++;
      }
      assertThat(rowId).isEqualTo(10 * scenarios.length);

      // Verify stats
      for (int rg = 0; rg < blocks.size(); rg++) {
        BlockMetaData block = blocks.get(rg);
        assertThat(block.getRowCount()).isEqualTo(10L);
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
    assertThat(stats.isNanCountSet()).isTrue();

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
      assertThat(stats.getNanCount()).isEqualTo(0L);
      assertThat(stats.getMin()).isEqualTo(-2f);
      assertThat(stats.getMax()).isEqualTo(5f);
      return;
    }

    if (scenario == Scenario.MIXED_NAN) {
      assertThat(stats.getNanCount()).isEqualTo(4L);
      if (isIeee(name)) {
        assertThat(stats.getMin()).isEqualTo(-2f);
        assertThat(stats.getMax()).isEqualTo(3f);
      }
      return;
    }

    if (scenario == Scenario.ALL_NAN) {
      assertThat(stats.getNanCount()).isEqualTo(10L);
      if (isIeee(name)) {
        assertThat(Float.floatToRawIntBits(stats.getMin())).isEqualTo(0xffffffff);
        assertThat(Float.floatToRawIntBits(stats.getMax())).isEqualTo(0x7fffffff);
      }
      return;
    }

    if (scenario == Scenario.ZERO_MIN) {
      assertThat(stats.getNanCount()).isEqualTo(0L);
      assertThat(Float.floatToRawIntBits(stats.getMin())).isEqualTo(isIeee(name) ? 0x00000000 : 0x80000000);
      assertThat(stats.getMax()).isEqualTo(5f);
      return;
    }

    assertThat(stats.getNanCount()).isEqualTo(0L);
    assertThat(stats.getMin()).isEqualTo(-5f);
    assertThat(Float.floatToRawIntBits(stats.getMax())).isEqualTo(isIeee(name) ? 0x80000000 : 0x00000000);
  }

  private static void verifyDoubleStats(Scenario scenario, String name, DoubleStatistics stats) {
    if (scenario == Scenario.NO_NAN) {
      assertThat(stats.getNanCount()).isEqualTo(0L);
      assertThat(stats.getMin()).isEqualTo(-2d);
      assertThat(stats.getMax()).isEqualTo(5d);
      return;
    }

    if (scenario == Scenario.MIXED_NAN) {
      assertThat(stats.getNanCount()).isEqualTo(4L);
      if (isIeee(name)) {
        assertThat(stats.getMin()).isEqualTo(-2d);
        assertThat(stats.getMax()).isEqualTo(3d);
      }
      return;
    }

    if (scenario == Scenario.ALL_NAN) {
      assertThat(stats.getNanCount()).isEqualTo(10L);
      if (isIeee(name)) {
        assertThat(Double.doubleToRawLongBits(stats.getMin())).isEqualTo(0xffffffffffffffffL);
        assertThat(Double.doubleToRawLongBits(stats.getMax())).isEqualTo(0x7fffffffffffffffL);
      }
      return;
    }

    if (scenario == Scenario.ZERO_MIN) {
      assertThat(stats.getNanCount()).isEqualTo(0L);
      assertThat(Double.doubleToRawLongBits(stats.getMin()))
          .isEqualTo(isIeee(name) ? 0x0000000000000000L : 0x8000000000000000L);
      assertThat(stats.getMax()).isEqualTo(5d);
      return;
    }

    assertThat(stats.getNanCount()).isEqualTo(0L);
    assertThat(stats.getMin()).isEqualTo(-5d);
    assertThat(Double.doubleToRawLongBits(stats.getMax()))
        .isEqualTo(isIeee(name) ? 0x8000000000000000L : 0x0000000000000000L);
  }

  private static void verifyFloat16Stats(Scenario scenario, String name, BinaryStatistics stats) {
    if (scenario == Scenario.NO_NAN) {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertThat(stats.getNanCount()).isEqualTo(0L);
      assertThat(min).isEqualTo((short) 0xc000);
      assertThat(max).isEqualTo((short) 0x4500);
      return;
    }

    if (scenario == Scenario.MIXED_NAN) {
      assertThat(stats.getNanCount()).isEqualTo(4L);
      if (isIeee(name)) {
        short min = stats.genericGetMin().get2BytesLittleEndian();
        short max = stats.genericGetMax().get2BytesLittleEndian();
        assertThat(min).isEqualTo((short) 0xc000);
        assertThat(max).isEqualTo((short) 0x4200);
      }
      return;
    }

    if (scenario == Scenario.ALL_NAN) {
      assertThat(stats.getNanCount()).isEqualTo(10L);
      if (isIeee(name)) {
        short min = stats.genericGetMin().get2BytesLittleEndian();
        short max = stats.genericGetMax().get2BytesLittleEndian();
        assertThat(min).isEqualTo((short) 0xffff);
        assertThat(max).isEqualTo((short) 0x7fff);
      }
      return;
    }

    if (scenario == Scenario.ZERO_MIN) {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertThat(stats.getNanCount()).isEqualTo(0L);
      assertThat(min).isEqualTo((short) (isIeee(name) ? 0x0000 : 0x8000));
      assertThat(max).isEqualTo((short) 0x4500);
      return;
    }

    assertThat(stats.getNanCount()).isEqualTo(0L);
    if (isIeee(name)) {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertThat(min).isEqualTo((short) 0xc500);
      assertThat(max).isEqualTo((short) 0x8000);
    } else {
      short min = stats.genericGetMin().get2BytesLittleEndian();
      short max = stats.genericGetMax().get2BytesLittleEndian();
      assertThat(min).isEqualTo((short) 0xc500);
      assertThat(max).isEqualTo((short) 0x0000);
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
      assertThat(columnIndex).isNull();
      return;
    }

    assertThat(columnIndex).isNotNull();
    assertThat(columnIndex.getNanCounts()).isNotNull();
    assertThat(columnIndex.getNanCounts()).hasSize(1);
    assertThat((long) columnIndex.getNanCounts().get(0)).isEqualTo(expectedNanCount(scenario));

    assertThat(columnIndex.getMinValues()).hasSize(1);
    assertThat(columnIndex.getMaxValues()).hasSize(1);
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
      assertThat(minV).isEqualTo((short) 0xffff);
      assertThat(maxV).isEqualTo((short) 0x7fff);
    } else if (scenario == Scenario.MIXED_NAN || scenario == Scenario.NO_NAN) {
      assertThat(minV).isEqualTo((short) 0xc000);
      assertThat(maxV).isEqualTo((short) (scenario == Scenario.MIXED_NAN ? 0x4200 : 0x4500));
    } else if (scenario == Scenario.ZERO_MIN) {
      assertThat(minV).isEqualTo((short) (ieee ? 0x0000 : 0x8000));
      assertThat(maxV).isEqualTo((short) 0x4500);
    } else {
      assertThat(minV).isEqualTo((short) 0xc500);
      assertThat(maxV).isEqualTo((short) (ieee ? 0x8000 : 0x0000));
    }
  }

  private static void verifyDoubleColumnIndexBounds(Scenario scenario, boolean ieee, ByteBuffer min, ByteBuffer max) {
    long minV = min.getLong(0);
    long maxV = max.getLong(0);
    if (scenario == Scenario.ALL_NAN) {
      assertThat(minV).isEqualTo(0xffffffffffffffffL);
      assertThat(maxV).isEqualTo(0x7fffffffffffffffL);
    } else if (scenario == Scenario.MIXED_NAN || scenario == Scenario.NO_NAN) {
      assertThat(minV).isEqualTo(Double.doubleToRawLongBits(-2d));
      assertThat(maxV).isEqualTo(Double.doubleToRawLongBits(scenario == Scenario.MIXED_NAN ? 3d : 5d));
    } else if (scenario == Scenario.ZERO_MIN) {
      assertThat(minV).isEqualTo(Double.doubleToRawLongBits(ieee ? 0d : -0d));
      assertThat(maxV).isEqualTo(Double.doubleToRawLongBits(5d));
    } else {
      assertThat(minV).isEqualTo(Double.doubleToRawLongBits(-5d));
      assertThat(maxV).isEqualTo(Double.doubleToRawLongBits(ieee ? -0d : 0d));
    }
  }

  private static void verifyFloatColumnIndexBounds(Scenario scenario, boolean ieee, ByteBuffer min, ByteBuffer max) {
    int minV = min.getInt(0);
    int maxV = max.getInt(0);
    if (scenario == Scenario.ALL_NAN) {
      assertThat(minV).isEqualTo(0xffffffff);
      assertThat(maxV).isEqualTo(0x7fffffff);
    } else if (scenario == Scenario.MIXED_NAN || scenario == Scenario.NO_NAN) {
      assertThat(minV).isEqualTo(Float.floatToRawIntBits(-2f));
      assertThat(maxV).isEqualTo(Float.floatToRawIntBits(scenario == Scenario.MIXED_NAN ? 3f : 5f));
    } else if (scenario == Scenario.ZERO_MIN) {
      assertThat(minV).isEqualTo(Float.floatToRawIntBits(ieee ? 0f : -0f));
      assertThat(maxV).isEqualTo(Float.floatToRawIntBits(5f));
    } else {
      assertThat(minV).isEqualTo(Float.floatToRawIntBits(-5f));
      assertThat(maxV).isEqualTo(Float.floatToRawIntBits(ieee ? -0f : 0f));
    }
  }

  private static boolean isIeee(String columnName) {
    return columnName.endsWith("ieee754");
  }

  private static void assertFloatValue(float expected, float actual) {
    assertThat(Float.floatToRawIntBits(actual)).isEqualTo(Float.floatToRawIntBits(expected));
  }

  private static void assertDoubleValue(double expected, double actual) {
    assertThat(Double.doubleToRawLongBits(actual)).isEqualTo(Double.doubleToRawLongBits(expected));
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
