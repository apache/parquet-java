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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.values.alp.AlpConfig;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-compatibility tests for ALP (Adaptive Lossless floating-Point) encoding.
 *
 * <p>Reads the ALP conformance file from parquet-testing and checks that every ALP column decodes
 * bit-identically to its PLAIN counterpart, and round-trips Java-written ALP files.
 */
public class TestInterOpReadAlp {
  private static final Logger LOG = LoggerFactory.getLogger(TestInterOpReadAlp.class);

  @TempDir
  java.nio.file.Path temp;

  /** Mirrors JUnit 4's {@code TemporaryFolder#newFolder()}: a fresh directory per call. */
  private File newFolder() throws IOException {
    return Files.createTempDirectory(temp, "junit").toFile();
  }

  private static final String ALP_EXTENDED_FILE = "alp_extended.zstd.parquet";
  private static final String CHANGESET = "09f3cdb";
  private final InterOpTester interop = new InterOpTester();

  private static final int EXTENDED_ROWS = 9032;
  private static final int EXTENDED_ROW_GROUPS = 5;
  private static final int EXTENDED_NULLS = 8;

  /** ALP column -> PLAIN column holding the same values. */
  private static final Map<String, String> ALP_TO_PLAIN = new LinkedHashMap<>();

  static {
    ALP_TO_PLAIN.put("float_alp_32", "float_plain");
    ALP_TO_PLAIN.put("float_alp_1024", "float_plain");
    ALP_TO_PLAIN.put("float_alp_4096", "float_plain");
    ALP_TO_PLAIN.put("double_alp_32", "double_plain");
    ALP_TO_PLAIN.put("double_alp_1024", "double_plain");
    ALP_TO_PLAIN.put("double_alp_4096", "double_plain");
  }

  private java.nio.file.Path getExtendedFile() throws IOException {
    return Paths.get(interop.GetInterOpFile(ALP_EXTENDED_FILE, CHANGESET).toString());
  }

  /** Read all rows from a parquet file using LocalInputFile (no Hadoop FileSystem). */
  private List<Group> readAllRows(java.nio.file.Path filePath) throws IOException {
    List<Group> rows = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(filePath))) {
      ParquetMetadata footer = reader.getFooter();
      MessageType schema = footer.getFileMetaData().getSchema();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        long rowCount = pages.getRowCount();
        RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
        for (long i = 0; i < rowCount; i++) {
          rows.add(recordReader.read());
        }
      }
    }
    return rows;
  }

  private static boolean isNull(Group row, String field) {
    return row.getFieldRepetitionCount(field) == 0;
  }

  /** Raw IEEE 754 bits of a float or double field, widened to a long for exact comparison. */
  private static long rawBits(Group row, String field, boolean isFloat) {
    return isFloat
        ? Float.floatToRawIntBits(row.getFloat(field, 0)) & 0xFFFFFFFFL
        : Double.doubleToRawLongBits(row.getDouble(field, 0));
  }

  @Test
  public void testAlpColumnsMatchPlainColumns() throws IOException {
    List<Group> rows = readAllRows(getExtendedFile());
    assertThat(rows.size()).as("row count").isEqualTo(EXTENDED_ROWS);

    for (Map.Entry<String, String> entry : ALP_TO_PLAIN.entrySet()) {
      String alpCol = entry.getKey();
      String plainCol = entry.getValue();
      boolean isFloat = alpCol.startsWith("float");
      int nulls = 0;
      for (int i = 0; i < rows.size(); i++) {
        Group row = rows.get(i);
        boolean alpNull = isNull(row, alpCol);
        assertThat(alpNull).as("nullness of " + alpCol + " at row " + i).isEqualTo(isNull(row, plainCol));
        if (alpNull) {
          nulls++;
          continue;
        }
        long alpBits = rawBits(row, alpCol, isFloat);
        long plainBits = rawBits(row, plainCol, isFloat);
        assertThat(alpBits)
            .as(String.format(
                "%s row %d: bits 0x%x, %s bits 0x%x", alpCol, i, alpBits, plainCol, plainBits))
            .isEqualTo(plainBits);
      }
      assertThat(nulls).as(alpCol + " nulls").isEqualTo(EXTENDED_NULLS);
    }
  }

  @Test
  public void testAlpColumnsAreAlpEncoded() throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(getExtendedFile()))) {
      ParquetMetadata footer = reader.getFooter();
      assertThat(footer.getBlocks().size()).as("row groups").isEqualTo(EXTENDED_ROW_GROUPS);
      int alpChunks = 0;
      for (org.apache.parquet.hadoop.metadata.BlockMetaData block : footer.getBlocks()) {
        for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData chunk : block.getColumns()) {
          String path = chunk.getPath().toDotString();
          boolean hasAlp = chunk.getEncodings().contains(Encoding.ALP);
          assertThat(hasAlp)
              .as("ALP on " + path + ": " + chunk.getEncodings())
              .isEqualTo(ALP_TO_PLAIN.containsKey(path));
          if (hasAlp) {
            alpChunks++;
          }
        }
      }
      assertThat(alpChunks).as("ALP column chunks").isEqualTo(ALP_TO_PLAIN.size() * EXTENDED_ROW_GROUPS);
    }
  }

  /**
   * Checks exact bit patterns at known rows, so a fault that corrupted the ALP and PLAIN paths the
   * same way would still be caught. NaN payloads must survive, since exceptions are stored verbatim.
   */
  @Test
  public void testSpecialValueBitPatterns() throws IOException {
    List<Group> rows = readAllRows(getExtendedFile());
    assertBits(rows, 1024, 0x7FF8000000000000L, 0x7FC00000L, "canonical NaN");
    assertBits(rows, 1500, 0x7FF800DEADBEEF00L, 0x7FC0DEADL, "NaN with payload");
    assertBits(rows, 2047, 0xFFF8000000000001L, 0xFFC00001L, "negative NaN with payload");
    assertBits(rows, 2000, 0x7FF0000000000000L, 0x7F800000L, "+Inf");
    assertBits(rows, 2001, 0xFFF0000000000000L, 0xFF800000L, "-Inf");
    assertBits(rows, 2002, 0x8000000000000000L, 0x80000000L, "-0.0");
    assertBits(rows, 2003, 0x1L, 0x1L, "smallest subnormal");
    assertBits(rows, 2500, 0x400921FB54442D18L, 0x40490FDBL, "pi");
    assertBits(rows, 7168, 0x401F147AE147AE14L, 0x40F8A3D7L, "constant 7.77");
    assertBits(rows, 9000, 0xC3DBC16D674EC800L, 0xDEDE0B6BL, "-8e18");
    assertBits(rows, 9001, 0x43DBC16D674EC800L, 0x5EDE0B6BL, "8e18");
    for (int row : new int[] {8200, 8900}) {
      for (String col : ALP_TO_PLAIN.keySet()) {
        assertThat(isNull(rows.get(row), col))
            .as(col + " null at row " + row)
            .isTrue();
      }
    }
  }

  private void assertBits(List<Group> rows, int row, long doubleBits, long floatBits, String what) {
    for (String col : ALP_TO_PLAIN.keySet()) {
      boolean isFloat = col.startsWith("float");
      assertThat(rawBits(rows.get(row), col, isFloat))
          .as(what + " at row " + row + " in " + col)
          .isEqualTo(isFloat ? floatBits : doubleBits);
    }
  }

  private static final String ALP_SCHEMA =
      "message alp_interop { " + "required double double_col; " + "required float float_col; " + "}";

  private static final double[] DOUBLE_VALUES = {
    1.23, 4.56, 7.89, 0.001, 1000.0, -3.14, 2.718281828, 9.99999, 0.123456789, 100.5
  };
  private static final float[] FLOAT_VALUES = {
    1.23f, 4.56f, 7.89f, 0.001f, 1000.0f, -3.14f, 2.718f, 9.999f, 0.1234f, 100.5f
  };

  /**
   * Write an ALP-encoded file from Java using the given page version, then read it back and verify
   * all double and float values round-trip exactly.
   */
  private void writeAndVerifyAlpFile(WriterVersion version) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(ALP_SCHEMA);
    java.nio.file.Path outPath =
        newFolder().toPath().resolve("alp_java_" + version.name().toLowerCase() + ".parquet");

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(outPath))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriterVersion(version)
        .withAlp()
        .withDictionaryEncoding(false)
        .withConf(new Configuration())
        .build()) {
      for (int i = 0; i < DOUBLE_VALUES.length; i++) {
        SimpleGroup row = new SimpleGroup(schema);
        row.add("double_col", DOUBLE_VALUES[i]);
        row.add("float_col", FLOAT_VALUES[i]);
        writer.write(row);
      }
    }

    List<Group> rows = readAllRows(outPath);
    assertThat(rows.size()).as("Row count mismatch for " + version).isEqualTo(DOUBLE_VALUES.length);
    for (int i = 0; i < DOUBLE_VALUES.length; i++) {
      assertThat(rows.get(i).getDouble("double_col", 0))
          .as("double_col mismatch at row " + i + " for " + version)
          .isEqualTo(DOUBLE_VALUES[i]);
      assertThat(rows.get(i).getFloat("float_col", 0))
          .as("float_col mismatch at row " + i + " for " + version)
          .isEqualTo(FLOAT_VALUES[i]);
    }
    LOG.info(
        "writeAndVerifyAlpFile [{}]: wrote and read back {} rows from {}",
        version,
        rows.size(),
        outPath.getFileName());
  }

  /**
   * Java writes ALP-encoded floats/doubles using V1 (PARQUET_1_0) data pages and reads them back.
   * Verifies the Java write path produces a valid file readable by this implementation.
   */
  @Test
  public void testJavaWriteAlpV1Pages() throws IOException {
    writeAndVerifyAlpFile(WriterVersion.PARQUET_1_0);
  }

  /**
   * Writes >4096 rows with vectorSize=4096 so multiple full vectors are flushed, then verifies
   * the file round-trips exactly. The reader pulls log_vector_size from the on-disk header to
   * size its unpacking window, so a wrong header byte would surface as decode garbage —
   * round-trip equality is sufficient proof that the configured vector size took effect.
   */
  @Test
  public void testJavaWriteAlpCustomVectorSize() throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(ALP_SCHEMA);
    int rowCount = 4500; // crosses one full vector + partial tail at vectorSize=4096
    double[] doubles = new double[rowCount];
    float[] floats = new float[rowCount];
    // 2-decimal sensor-like data — the ALP sweet spot, so few/no exceptions
    for (int i = 0; i < rowCount; i++) {
      doubles[i] = (i * 13L % 100000) / 100.0;
      floats[i] = (float) ((i * 7L % 10000) / 100.0);
    }

    java.nio.file.Path outPath = newFolder().toPath().resolve("alp_java_vs4096.parquet");

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(outPath))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withAlp(new AlpConfig(4096))
        .withDictionaryEncoding(false)
        .withConf(new Configuration())
        .build()) {
      for (int i = 0; i < rowCount; i++) {
        SimpleGroup row = new SimpleGroup(schema);
        row.add("double_col", doubles[i]);
        row.add("float_col", floats[i]);
        writer.write(row);
      }
    }

    List<Group> rows = readAllRows(outPath);
    assertThat(rows.size()).as("Row count mismatch at vectorSize=4096").isEqualTo(rowCount);
    for (int i = 0; i < rowCount; i++) {
      assertThat(rows.get(i).getDouble("double_col", 0))
          .as("double_col mismatch at row " + i)
          .isEqualTo(doubles[i]);
      assertThat(rows.get(i).getFloat("float_col", 0))
          .as("float_col mismatch at row " + i)
          .isEqualTo(floats[i]);
    }
    LOG.info("testJavaWriteAlpCustomVectorSize: {} rows round-tripped at vectorSize=4096", rowCount);
  }

  /**
   * Java writes ALP-encoded floats/doubles using V2 (PARQUET_2_0) data pages and reads them back.
   */
  @Test
  public void testJavaWriteAlpV2Pages() throws IOException {
    writeAndVerifyAlpFile(WriterVersion.PARQUET_2_0);
  }

  // ---------------------------------------------------------------------------
  // Statistics correctness for ALP-encoded columns.
  //
  // Statistics are populated by the column writer wrapper (not the encoder),
  // so they should "just work" regardless of which encoding is in use — but
  // the assumption is worth pinning. Wrong/missing statistics break parquet
  // predicate pushdown: readers either skip row groups they shouldn't or scan
  // ones they could have skipped.
  // ---------------------------------------------------------------------------

  @Test
  public void testAlpColumnStatisticsAreCorrect() throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(ALP_SCHEMA);
    // Mix positive / negative / zero so min/max aren't trivially the endpoints
    double[] doubleVals = {1.23, -4.56, 7.89, 0.0, 1000.0, -3.14, 9.99, 0.001};
    float[] floatVals = {1.23f, -4.56f, 7.89f, 0.0f, 1000.0f, -3.14f, 9.99f, 0.001f};
    double expectedDoubleMin = -4.56, expectedDoubleMax = 1000.0;
    float expectedFloatMin = -4.56f, expectedFloatMax = 1000.0f;

    java.nio.file.Path outPath = newFolder().toPath().resolve("alp_stats.parquet");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(outPath))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withAlp()
        .withDictionaryEncoding(false)
        .withConf(new Configuration())
        .build()) {
      for (int i = 0; i < doubleVals.length; i++) {
        SimpleGroup row = new SimpleGroup(schema);
        row.add("double_col", doubleVals[i]);
        row.add("float_col", floatVals[i]);
        writer.write(row);
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(outPath))) {
      ParquetMetadata footer = reader.getFooter();
      assertThat(footer.getBlocks().size()).as("expected one row group").isEqualTo(1);
      org.apache.parquet.hadoop.metadata.BlockMetaData block =
          footer.getBlocks().get(0);

      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData doubleChunk = null;
      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData floatChunk = null;
      for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData c : block.getColumns()) {
        String path = c.getPath().toDotString();
        if (path.equals("double_col")) doubleChunk = c;
        if (path.equals("float_col")) floatChunk = c;
      }
      assertThat(doubleChunk.getEncodings().contains(Encoding.ALP))
          .as("double_col must be ALP-encoded")
          .isTrue();
      assertThat(floatChunk.getEncodings().contains(Encoding.ALP))
          .as("float_col must be ALP-encoded")
          .isTrue();

      org.apache.parquet.column.statistics.Statistics<?> dStats = doubleChunk.getStatistics();
      org.apache.parquet.column.statistics.Statistics<?> fStats = floatChunk.getStatistics();

      assertThat(dStats.hasNonNullValue())
          .as("double stats must have min/max set")
          .isTrue();
      assertThat(fStats.hasNonNullValue())
          .as("float stats must have min/max set")
          .isTrue();

      // Bit-exact: same encoder path produces same IEEE 754 representation;
      // statistics min/max should reflect the input data exactly.
      assertThat(Double.doubleToRawLongBits((Double) dStats.genericGetMin()))
          .as("double min mismatch")
          .isEqualTo(Double.doubleToRawLongBits(expectedDoubleMin));
      assertThat(Double.doubleToRawLongBits((Double) dStats.genericGetMax()))
          .as("double max mismatch")
          .isEqualTo(Double.doubleToRawLongBits(expectedDoubleMax));
      assertThat(Float.floatToRawIntBits((Float) fStats.genericGetMin()))
          .as("float min mismatch")
          .isEqualTo(Float.floatToRawIntBits(expectedFloatMin));
      assertThat(Float.floatToRawIntBits((Float) fStats.genericGetMax()))
          .as("float max mismatch")
          .isEqualTo(Float.floatToRawIntBits(expectedFloatMax));
      assertThat(dStats.getNumNulls()).as("double null count must be 0").isEqualTo(0L);
      assertThat(fStats.getNumNulls()).as("float null count must be 0").isEqualTo(0L);
    }
  }

  // ---------------------------------------------------------------------------
  // Integration coverage: the production paths normal ALP tests skip (dictionary
  // fallback, compression, adversarial statistics, repeated/nested columns).
  // ---------------------------------------------------------------------------

  private boolean columnUsesAlp(java.nio.file.Path file, String colDotPath) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
      for (org.apache.parquet.hadoop.metadata.BlockMetaData b :
          reader.getFooter().getBlocks()) {
        for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData c : b.getColumns()) {
          if ((colDotPath == null || c.getPath().toDotString().equals(colDotPath))
              && c.getEncodings().contains(Encoding.ALP)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Test
  public void testDictionaryOverflowFallsBackToAlp() throws IOException {
    // Dictionary ENABLED (the real default) with a small dictionary page, so a high-cardinality
    // double/float column overflows the dictionary and FALLS BACK to ALP mid-column. Normal ALP
    // tests disable the dictionary, so this exercises the untested production path where the
    // FallbackValuesWriter replays buffered values into ALP.
    MessageType schema = MessageTypeParser.parseMessageType("message m { required double d; required float f; }");
    int n = 20000;
    double[] ds = new double[n];
    float[] fs = new float[n];
    java.util.Random rng = new java.util.Random(3);
    for (int i = 0; i < n; i++) {
      ds[i] = Math.round(rng.nextDouble() * 1e9) / 100.0;
      fs[i] = (float) (Math.round(rng.nextFloat() * 1e6) / 100.0);
    }
    java.nio.file.Path outPath = newFolder().toPath().resolve("alp_dict_fallback.parquet");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(outPath))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withAlp()
        .withDictionaryEncoding(true)
        .withDictionaryPageSize(4096)
        .withConf(new Configuration())
        .build()) {
      for (int i = 0; i < n; i++) {
        SimpleGroup row = new SimpleGroup(schema);
        row.add("d", ds[i]);
        row.add("f", fs[i]);
        writer.write(row);
      }
    }
    List<Group> rows = readAllRows(outPath);
    assertThat(rows.size()).isEqualTo(n);
    for (int i = 0; i < n; i++) {
      assertThat(Double.doubleToRawLongBits(rows.get(i).getDouble("d", 0)))
          .as("d mismatch at " + i)
          .isEqualTo(Double.doubleToRawLongBits(ds[i]));
      assertThat(Float.floatToRawIntBits(rows.get(i).getFloat("f", 0)))
          .as("f mismatch at " + i)
          .isEqualTo(Float.floatToRawIntBits(fs[i]));
    }
    assertThat(columnUsesAlp(outPath, null))
        .as("Expected dictionary overflow to fall back to ALP encoding")
        .isTrue();
  }

  @Test
  public void testAlpUnderCompressionCodecs() throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required double d; required float f; }");
    int n = 5000;
    double[] ds = new double[n];
    float[] fs = new float[n];
    for (int i = 0; i < n; i++) {
      ds[i] = (i % 1000) / 100.0 - 5.0;
      fs[i] = (i % 777) / 100.0f - 3.0f;
    }
    for (CompressionCodecName codec : new CompressionCodecName[] {
      CompressionCodecName.SNAPPY, CompressionCodecName.GZIP, CompressionCodecName.ZSTD
    }) {
      java.nio.file.Path outPath = newFolder().toPath().resolve("alp_" + codec.name() + ".parquet");
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(outPath))
          .withType(schema)
          .withCompressionCodec(codec)
          .withWriterVersion(WriterVersion.PARQUET_2_0)
          .withAlp()
          .withDictionaryEncoding(false)
          .withConf(new Configuration())
          .build()) {
        for (int i = 0; i < n; i++) {
          SimpleGroup row = new SimpleGroup(schema);
          row.add("d", ds[i]);
          row.add("f", fs[i]);
          writer.write(row);
        }
      }
      List<Group> rows = readAllRows(outPath);
      assertThat(rows.size()).as("row count for " + codec).isEqualTo(n);
      for (int i = 0; i < n; i++) {
        assertThat(Double.doubleToRawLongBits(rows.get(i).getDouble("d", 0)))
            .as("d mismatch " + codec + " row " + i)
            .isEqualTo(Double.doubleToRawLongBits(ds[i]));
        assertThat(Float.floatToRawIntBits(rows.get(i).getFloat("f", 0)))
            .as("f mismatch " + codec + " row " + i)
            .isEqualTo(Float.floatToRawIntBits(fs[i]));
      }
      assertThat(columnUsesAlp(outPath, "d"))
          .as(codec + ": column should be ALP-encoded")
          .isTrue();
    }
  }

  @Test
  public void testAlpStatisticsExcludeNaNAndCountNulls() throws IOException {
    // Optional double column mixing finite values, NaN (must be excluded from min/max), and nulls
    // (must be counted). Combines def-level nulls (num_elements < rowCount) with ALP exceptions (NaN)
    // and verifies the footer statistics that predicate pushdown relies on are correct.
    MessageType schema = MessageTypeParser.parseMessageType("message m { optional double d; }");
    Double[] vals = {1.0, Double.NaN, -2.0, 5.0, null, 3.0, Double.NaN, null, 0.0};
    java.nio.file.Path outPath = newFolder().toPath().resolve("alp_stats_adv.parquet");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(outPath))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withAlp()
        .withDictionaryEncoding(false)
        .withConf(new Configuration())
        .build()) {
      for (Double v : vals) {
        SimpleGroup row = new SimpleGroup(schema);
        if (v != null) {
          row.add("d", v.doubleValue());
        }
        writer.write(row);
      }
    }
    // Values also round-trip (with NaN payload preserved and nulls in the right places).
    List<Group> rows = readAllRows(outPath);
    assertThat(rows.size()).isEqualTo(vals.length);
    for (int i = 0; i < vals.length; i++) {
      int rep = rows.get(i).getFieldRepetitionCount("d");
      if (vals[i] == null) {
        assertThat(rep).as("row " + i + " should be null").isEqualTo(0);
      } else {
        assertThat(rep).as("row " + i + " should be present").isEqualTo(1);
        assertThat(Double.doubleToRawLongBits(rows.get(i).getDouble("d", 0)))
            .as("value row " + i)
            .isEqualTo(Double.doubleToRawLongBits(vals[i]));
      }
    }
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(outPath))) {
      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData chunk =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);
      org.apache.parquet.column.statistics.Statistics<?> s = chunk.getStatistics();
      assertThat(s.getNumNulls()).as("null count").isEqualTo(2L);
      double min = (Double) s.genericGetMin();
      double max = (Double) s.genericGetMax();
      assertThat(!Double.isNaN(min)).as("min must not be NaN").isTrue();
      assertThat(!Double.isNaN(max)).as("max must not be NaN").isTrue();
      assertThat(min).as("min").isEqualTo(-2.0);
      assertThat(max).as("max").isEqualTo(5.0);
    }
  }

  @Test
  public void testAlpLargeScaleManyRowGroups() throws IOException {
    // Half a million rows with a small row-group size, forcing many row groups, exercised through
    // the full write/read pipeline (compression on). Verification is streaming (regenerate the
    // expected sequence, no List accumulation) so the read side is memory-safe at scale too.
    MessageType schema = MessageTypeParser.parseMessageType("message m { required double d; required float f; }");
    int n = 500_000;
    long seed = 5;
    java.nio.file.Path outPath = newFolder().toPath().resolve("alp_large.parquet");
    java.util.Random rng = new java.util.Random(seed);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(outPath))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withAlp()
        .withDictionaryEncoding(false)
        .withRowGroupSize(1L << 19) // 512KB, so 500k rows span many row groups
        .withConf(new Configuration())
        .build()) {
      for (int i = 0; i < n; i++) {
        SimpleGroup row = new SimpleGroup(schema);
        row.add("d", Math.round(rng.nextDouble() * 1e7) / 100.0);
        row.add("f", (float) (Math.round(rng.nextFloat() * 1e5) / 100.0));
        writer.write(row);
      }
    }
    java.util.Random verify = new java.util.Random(seed);
    int total = 0;
    int rowGroups = 0;
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(outPath))) {
      MessageType s = reader.getFooter().getFileMetaData().getSchema();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(s);
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        rowGroups++;
        long rc = pages.getRowCount();
        RecordReader<Group> rr = columnIO.getRecordReader(pages, new GroupRecordConverter(s));
        for (long i = 0; i < rc; i++) {
          Group g = rr.read();
          double ed = Math.round(verify.nextDouble() * 1e7) / 100.0;
          float ef = (float) (Math.round(verify.nextFloat() * 1e5) / 100.0);
          assertThat(Double.doubleToRawLongBits(g.getDouble("d", 0)))
              .as("d mismatch at row " + total)
              .isEqualTo(Double.doubleToRawLongBits(ed));
          assertThat(Float.floatToRawIntBits(g.getFloat("f", 0)))
              .as("f mismatch at row " + total)
              .isEqualTo(Float.floatToRawIntBits(ef));
          total++;
        }
      }
    }
    assertThat(total).as("row count").isEqualTo(n);
    assertThat(rowGroups >= 2)
        .as("expected multiple row groups, got " + rowGroups)
        .isTrue();
  }

  @Test
  public void testAlpOnRepeatedDoubleField() throws IOException {
    // ALP on a REPEATED double field, so it must interleave correctly with repetition/definition
    // levels (empty rows, varying counts). Nested/repeated columns are otherwise untested.
    MessageType schema =
        MessageTypeParser.parseMessageType("message m { required int64 id; repeated double vals; }");
    int nRows = 3000;
    java.util.Random rng = new java.util.Random(9);
    double[][] expected = new double[nRows][];
    for (int r = 0; r < nRows; r++) {
      int k = rng.nextInt(5); // 0..4 values per row (exercises empty rows + varying rep levels)
      expected[r] = new double[k];
      for (int j = 0; j < k; j++) {
        expected[r][j] = Math.round(rng.nextDouble() * 100000) / 100.0;
      }
    }
    java.nio.file.Path outPath = newFolder().toPath().resolve("alp_repeated.parquet");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(outPath))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withAlp()
        .withDictionaryEncoding(false)
        .withConf(new Configuration())
        .build()) {
      for (int r = 0; r < nRows; r++) {
        SimpleGroup row = new SimpleGroup(schema);
        row.add("id", (long) r);
        for (double v : expected[r]) {
          row.add("vals", v);
        }
        writer.write(row);
      }
    }
    List<Group> rows = readAllRows(outPath);
    assertThat(rows.size()).isEqualTo(nRows);
    for (int r = 0; r < nRows; r++) {
      Group g = rows.get(r);
      int k = g.getFieldRepetitionCount("vals");
      assertThat(k).as("rep count row " + r).isEqualTo(expected[r].length);
      for (int j = 0; j < k; j++) {
        assertThat(Double.doubleToRawLongBits(g.getDouble("vals", j)))
            .as("val row " + r + " idx " + j)
            .isEqualTo(Double.doubleToRawLongBits(expected[r][j]));
      }
    }
    assertThat(columnUsesAlp(outPath, "vals"))
        .as("repeated double column should be ALP-encoded")
        .isTrue();
  }
}
