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

import static org.apache.parquet.schema.LogicalTypeAnnotation.float16Type;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

public final class FloatingPointNanInteropFileGenerator {

  public static final String FILE_NO_NAN = "floating_orders_nan_count_no_nan.parquet";
  public static final String FILE_MIXED_NAN = "floating_orders_nan_count_mixed_nan.parquet";
  public static final String FILE_ALL_NAN = "floating_orders_nan_count_all_nan.parquet";
  public static final String FILE_ZERO_MIN = "floating_orders_nan_count_zero_min.parquet";
  public static final String FILE_ZERO_MAX = "floating_orders_nan_count_zero_max.parquet";
  public static final String FILE_MERGED = "floating_orders_nan_count_merged.parquet";

  public static final int ROWS_PER_FILE = 10;

  private static final float FLOAT_NEG_NAN_SMALL = Float.intBitsToFloat(0xffffffff);
  private static final float FLOAT_NEG_NAN_LARGE = Float.intBitsToFloat(0xfff00001);
  private static final float FLOAT_NAN_SMALL = Float.intBitsToFloat(0x7fc00001);
  private static final float FLOAT_NAN_LARGE = Float.intBitsToFloat(0x7fffffff);
  private static final double DOUBLE_NEG_NAN_SMALL = Double.longBitsToDouble(0xffffffffffffffffL);
  private static final double DOUBLE_NEG_NAN_LARGE = Double.longBitsToDouble(0xfff0000000000001L);
  private static final double DOUBLE_NAN_SMALL = Double.longBitsToDouble(0x7ff0000000000001L);
  private static final double DOUBLE_NAN_LARGE = Double.longBitsToDouble(0x7fffffffffffffffL);
  private static final Binary FLOAT16_NEG_NAN_SMALL = float16Binary((short) 0xffff);
  private static final Binary FLOAT16_NEG_NAN_LARGE = float16Binary((short) 0xfc01);
  private static final Binary FLOAT16_NAN_SMALL = float16Binary((short) 0x7c01);
  private static final Binary FLOAT16_NAN_LARGE = float16Binary((short) 0x7fff);

  private static final MessageType SCHEMA = Types.buildMessage()
      .required(FLOAT)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("float_ieee754")
      .required(FLOAT)
      .named("float_typedef")
      .required(DOUBLE)
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("double_ieee754")
      .required(DOUBLE)
      .named("double_typedef")
      .required(FIXED_LEN_BYTE_ARRAY)
      .length(2)
      .as(float16Type())
      .columnOrder(ColumnOrder.ieee754TotalOrder())
      .named("float16_ieee754")
      .required(FIXED_LEN_BYTE_ARRAY)
      .length(2)
      .as(float16Type())
      .named("float16_typedef")
      .named("msg");

  private static final float[] NO_NAN_FLOATS = new float[] {-2f, -1f, -0f, 0f, 0.5f, 1f, 2f, 3f, 4f, 5f};
  private static final double[] NO_NAN_DOUBLES = new double[] {-2d, -1d, -0d, 0d, 0.5d, 1d, 2d, 3d, 4d, 5d};
  private static final Binary[] NO_NAN_FLOAT16 = new Binary[] {
    float16Binary((short) 0xc000),
    float16Binary((short) 0xbc00),
    float16Binary((short) 0x8000),
    float16Binary((short) 0x0000),
    float16Binary((short) 0x3800),
    float16Binary((short) 0x3c00),
    float16Binary((short) 0x4000),
    float16Binary((short) 0x4200),
    float16Binary((short) 0x4400),
    float16Binary((short) 0x4500)
  };

  private static final float[] MIXED_NAN_FLOATS = new float[] {
    FLOAT_NEG_NAN_SMALL, -2f, FLOAT_NEG_NAN_LARGE, -1f, -0f, 0f, 1f, FLOAT_NAN_SMALL, 3f, FLOAT_NAN_LARGE
  };
  private static final double[] MIXED_NAN_DOUBLES = new double[] {
    DOUBLE_NEG_NAN_SMALL, -2d, DOUBLE_NEG_NAN_LARGE, -1d, -0d, 0d, 1d, DOUBLE_NAN_SMALL, 3d, DOUBLE_NAN_LARGE
  };
  private static final Binary[] MIXED_NAN_FLOAT16 = new Binary[] {
    FLOAT16_NEG_NAN_SMALL,
    float16Binary((short) 0xc000),
    FLOAT16_NEG_NAN_LARGE,
    float16Binary((short) 0xbc00),
    float16Binary((short) 0x8000),
    float16Binary((short) 0x0000),
    float16Binary((short) 0x3c00),
    FLOAT16_NAN_SMALL,
    float16Binary((short) 0x4200),
    FLOAT16_NAN_LARGE
  };

  private static final float[] ALL_NAN_FLOATS = new float[] {
    FLOAT_NEG_NAN_SMALL,
    FLOAT_NEG_NAN_LARGE,
    FLOAT_NAN_SMALL,
    FLOAT_NAN_LARGE,
    FLOAT_NEG_NAN_SMALL,
    FLOAT_NEG_NAN_LARGE,
    FLOAT_NAN_SMALL,
    FLOAT_NAN_LARGE,
    FLOAT_NEG_NAN_SMALL,
    FLOAT_NAN_LARGE
  };

  private static final double[] ALL_NAN_DOUBLES = new double[] {
    DOUBLE_NEG_NAN_SMALL,
    DOUBLE_NEG_NAN_LARGE,
    DOUBLE_NAN_SMALL,
    DOUBLE_NAN_LARGE,
    DOUBLE_NEG_NAN_SMALL,
    DOUBLE_NEG_NAN_LARGE,
    DOUBLE_NAN_SMALL,
    DOUBLE_NAN_LARGE,
    DOUBLE_NEG_NAN_SMALL,
    DOUBLE_NAN_LARGE
  };

  private static final Binary[] ALL_NAN_FLOAT16 = new Binary[] {
    FLOAT16_NEG_NAN_SMALL,
    FLOAT16_NEG_NAN_LARGE,
    FLOAT16_NAN_SMALL,
    FLOAT16_NAN_LARGE,
    FLOAT16_NEG_NAN_SMALL,
    FLOAT16_NEG_NAN_LARGE,
    FLOAT16_NAN_SMALL,
    FLOAT16_NAN_LARGE,
    FLOAT16_NEG_NAN_SMALL,
    FLOAT16_NAN_LARGE
  };

  private static final float[] ZERO_MIN_FLOATS = new float[] {0f, 0f, 0f, 0.5f, 1f, 1.5f, 2f, 3f, 4f, 5f};
  private static final double[] ZERO_MIN_DOUBLES = new double[] {0d, 0d, 0d, 0.5d, 1d, 1.5d, 2d, 3d, 4d, 5d};
  private static final Binary[] ZERO_MIN_FLOAT16 = new Binary[] {
    float16Binary((short) 0x0000),
    float16Binary((short) 0x0000),
    float16Binary((short) 0x0000),
    float16Binary((short) 0x3800),
    float16Binary((short) 0x3c00),
    float16Binary((short) 0x3e00),
    float16Binary((short) 0x4000),
    float16Binary((short) 0x4200),
    float16Binary((short) 0x4400),
    float16Binary((short) 0x4500)
  };

  private static final float[] ZERO_MAX_FLOATS = new float[] {-5f, -4f, -3f, -2f, -1.5f, -1f, -0.5f, -0f, -0f, -0f};
  private static final double[] ZERO_MAX_DOUBLES =
      new double[] {-5d, -4d, -3d, -2d, -1.5d, -1d, -0.5d, -0d, -0d, -0d};
  private static final Binary[] ZERO_MAX_FLOAT16 = new Binary[] {
    float16Binary((short) 0xc500),
    float16Binary((short) 0xc400),
    float16Binary((short) 0xc200),
    float16Binary((short) 0xc000),
    float16Binary((short) 0xbe00),
    float16Binary((short) 0xbc00),
    float16Binary((short) 0xb800),
    float16Binary((short) 0x8000),
    float16Binary((short) 0x8000),
    float16Binary((short) 0x8000)
  };

  public enum Scenario {
    NO_NAN,
    MIXED_NAN,
    ALL_NAN,
    ZERO_MIN,
    ZERO_MAX
  }

  public static final class GenerationResult {
    private final Path noNanFile;
    private final Path mixedNanFile;
    private final Path allNanFile;
    private final Path zeroMinFile;
    private final Path zeroMaxFile;
    private final Path mergedFile;

    private GenerationResult(
        Path noNanFile,
        Path mixedNanFile,
        Path allNanFile,
        Path zeroMinFile,
        Path zeroMaxFile,
        Path mergedFile) {
      this.noNanFile = noNanFile;
      this.mixedNanFile = mixedNanFile;
      this.allNanFile = allNanFile;
      this.zeroMinFile = zeroMinFile;
      this.zeroMaxFile = zeroMaxFile;
      this.mergedFile = mergedFile;
    }

    public Path getNoNanFile() {
      return noNanFile;
    }

    public Path getMixedNanFile() {
      return mixedNanFile;
    }

    public Path getAllNanFile() {
      return allNanFile;
    }

    public Path getZeroMinFile() {
      return zeroMinFile;
    }

    public Path getZeroMaxFile() {
      return zeroMaxFile;
    }

    public Path getMergedFile() {
      return mergedFile;
    }
  }

  private FloatingPointNanInteropFileGenerator() {}

  public static MessageType schema() {
    return SCHEMA;
  }

  public static float[] floatValues(Scenario scenario) {
    switch (scenario) {
      case NO_NAN:
        return Arrays.copyOf(NO_NAN_FLOATS, NO_NAN_FLOATS.length);
      case MIXED_NAN:
        return Arrays.copyOf(MIXED_NAN_FLOATS, MIXED_NAN_FLOATS.length);
      case ALL_NAN:
        return Arrays.copyOf(ALL_NAN_FLOATS, ALL_NAN_FLOATS.length);
      case ZERO_MIN:
        return Arrays.copyOf(ZERO_MIN_FLOATS, ZERO_MIN_FLOATS.length);
      case ZERO_MAX:
        return Arrays.copyOf(ZERO_MAX_FLOATS, ZERO_MAX_FLOATS.length);
      default:
        throw new IllegalArgumentException("Unknown scenario: " + scenario);
    }
  }

  public static double[] doubleValues(Scenario scenario) {
    switch (scenario) {
      case NO_NAN:
        return Arrays.copyOf(NO_NAN_DOUBLES, NO_NAN_DOUBLES.length);
      case MIXED_NAN:
        return Arrays.copyOf(MIXED_NAN_DOUBLES, MIXED_NAN_DOUBLES.length);
      case ALL_NAN:
        return Arrays.copyOf(ALL_NAN_DOUBLES, ALL_NAN_DOUBLES.length);
      case ZERO_MIN:
        return Arrays.copyOf(ZERO_MIN_DOUBLES, ZERO_MIN_DOUBLES.length);
      case ZERO_MAX:
        return Arrays.copyOf(ZERO_MAX_DOUBLES, ZERO_MAX_DOUBLES.length);
      default:
        throw new IllegalArgumentException("Unknown scenario: " + scenario);
    }
  }

  public static Binary[] float16Values(Scenario scenario) {
    Binary[] values;
    switch (scenario) {
      case NO_NAN:
        values = NO_NAN_FLOAT16;
        break;
      case MIXED_NAN:
        values = MIXED_NAN_FLOAT16;
        break;
      case ALL_NAN:
        values = ALL_NAN_FLOAT16;
        break;
      case ZERO_MIN:
        values = ZERO_MIN_FLOAT16;
        break;
      case ZERO_MAX:
        values = ZERO_MAX_FLOAT16;
        break;
      default:
        throw new IllegalArgumentException("Unknown scenario: " + scenario);
    }

    Binary[] copy = new Binary[values.length];
    for (int i = 0; i < values.length; i++) {
      copy[i] = values[i].copy();
    }
    return copy;
  }

  public static GenerationResult generateAndMerge(Configuration conf, Path outputDir) throws IOException {
    FileSystem fs = outputDir.getFileSystem(conf);
    if (!fs.exists(outputDir) && !fs.mkdirs(outputDir)) {
      throw new IOException("Cannot create output directory: " + outputDir);
    }

    Path noNanPath = new Path(outputDir, FILE_NO_NAN);
    Path mixedNanPath = new Path(outputDir, FILE_MIXED_NAN);
    Path allNanPath = new Path(outputDir, FILE_ALL_NAN);
    Path zeroMinPath = new Path(outputDir, FILE_ZERO_MIN);
    Path zeroMaxPath = new Path(outputDir, FILE_ZERO_MAX);
    Path mergedPath = new Path(outputDir, FILE_MERGED);

    deleteIfExists(fs, noNanPath);
    deleteIfExists(fs, mixedNanPath);
    deleteIfExists(fs, allNanPath);
    deleteIfExists(fs, zeroMinPath);
    deleteIfExists(fs, zeroMaxPath);
    deleteIfExists(fs, mergedPath);

    writeScenarioFile(conf, noNanPath, Scenario.NO_NAN);
    writeScenarioFile(conf, mixedNanPath, Scenario.MIXED_NAN);
    writeScenarioFile(conf, allNanPath, Scenario.ALL_NAN);
    writeScenarioFile(conf, zeroMinPath, Scenario.ZERO_MIN);
    writeScenarioFile(conf, zeroMaxPath, Scenario.ZERO_MAX);
    mergeFiles(conf, List.of(noNanPath, mixedNanPath, allNanPath, zeroMinPath, zeroMaxPath), mergedPath);

    return new GenerationResult(noNanPath, mixedNanPath, allNanPath, zeroMinPath, zeroMaxPath, mergedPath);
  }

  public static void writeScenarioFile(Configuration conf, Path outputFile, Scenario scenario) throws IOException {
    float[] floatValues = floatValues(scenario);
    double[] doubleValues = doubleValues(scenario);
    Binary[] float16Values = float16Values(scenario);

    if (floatValues.length != ROWS_PER_FILE
        || doubleValues.length != ROWS_PER_FILE
        || float16Values.length != ROWS_PER_FILE) {
      throw new IllegalStateException("Scenario " + scenario + " must have exactly " + ROWS_PER_FILE + " rows");
    }

    GroupFactory factory = new SimpleGroupFactory(SCHEMA);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
        .withConf(conf)
        .withType(SCHEMA)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withDictionaryEncoding(false)
        .build()) {
      for (int i = 0; i < ROWS_PER_FILE; i++) {
        writer.write(factory.newGroup()
            .append("float_ieee754", floatValues[i])
            .append("float_typedef", floatValues[i])
            .append("double_ieee754", doubleValues[i])
            .append("double_typedef", doubleValues[i])
            .append("float16_ieee754", float16Values[i])
            .append("float16_typedef", float16Values[i]));
      }
    }
  }

  public static void mergeFiles(Configuration conf, List<Path> inputFiles, Path outputFile) throws IOException {
    RewriteOptions options = new RewriteOptions.Builder(conf, inputFiles, outputFile)
        .transform(CompressionCodecName.UNCOMPRESSED)
        .build();
    try (ParquetRewriter rewriter = new ParquetRewriter(options)) {
      rewriter.processBlocks();
    }
  }

  private static void deleteIfExists(FileSystem fs, Path path) throws IOException {
    if (fs.exists(path) && !fs.delete(path, false)) {
      throw new IOException("Failed to delete existing file: " + path);
    }
  }

  private static Binary float16Binary(short bits) {
    return Binary.fromConstantByteArray(new byte[] {(byte) (bits & 0xff), (byte) ((bits >> 8) & 0xff)});
  }

  public static void main(String[] args) throws IOException {
    String outputDir = args.length > 0 ? args[0] : "target/parquet-testing/data";
    Configuration conf = new Configuration();
    GenerationResult result = generateAndMerge(conf, new Path(outputDir));
    System.out.println("Generated files:");
    System.out.println("  " + result.getNoNanFile());
    System.out.println("  " + result.getMixedNanFile());
    System.out.println("  " + result.getAllNanFile());
    System.out.println("  " + result.getZeroMinFile());
    System.out.println("  " + result.getZeroMaxFile());
    System.out.println("  " + result.getMergedFile());
  }
}
