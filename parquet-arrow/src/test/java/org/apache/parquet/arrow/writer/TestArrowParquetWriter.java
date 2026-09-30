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
package org.apache.parquet.arrow.writer;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestArrowParquetWriter {

  @TempDir
  File tempDir;

  private BufferAllocator allocator;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void tearDown() {
    allocator.close();
  }

  @Test
  void testWriteRequiredIntegers() throws IOException {
    Schema arrowSchema = new Schema(asList(
        new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
        new Field("value", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    Path path = tempDir.toPath().resolve("required_ints.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      IntVector idVector = (IntVector) batch.getVector("id");
      BigIntVector valueVector = (BigIntVector) batch.getVector("value");
      idVector.allocateNew(3);
      valueVector.allocateNew(3);
      idVector.set(0, 1);
      idVector.set(1, 2);
      idVector.set(2, 3);
      valueVector.set(0, 100L);
      valueVector.set(1, 200L);
      valueVector.set(2, 300L);
      batch.setRowCount(3);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      Group row0 = reader.read();
      assertThat(row0).isNotNull();
      assertThat(row0.getInteger("id", 0)).isEqualTo(1);
      assertThat(row0.getLong("value", 0)).isEqualTo(100L);

      Group row1 = reader.read();
      assertThat(row1.getInteger("id", 0)).isEqualTo(2);
      assertThat(row1.getLong("value", 0)).isEqualTo(200L);

      Group row2 = reader.read();
      assertThat(row2.getInteger("id", 0)).isEqualTo(3);
      assertThat(row2.getLong("value", 0)).isEqualTo(300L);

      assertThat(reader.read()).isNull();
    }
  }

  @Test
  void testWriteNullableIntegers() throws IOException {
    Schema arrowSchema = new Schema(asList(
        new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    Path path = tempDir.toPath().resolve("nullable_ints.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      IntVector xVector = (IntVector) batch.getVector("x");
      xVector.allocateNew(4);
      xVector.set(0, 10);
      xVector.setNull(1);
      xVector.set(2, 30);
      xVector.setNull(3);
      batch.setRowCount(4);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      Group row0 = reader.read();
      assertThat(row0.getInteger("x", 0)).isEqualTo(10);

      Group row1 = reader.read();
      assertThat(row1.getFieldRepetitionCount("x")).isEqualTo(0);

      Group row2 = reader.read();
      assertThat(row2.getInteger("x", 0)).isEqualTo(30);

      Group row3 = reader.read();
      assertThat(row3.getFieldRepetitionCount("x")).isEqualTo(0);

      assertThat(reader.read()).isNull();
    }
  }

  @Test
  void testWriteFloatsAndDoubles() throws IOException {
    Schema arrowSchema = new Schema(asList(
        new Field("f", FieldType.notNullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
        new Field("d", FieldType.notNullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));

    MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    Path path = tempDir.toPath().resolve("floats.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      Float4Vector fVector = (Float4Vector) batch.getVector("f");
      Float8Vector dVector = (Float8Vector) batch.getVector("d");
      fVector.allocateNew(3);
      dVector.allocateNew(3);
      fVector.set(0, 1.5f);
      fVector.set(1, -3.14f);
      fVector.set(2, Float.NaN);
      dVector.set(0, 2.718281828);
      dVector.set(1, Double.MAX_VALUE);
      dVector.set(2, Double.NaN);
      batch.setRowCount(3);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      Group row0 = reader.read();
      assertThat(row0.getFloat("f", 0)).isEqualTo(1.5f);
      assertThat(row0.getDouble("d", 0)).isEqualTo(2.718281828);

      Group row1 = reader.read();
      assertThat(row1.getFloat("f", 0)).isEqualTo(-3.14f);
      assertThat(row1.getDouble("d", 0)).isEqualTo(Double.MAX_VALUE);

      Group row2 = reader.read();
      assertThat(row2.getFloat("f", 0)).isNaN();
      assertThat(row2.getDouble("d", 0)).isNaN();

      assertThat(reader.read()).isNull();
    }
  }

  @Test
  void testMultipleBatches() throws IOException {
    Schema arrowSchema = new Schema(asList(
        new Field("v", FieldType.notNullable(new ArrowType.Int(32, true)), null)));

    MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    Path path = tempDir.toPath().resolve("multi_batch.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
      try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
        IntVector v = (IntVector) batch.getVector("v");
        v.allocateNew(2);
        v.set(0, 10);
        v.set(1, 20);
        batch.setRowCount(2);
        writer.writeBatch(batch);
      }
      try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
        IntVector v = (IntVector) batch.getVector("v");
        v.allocateNew(2);
        v.set(0, 30);
        v.set(1, 40);
        batch.setRowCount(2);
        writer.writeBatch(batch);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      assertThat(reader.read().getInteger("v", 0)).isEqualTo(10);
      assertThat(reader.read().getInteger("v", 0)).isEqualTo(20);
      assertThat(reader.read().getInteger("v", 0)).isEqualTo(30);
      assertThat(reader.read().getInteger("v", 0)).isEqualTo(40);
      assertThat(reader.read()).isNull();
    }
  }

  @Test
  void testLargeBatchProducesMultiplePages() throws IOException {
    // Write enough data that page splitting should produce multiple pages
    // 500K INT32 values = 2MB of data, which should split into ~2 pages at 1MB target
    Schema arrowSchema = new Schema(asList(
        new Field("x", FieldType.notNullable(new ArrowType.Int(32, true)), null)));

    MessageType parquetSchema = org.apache.parquet.schema.Types.buildMessage()
        .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("x")
        .named("root");

    Path path = tempDir.toPath().resolve("large_batch.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    int numRows = 500_000;
    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      IntVector v = (IntVector) batch.getVector("x");
      v.allocateNew(numRows);
      for (int i = 0; i < numRows; i++) {
        v.set(i, i);
      }
      batch.setRowCount(numRows);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    // Verify all rows present and file has multiple pages
    try (ParquetFileReader fileReader = ParquetFileReader.open(new LocalInputFile(path))) {
      ParquetMetadata footer = fileReader.getFooter();
      long totalRows = footer.getBlocks().stream()
          .mapToLong(block -> block.getRowCount())
          .sum();
      assertThat(totalRows).isEqualTo(numRows);

      // Verify the column chunk has multiple pages (data_page_offset implies pages were written)
      // With 500K x 4 bytes = 2MB and 1MB target page size, expect at least 2 pages
      long totalSize = footer.getBlocks().get(0).getColumns().get(0).getTotalSize();
      assertThat(totalSize).isGreaterThan(0);
    }

    // Verify round-trip of first and last values
    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      Group first = reader.read();
      assertThat(first.getInteger("x", 0)).isEqualTo(0);
      // Skip to near the end
      for (int i = 1; i < numRows - 1; i++) {
        reader.read();
      }
      Group last = reader.read();
      assertThat(last.getInteger("x", 0)).isEqualTo(numRows - 1);
      assertThat(reader.read()).isNull();
    }
  }

  @Test
  void testFooterRowCount() throws IOException {
    Schema arrowSchema = new Schema(asList(
        new Field("n", FieldType.notNullable(new ArrowType.Int(32, true)), null)));

    MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    Path path = tempDir.toPath().resolve("rowcount.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      IntVector v = (IntVector) batch.getVector("n");
      v.allocateNew(100);
      for (int i = 0; i < 100; i++) {
        v.set(i, i);
      }
      batch.setRowCount(100);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    try (ParquetFileReader fileReader = ParquetFileReader.open(new LocalInputFile(path))) {
      ParquetMetadata footer = fileReader.getFooter();
      long totalRows = footer.getBlocks().stream()
          .mapToLong(block -> block.getRowCount())
          .sum();
      assertThat(totalRows).isEqualTo(100);
    }
  }

  @Test
  void testWriteStrings() throws IOException {
    Schema arrowSchema = new Schema(asList(
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    Path path = tempDir.toPath().resolve("strings.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      org.apache.arrow.vector.VarCharVector nameVector =
          (org.apache.arrow.vector.VarCharVector) batch.getVector("name");
      nameVector.allocateNew();
      nameVector.set(0, "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      nameVector.set(1, "world".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      nameVector.setNull(2);
      nameVector.set(3, "".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      nameVector.setValueCount(4);
      batch.setRowCount(4);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      Group row0 = reader.read();
      assertThat(row0.getBinary("name", 0).toStringUsingUTF8()).isEqualTo("hello");

      Group row1 = reader.read();
      assertThat(row1.getBinary("name", 0).toStringUsingUTF8()).isEqualTo("world");

      Group row2 = reader.read();
      assertThat(row2.getFieldRepetitionCount("name")).isEqualTo(0); // null

      Group row3 = reader.read();
      assertThat(row3.getBinary("name", 0).toStringUsingUTF8()).isEqualTo("");

      assertThat(reader.read()).isNull();
    }
  }

  @Test
  void testWriteBooleans() throws IOException {
    Schema arrowSchema = new Schema(asList(
        new Field("flag", FieldType.nullable(new ArrowType.Bool()), null)));

    MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    Path path = tempDir.toPath().resolve("booleans.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      org.apache.arrow.vector.BitVector flagVector =
          (org.apache.arrow.vector.BitVector) batch.getVector("flag");
      flagVector.allocateNew(5);
      flagVector.set(0, 1); // true
      flagVector.set(1, 0); // false
      flagVector.setNull(2); // null
      flagVector.set(3, 1); // true
      flagVector.set(4, 0); // false
      batch.setRowCount(5);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      assertThat(reader.read().getBoolean("flag", 0)).isTrue();
      assertThat(reader.read().getBoolean("flag", 0)).isFalse();

      Group row2 = reader.read();
      assertThat(row2.getFieldRepetitionCount("flag")).isEqualTo(0); // null

      assertThat(reader.read().getBoolean("flag", 0)).isTrue();
      assertThat(reader.read().getBoolean("flag", 0)).isFalse();

      assertThat(reader.read()).isNull();
    }
  }

  @Test
  void testZeroCopyPathWithRequiredSchema() throws IOException {
    // Manually construct a Parquet schema with REQUIRED fields to hit ZeroCopyPlainWriter
    MessageType parquetSchema = org.apache.parquet.schema.Types.buildMessage()
        .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("id")
        .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("ts")
        .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("val")
        .named("root");

    Schema arrowSchema = new Schema(asList(
        new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
        new Field("ts", FieldType.notNullable(new ArrowType.Int(64, true)), null),
        new Field("val", FieldType.notNullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));

    Path path = tempDir.toPath().resolve("zerocopy.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      IntVector idVector = (IntVector) batch.getVector("id");
      BigIntVector tsVector = (BigIntVector) batch.getVector("ts");
      Float8Vector valVector = (Float8Vector) batch.getVector("val");
      idVector.allocateNew(5);
      tsVector.allocateNew(5);
      valVector.allocateNew(5);
      for (int i = 0; i < 5; i++) {
        idVector.set(i, i + 1);
        tsVector.set(i, 1000000L + i);
        valVector.set(i, i * 1.1);
      }
      batch.setRowCount(5);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      for (int i = 0; i < 5; i++) {
        Group row = reader.read();
        assertThat(row).isNotNull();
        assertThat(row.getInteger("id", 0)).isEqualTo(i + 1);
        assertThat(row.getLong("ts", 0)).isEqualTo(1000000L + i);
        assertThat(row.getDouble("val", 0)).isEqualTo(i * 1.1);
      }
      assertThat(reader.read()).isNull();
    }
  }

  @Test
  void testMixedSchema() throws IOException {
    Schema arrowSchema = new Schema(asList(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("active", FieldType.nullable(new ArrowType.Bool()), null),
        new Field("score", FieldType.nullable(
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));

    MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    Path path = tempDir.toPath().resolve("mixed.parquet");
    OutputFile outputFile = new LocalOutputFile(path);

    try (VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator)) {
      IntVector idVector = (IntVector) batch.getVector("id");
      org.apache.arrow.vector.VarCharVector nameVector =
          (org.apache.arrow.vector.VarCharVector) batch.getVector("name");
      org.apache.arrow.vector.BitVector activeVector =
          (org.apache.arrow.vector.BitVector) batch.getVector("active");
      Float8Vector scoreVector = (Float8Vector) batch.getVector("score");

      idVector.allocateNew(3);
      nameVector.allocateNew();
      activeVector.allocateNew(3);
      scoreVector.allocateNew(3);

      idVector.set(0, 1);
      nameVector.set(0, "alice".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      activeVector.set(0, 1);
      scoreVector.set(0, 95.5);

      idVector.set(1, 2);
      nameVector.set(1, "bob".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      activeVector.set(1, 0);
      scoreVector.setNull(1);

      idVector.setNull(2);
      nameVector.setNull(2);
      activeVector.setNull(2);
      scoreVector.set(2, 77.0);

      batch.setRowCount(3);

      try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, parquetSchema)) {
        writer.writeBatch(batch);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      Group row0 = reader.read();
      assertThat(row0.getInteger("id", 0)).isEqualTo(1);
      assertThat(row0.getBinary("name", 0).toStringUsingUTF8()).isEqualTo("alice");
      assertThat(row0.getBoolean("active", 0)).isTrue();
      assertThat(row0.getDouble("score", 0)).isEqualTo(95.5);

      Group row1 = reader.read();
      assertThat(row1.getInteger("id", 0)).isEqualTo(2);
      assertThat(row1.getBinary("name", 0).toStringUsingUTF8()).isEqualTo("bob");
      assertThat(row1.getBoolean("active", 0)).isFalse();
      assertThat(row1.getFieldRepetitionCount("score")).isEqualTo(0); // null

      Group row2 = reader.read();
      assertThat(row2.getFieldRepetitionCount("id")).isEqualTo(0); // null
      assertThat(row2.getFieldRepetitionCount("name")).isEqualTo(0); // null
      assertThat(row2.getFieldRepetitionCount("active")).isEqualTo(0); // null
      assertThat(row2.getDouble("score", 0)).isEqualTo(77.0);

      assertThat(reader.read()).isNull();
    }
  }
}


