/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.example;

import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for unsigned integer validation in ExampleParquetWriter.
 */
public class TestStrictUnsignedIntegerValidation {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testValidUnsignedIntegerValues() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(8, false))
        .named("uint8_field")
        .required(INT32)
        .as(intType(16, false))
        .named("uint16_field")
        .required(INT32)
        .as(intType(32, false))
        .named("uint32_field")
        .required(INT64)
        .as(intType(64, false))
        .named("uint64_field")
        .named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "valid_unsigned.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withValidation(true)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group validGroup = groupFactory
          .newGroup()
          .append("uint8_field", 255)
          .append("uint16_field", 65535)
          .append("uint32_field", Integer.MAX_VALUE)
          .append("uint64_field", Long.MAX_VALUE);

      writer.write(validGroup);

      Group zeroGroup = groupFactory
          .newGroup()
          .append("uint8_field", 0)
          .append("uint16_field", 0)
          .append("uint32_field", 0)
          .append("uint64_field", 0L);

      writer.write(zeroGroup);
    }
  }

  @Test
  public void testMaximumUnsignedIntegerValues() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(8, false))
        .named("uint8_field")
        .required(INT32)
        .as(intType(16, false))
        .named("uint16_field")
        .required(INT32)
        .as(intType(32, false))
        .named("uint32_field")
        .required(INT64)
        .as(intType(64, false))
        .named("uint64_field")
        .named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "max_unsigned.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withValidation(true)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group maxGroup = groupFactory
          .newGroup()
          .append("uint8_field", 255)
          .append("uint16_field", 65535)
          .append("uint32_field", Integer.MAX_VALUE)
          .append("uint64_field", Long.MAX_VALUE);

      writer.write(maxGroup);
    }
  }

  @Test
  public void testInvalidUint8Values() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(8, false))
        .named("uint8_field")
        .named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "invalid_uint8.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withValidation(true)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group invalidGroup = groupFactory.newGroup().append("uint8_field", -1);
      assertThrows(InvalidRecordException.class, () -> {
        writer.write(invalidGroup);
      });
    }
  }

  @Test
  public void testInvalidUint16Values() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(16, false))
        .named("uint16_field")
        .named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "invalid_uint16.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withValidation(true)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group invalidGroup = groupFactory.newGroup().append("uint16_field", -20);
      assertThrows(InvalidRecordException.class, () -> {
        writer.write(invalidGroup);
      });
    }
  }

  @Test
  public void testInvalidUint32Values() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(32, false))
        .named("uint32_field")
        .named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "invalid_uint32.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withValidation(true)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group invalidGroup = groupFactory.newGroup().append("uint32_field", -100);
      assertThrows(InvalidRecordException.class, () -> {
        writer.write(invalidGroup);
      });
    }
  }

  @Test
  public void testInvalidUint64Values() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT64)
        .as(intType(64, false))
        .named("uint64_field")
        .named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "invalid_uint64.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withValidation(true)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group invalidGroup = groupFactory.newGroup().append("uint64_field", -1000L);
      assertThrows(InvalidRecordException.class, () -> {
        writer.write(invalidGroup);
      });
    }
  }

  @Test
  public void testValidationDisabledByDefault() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(8, false))
        .named("uint8_field")
        .named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "validation_disabled.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(outputPath).withType(schema).build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group invalidGroup = groupFactory.newGroup().append("uint8_field", -5);
      writer.write(invalidGroup);
    }
  }

  @Test
  public void testValidationCanBeExplicitlyDisabled() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .as(intType(8, false))
        .named("uint8_field")
        .named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "validation_explicit_disabled.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withValidation(false)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group invalidGroup = groupFactory.newGroup().append("uint8_field", -10);
      writer.write(invalidGroup);
    }
  }

  @Test
  public void testBasicValidation() throws IOException {
    MessageType schema =
        Types.buildMessage().required(INT32).named("int32_field").named("test_schema");

    File tempFile = new File(tempFolder.getRoot(), "basic_validation.parquet");
    Path outputPath = new Path(tempFile.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withValidation(true)
        .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      Group validGroup = groupFactory.newGroup().append("int32_field", 42);
      writer.write(validGroup);

      MessageType stringSchema =
          Types.buildMessage().required(BINARY).named("int32_field").named("test_schema");

      SimpleGroupFactory stringGroupFactory = new SimpleGroupFactory(stringSchema);
      Group invalidGroup = stringGroupFactory.newGroup().append("int32_field", Binary.fromString("not_an_int"));

      assertThrows(InvalidRecordException.class, () -> {
        writer.write(invalidGroup);
      });
    }
  }
}
