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

package org.apache.parquet.hadoop.util;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

public class ColumnPrunerTest {

  private final int numRecord = 1000;
  private ColumnPruner columnPruner = new ColumnPruner();
  private Configuration conf = new Configuration();

  @Test
  public void testPruneOneColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile("input");
    String outputFile = createTempFile("output");

    // Remove column Gender
    List<String> cols = List.of("Gender");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertThat(fields).hasSize(3);
    assertThat(fields.get(0).getName()).isEqualTo("DocId");
    assertThat(fields.get(1).getName()).isEqualTo("Name");
    assertThat(fields.get(2).getName()).isEqualTo("Links");
    List<Type> subFields = fields.get(2).asGroupType().getFields();
    assertThat(subFields).hasSize(2);
    assertThat(subFields.get(0).getName()).isEqualTo("Backward");
    assertThat(subFields.get(1).getName()).isEqualTo("Forward");

    // Verify the data are not changed for the columns not pruned
    List<String> prunePaths = List.of("Gender");
    validateColumns(inputFile, prunePaths);
  }

  @Test
  public void testPruneMultiColumns() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile("input");
    String outputFile = createTempFile("output");

    // Remove columns
    String cargs[] = {inputFile, outputFile, "Name", "Gender"};
    List<String> cols = List.of("Name", "Gender");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertThat(fields).hasSize(2);
    assertThat(fields.get(0).getName()).isEqualTo("DocId");
    assertThat(fields.get(1).getName()).isEqualTo("Links");
    List<Type> subFields = fields.get(1).asGroupType().getFields();
    assertThat(subFields).hasSize(2);
    assertThat(subFields.get(0).getName()).isEqualTo("Backward");
    assertThat(subFields.get(1).getName()).isEqualTo("Forward");

    // Verify the data are not changed for the columns not pruned
    List<String> prunePaths = List.of("Name", "Gender");
    validateColumns(inputFile, prunePaths);
  }

  @Test
  public void testNotExistsColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile("input");
    String outputFile = createTempFile("output");
    List<String> cols = List.of("no_exist");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);
  }

  @Test
  public void testPruneNestedColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile("input");
    String outputFile = createTempFile("output");

    // Remove nested column
    List<String> cols = List.of("Links.Backward");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertThat(fields).hasSize(4);
    assertThat(fields.get(0).getName()).isEqualTo("DocId");
    assertThat(fields.get(1).getName()).isEqualTo("Name");
    assertThat(fields.get(2).getName()).isEqualTo("Gender");
    assertThat(fields.get(3).getName()).isEqualTo("Links");
    List<Type> subFields = fields.get(3).asGroupType().getFields();
    assertThat(subFields).hasSize(1);
    assertThat(subFields.get(0).getName()).isEqualTo("Forward");

    // Verify the data are not changed for the columns not pruned
    List<String> prunePaths = List.of("Links.Backward");
    validateColumns(inputFile, prunePaths);
  }

  @Test
  public void testPruneNestedParentColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile("input");
    String outputFile = createTempFile("output");

    // Remove parent column. All of it's children will be removed.
    List<String> cols = List.of("Links");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertThat(fields).hasSize(3);
    assertThat(fields.get(0).getName()).isEqualTo("DocId");
    assertThat(fields.get(1).getName()).isEqualTo("Name");
    assertThat(fields.get(2).getName()).isEqualTo("Gender");

    // Verify the data are not changed for the columns not pruned
    List<String> prunePaths = List.of("Links");
    validateColumns(inputFile, prunePaths);
  }

  @Test
  public void testNotExistsNestedColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile("input");
    String outputFile = createTempFile("output");
    List<String> cols = List.of("Links.Not_exists");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);
  }

  private void validateColumns(String inputFile, List<String> prunePaths) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(inputFile))
        .withConf(conf)
        .build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      if (!prunePaths.contains("DocId")) {
        assertThat(group.getLong("DocId", 0)).isEqualTo(1l);
      }
      if (!prunePaths.contains("Name")) {
        assertThat(group.getBinary("Name", 0).toStringUsingUTF8()).isEqualTo("foo");
      }
      if (!prunePaths.contains("Gender")) {
        assertThat(group.getBinary("Gender", 0).toStringUsingUTF8()).isEqualTo("male");
      }
      if (!prunePaths.contains("Links")) {
        Group subGroup = group.getGroup("Links", 0);
        if (!prunePaths.contains("Links.Backward")) {
          assertThat(subGroup.getLong("Backward", 0)).isEqualTo(2l);
        }
        if (!prunePaths.contains("Links.Forward")) {
          assertThat(subGroup.getLong("Forward", 0)).isEqualTo(3l);
        }
      }
    }
    reader.close();
  }

  private String createParquetFile(String prefix) throws IOException {
    MessageType schema = new MessageType(
        "schema",
        new PrimitiveType(REQUIRED, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(REQUIRED, BINARY, "Gender"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, INT64, "Backward"),
            new PrimitiveType(REPEATED, INT64, "Forward")));

    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    String file = createTempFile(prefix);
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(new Path(file)).withConf(conf);
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("DocId", 1l);
        g.add("Name", "foo");
        g.add("Gender", "male");
        Group links = g.addGroup("Links");
        links.add(0, 2l);
        links.add(1, 3l);
        writer.write(g);
      }
    }

    return file;
  }

  private static String createTempFile(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }
}
