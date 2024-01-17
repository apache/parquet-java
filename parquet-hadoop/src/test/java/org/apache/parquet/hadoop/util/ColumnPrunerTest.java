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
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ColumnPrunerTest {

  private final int numRecord = 1000;
  private ColumnPruner columnPruner = new ColumnPruner();
  private Configuration conf = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String newTempFile() throws IOException {
    File file = tempFolder.newFile();
    file.delete();
    return file.getAbsolutePath();
  }

  @Test
  public void testPruneOneColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile();
    String outputFile = newTempFile();

    // Remove column Gender
    List<String> cols = Arrays.asList("Gender");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 3);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Links");
    List<Type> subFields = fields.get(2).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "Backward");
    assertEquals(subFields.get(1).getName(), "Forward");

    // Verify the data are not changed for the columns not pruned
    List<String> prunePaths = Arrays.asList("Gender");
    validateColumns(inputFile, prunePaths);
  }

  @Test
  public void testPruneMultiColumns() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile();
    String outputFile = newTempFile();

    // Remove columns
    String cargs[] = {inputFile, outputFile, "Name", "Gender"};
    List<String> cols = Arrays.asList("Name", "Gender");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 2);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Links");
    List<Type> subFields = fields.get(1).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "Backward");
    assertEquals(subFields.get(1).getName(), "Forward");

    // Verify the data are not changed for the columns not pruned
    List<String> prunePaths = Arrays.asList("Name", "Gender");
    validateColumns(inputFile, prunePaths);
  }

  @Test
  public void testNotExistsColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile();
    String outputFile = newTempFile();
    List<String> cols = Arrays.asList("no_exist");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);
  }

  @Test
  public void testPruneNestedColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile();
    String outputFile = newTempFile();

    // Remove nested column
    List<String> cols = Arrays.asList("Links.Backward");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 4);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Gender");
    assertEquals(fields.get(3).getName(), "Links");
    List<Type> subFields = fields.get(3).asGroupType().getFields();
    assertEquals(subFields.size(), 1);
    assertEquals(subFields.get(0).getName(), "Forward");

    // Verify the data are not changed for the columns not pruned
    List<String> prunePaths = Arrays.asList("Links.Backward");
    validateColumns(inputFile, prunePaths);
  }

  @Test
  public void testPruneNestedParentColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile();
    String outputFile = newTempFile();

    // Remove parent column. All of its children will be removed.
    List<String> cols = Arrays.asList("Links");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 3);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Gender");

    // Verify the data are not changed for the columns not pruned
    List<String> prunePaths = Arrays.asList("Links");
    validateColumns(inputFile, prunePaths);
  }

  @Test
  public void testNotExistsNestedColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile();
    String outputFile = newTempFile();
    List<String> cols = Arrays.asList("Links.Not_exists");
    columnPruner.pruneColumns(conf, new Path(inputFile), new Path(outputFile), cols);
  }

  private void validateColumns(String inputFile, List<String> prunePaths) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(inputFile))
        .withConf(conf)
        .build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      if (!prunePaths.contains("DocId")) {
        assertEquals(1l, group.getLong("DocId", 0));
      }
      if (!prunePaths.contains("Name")) {
        assertEquals("foo", group.getBinary("Name", 0).toStringUsingUTF8());
      }
      if (!prunePaths.contains("Gender")) {
        assertEquals("male", group.getBinary("Gender", 0).toStringUsingUTF8());
      }
      if (!prunePaths.contains("Links")) {
        Group subGroup = group.getGroup("Links", 0);
        if (!prunePaths.contains("Links.Backward")) {
          assertEquals(2l, subGroup.getLong("Backward", 0));
        }
        if (!prunePaths.contains("Links.Forward")) {
          assertEquals(3l, subGroup.getLong("Forward", 0));
        }
      }
    }
    reader.close();
  }

  private String createParquetFile() throws IOException {
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

    String file = newTempFile();
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
}
