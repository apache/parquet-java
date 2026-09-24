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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * Standalone utility to generate ALP-encoded parquet files from CSV test data.
 *
 * <p>Reads the existing expect CSV files (alp_spotify1_expect.csv.gz, alp_arade_expect.csv.gz)
 * from test resources and writes ALP-encoded parquet files using the Java ALP encoder.
 *
 * <p>Usage: java GenerateAlpParquet [output_directory]
 * If no output directory is specified, files are written to the current directory.
 */
public class GenerateAlpParquet {

  public static void main(String[] args) throws IOException {
    String outputDir = args.length > 0 ? args[0] : ".";
    Files.createDirectories(Paths.get(outputDir));

    generateAlpParquet("/alp_arade_expect.csv.gz", outputDir + "/alp_java_arade.parquet");
    System.out.println("Generated: " + outputDir + "/alp_java_arade.parquet");

    generateAlpParquet("/alp_spotify1_expect.csv.gz", outputDir + "/alp_java_spotify1.parquet");
    System.out.println("Generated: " + outputDir + "/alp_java_spotify1.parquet");

    generateAlpParquetFloat(
        "/alp_float_arade_expect.csv.gz", outputDir + "/alp_java_float_arade.parquet");
    System.out.println("Generated: " + outputDir + "/alp_java_float_arade.parquet");

    generateAlpParquetFloat(
        "/alp_float_spotify1_expect.csv.gz", outputDir + "/alp_java_float_spotify1.parquet");
    System.out.println("Generated: " + outputDir + "/alp_java_float_spotify1.parquet");
  }

  private static void generateAlpParquet(String csvResource, String outputPath) throws IOException {
    // Read CSV
    String[] columnNames;
    List<double[]> rows = new ArrayList<>();

    try (InputStream raw = GenerateAlpParquet.class.getResourceAsStream(csvResource);
        InputStream is = new GZIPInputStream(raw);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      // Parse header
      String header = br.readLine();
      columnNames = header.split(",");

      // Parse data rows
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(",");
        double[] values = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
          values[i] = Double.parseDouble(parts[i]);
        }
        rows.add(values);
      }
    }

    // Build schema: all required DOUBLE columns
    Types.MessageTypeBuilder schemaBuilder = Types.buildMessage();
    for (String name : columnNames) {
      schemaBuilder.required(PrimitiveTypeName.DOUBLE).named(name);
    }
    MessageType schema = schemaBuilder.named("schema");

    // Delete output file if it exists
    java.io.File outFile = new java.io.File(outputPath);
    if (outFile.exists()) {
      outFile.delete();
    }

    // Write ALP-encoded parquet
    Path path = new Path(outFile.getAbsolutePath());
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withWriterVersion(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0)
        .withAlpEncoding(true)
        .withDictionaryEncoding(false)
        .build()) {
      for (double[] row : rows) {
        Group group = groupFactory.newGroup();
        for (int c = 0; c < columnNames.length; c++) {
          group.append(columnNames[c], row[c]);
        }
        writer.write(group);
      }
    }
  }

  private static void generateAlpParquetFloat(String csvResource, String outputPath)
      throws IOException {
    // Read CSV into float values
    String[] columnNames;
    List<float[]> rows = new ArrayList<>();

    try (InputStream raw = GenerateAlpParquet.class.getResourceAsStream(csvResource);
        InputStream is = new GZIPInputStream(raw);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      // Parse header
      String header = br.readLine();
      columnNames = header.split(",");

      // Parse data rows
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(",");
        float[] values = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
          values[i] = Float.parseFloat(parts[i]);
        }
        rows.add(values);
      }
    }

    // Build schema: all required FLOAT columns
    Types.MessageTypeBuilder schemaBuilder = Types.buildMessage();
    for (String name : columnNames) {
      schemaBuilder.required(PrimitiveTypeName.FLOAT).named(name);
    }
    MessageType schema = schemaBuilder.named("schema");

    // Delete output file if it exists
    java.io.File outFile = new java.io.File(outputPath);
    if (outFile.exists()) {
      outFile.delete();
    }

    // Write ALP-encoded parquet
    Path path = new Path(outFile.getAbsolutePath());
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withWriterVersion(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0)
        .withAlpEncoding(true)
        .withDictionaryEncoding(false)
        .build()) {
      for (float[] row : rows) {
        Group group = groupFactory.newGroup();
        for (int c = 0; c < columnNames.length; c++) {
          group.append(columnNames[c], row[c]);
        }
        writer.write(group);
      }
    }
  }
}
