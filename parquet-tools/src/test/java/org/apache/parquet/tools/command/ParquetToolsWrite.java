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

package org.apache.parquet.tools.command;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/** Simple Parquet writer for parquet-tools to either create single file or table */
public class ParquetToolsWrite {
  private static final Configuration CONF = new Configuration();
  private static final int NUM_RECORDS = 10;
  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
    "message spark_schema { " +
      "required int64 id; " +
      "required binary str (UTF8); " +
      "required boolean flag; " +
    "}");

  private static void writeData(ParquetWriter<Group> writer, int numRecords) throws IOException {
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    for (int i = 0; i < numRecords; i++) {
      Group group = factory.newGroup()
        .append("id", (long) i)
        .append("str", "id-" + i)
        .append("flag", i % 2 == 0);
      writer.write(group);
    }
  }

  /** Create single Parquet file with schema defined above, path should not exist prior write */
  public static void writeParquetFile(Path path, int numRecords) throws IOException {
    ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
      .withWriterVersion(WriterVersion.PARQUET_1_0)
      .withPageSize(1024)
      .enableDictionaryEncoding()
      .withDictionaryPageSize(2 * 1024)
      .withConf(CONF)
      .withType(SCHEMA)
      .build();
    writeData(writer, numRecords);
    writer.close();
  }

  public static void writeParquetFile(Path path) throws IOException {
    writeParquetFile(path, NUM_RECORDS);
  }

  /**
   * Create Parquet table, e.g. directory with files per partition.
   * This is similar to how Spark would save using Parquet datasource.
   */
  public static void writeParquetTable(
      Path path,
      int recordsPerSplit,
      int splits,
      boolean withSuccessFile) throws IOException {
    FileSystem fs = path.getFileSystem(CONF);
    if (fs.exists(path)) {
      throw new IOException("Path already exists: " + path);
    }

    fs.mkdirs(path);
    // Generate parts for each split
    for (int split = 0; split < splits; split++) {
      Path splitPath = path.suffix(Path.SEPARATOR + "part-" + split + ".parquet");
      writeParquetFile(splitPath, recordsPerSplit);
    }
    // This is added to simulate Spark behaviour of creating file when writing Parquet table
    if (withSuccessFile) {
      fs.createNewFile(path.suffix(Path.SEPARATOR + "_SUCCESS"));
    }
  }

  public static void writeParquetTable(
      Path path, int splits, boolean withSuccessFile) throws IOException {
    writeParquetTable(path, NUM_RECORDS, splits, withSuccessFile);
  }
}
