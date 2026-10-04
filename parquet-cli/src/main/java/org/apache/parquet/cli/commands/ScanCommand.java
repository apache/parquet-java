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
package org.apache.parquet.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Expressions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;

@Parameters(commandDescription = "Scan all records from a file")
public class ScanCommand extends BaseCommand {

  @Parameter(description = "<file>")
  List<String> sourceFiles;

  @Parameter(
      names = {"-c", "--column", "--columns"},
      description = "List of columns")
  List<String> columns;

  public ScanCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(sourceFiles != null && !sourceFiles.isEmpty(), "Missing file name");

    List<ScanFile> scanFiles = new ArrayList<>(sourceFiles.size());
    for (String sourceFile : sourceFiles) {
      scanFiles.add(prepare(sourceFile));
    }

    long totalStartTime = System.currentTimeMillis();
    long totalCount = 0;
    for (ScanFile scanFile : scanFiles) {
      long startTime = System.currentTimeMillis();
      long count = scan(scanFile);
      totalCount += count;
      if (1 < sourceFiles.size()) {
        long endTime = System.currentTimeMillis();
        console.info("Scanned " + count + " records from " + scanFile.sourceFile + " in "
            + (endTime - startTime) / 1000.0 + " s");
      }
    }
    long totalEndTime = System.currentTimeMillis();
    console.info("Scanned " + totalCount + " records from " + sourceFiles.size() + " file(s)");
    console.info("Time: " + (totalEndTime - totalStartTime) / 1000.0 + " s");
    return 0;
  }

  private ScanFile prepare(String sourceFile) throws IOException {
    if (isParquetFile(sourceFile)) {
      MessageType projection = columns == null || columns.isEmpty() ? null : getParquetProjection(sourceFile, columns);
      return ScanFile.parquet(sourceFile, projection);
    } else {
      Schema schema = getAvroSchema(sourceFile);
      return ScanFile.avro(sourceFile, Expressions.filterSchema(schema, columns));
    }
  }

  private long scan(ScanFile scanFile) throws IOException {
    if (scanFile.parquet) {
      return scanParquetDataFile(scanFile);
    } else {
      return scanAvroDataFile(scanFile);
    }
  }

  private long scanParquetDataFile(ScanFile scanFile) throws IOException {
    ParquetReader<Group> reader = openParquetGroupReader(scanFile.sourceFile, scanFile.parquetProjection);
    boolean threw = true;
    long count = 0;
    try {
      for (Group record = reader.read(); record != null; record = reader.read()) {
        count += 1;
      }
      threw = false;
    } catch (IOException | RuntimeException e) {
      throw new RuntimeException("Failed on record " + count + " in " + scanFile.sourceFile, e);
    } finally {
      Closeables.close(reader, threw);
    }
    return count;
  }

  private long scanAvroDataFile(ScanFile scanFile) throws IOException {
    Iterable<Object> reader = openDataFile(scanFile.sourceFile, scanFile.avroProjection);
    boolean threw = true;
    long count = 0;
    try {
      for (Object record : reader) {
        count += 1;
      }
      threw = false;
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed on record " + count + " in " + scanFile.sourceFile, e);
    } finally {
      if (reader instanceof Closeable) {
        Closeables.close((Closeable) reader, threw);
      }
    }
    return count;
  }

  private static class ScanFile {
    private final String sourceFile;
    private final boolean parquet;
    private final MessageType parquetProjection;
    private final Schema avroProjection;

    private ScanFile(String sourceFile, boolean parquet, MessageType parquetProjection, Schema avroProjection) {
      this.sourceFile = sourceFile;
      this.parquet = parquet;
      this.parquetProjection = parquetProjection;
      this.avroProjection = avroProjection;
    }

    private static ScanFile parquet(String sourceFile, MessageType projection) {
      return new ScanFile(sourceFile, true, projection, null);
    }

    private static ScanFile avro(String sourceFile, Schema projection) {
      return new ScanFile(sourceFile, false, null, projection);
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Scan all the records from file \"data.avro\":",
        "data.avro",
        "# Scan all the records from file \"data.parquet\":",
        "data.parquet");
  }
}
