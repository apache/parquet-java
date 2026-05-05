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

import static org.apache.parquet.cli.util.Expressions.select;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Expressions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;

@Parameters(commandDescription = "Print the first N records from a file")
public class CatCommand extends BaseCommand {

  @Parameter(description = "<file>")
  List<String> sourceFiles;

  @Parameter(
      names = {"-n", "--num-records"},
      description = "The number of records to print")
  long numRecords;

  @Parameter(
      names = {"-c", "--column", "--columns"},
      description = "List of columns")
  List<String> columns;

  public CatCommand(Logger console, long defaultNumRecords) {
    super(console);
    this.numRecords = defaultNumRecords;
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(sourceFiles != null && !sourceFiles.isEmpty(), "Missing file name");

    for (String source : sourceFiles) {
      try {
        runWithAvroSchema(source);
      } catch (SchemaParseException e) {
        console.debug(
            "Avro schema conversion failed for {}, falling back to Group reader: {}",
            source,
            e.getMessage());
        runWithGroupReader(source);
      }
    }

    return 0;
  }

  private void runWithAvroSchema(String source) throws IOException {
    Schema schema = getAvroSchema(source);
    Schema projection = Expressions.filterSchema(schema, columns);

    Iterable<Object> reader = openDataFile(source, projection);
    boolean threw = true;
    long count = 0;
    try {
      for (Object record : reader) {
        if (numRecords > 0 && count >= numRecords) {
          break;
        }
        if (columns == null || columns.size() != 1) {
          console.info(String.valueOf(record));
        } else {
          console.info(String.valueOf(select(projection, record, columns.get(0))));
        }
        count += 1;
      }
      threw = false;
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed on record " + count + " in file " + source, e);
    } finally {
      if (reader instanceof Closeable) {
        Closeables.close((Closeable) reader, threw);
      }
    }
  }

  private void runWithGroupReader(String source) throws IOException {
    ParquetReader<Group> reader = ParquetReader.<Group>builder(new GroupReadSupport(), qualifiedPath(source))
        .withConf(getConf())
        .build();

    boolean threw = true;
    long count = 0;
    try {
      for (Group record = reader.read(); record != null; record = reader.read()) {
        if (numRecords > 0 && count >= numRecords) {
          break;
        }

        if (columns == null) {
          console.info(record.toString());
        } else {
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            try {
              Object value =
                  record.getValueToString(record.getType().getFieldIndex(columnName), 0);
              if (i > 0) sb.append(", ");
              sb.append(columnName).append(": ").append(value);
            } catch (Exception e) {
              console.warn("Column '{}' not found in file {}", columnName, source);
            }
          }
          if (sb.length() > 0) {
            console.info(sb.toString());
          }
        }
        count += 1;
      }
      threw = false;
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed on record " + count + " in file " + source, e);
    } finally {
      Closeables.close(reader, threw);
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Show the first 10 records in file \"data.avro\":",
        "data.avro",
        "# Show the first 50 records in file \"data.parquet\":",
        "data.parquet -n 50");
  }
}
