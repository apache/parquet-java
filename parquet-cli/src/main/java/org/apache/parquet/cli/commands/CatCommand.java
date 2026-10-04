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
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Expressions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
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
      if (isParquetFile(source)) {
        runWithGroupReader(source);
      } else {
        runWithAvroSchema(source);
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
    List<String> projectedColumns = uniqueColumns(columns);
    ParquetReader<Group> reader = openParquetGroupReader(source, projectedColumns);

    boolean threw = true;
    long count = 0;
    try {
      for (Group record = reader.read(); record != null; record = reader.read()) {
        if (numRecords > 0 && count >= numRecords) {
          break;
        }

        if (projectedColumns == null || projectedColumns.isEmpty() || projectedColumns.size() > 1) {
          console.info(record.toString());
        } else {
          console.info(selectGroupValue(record, projectedColumns.get(0)));
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

  private String selectGroupValue(Group record, String column) {
    String[] path = column.split("\\.");
    Group group = record;
    for (int i = 0; i < path.length - 1; i++) {
      group = group.getGroup(group.getType().getFieldIndex(path[i]), 0);
    }
    return group.getValueToString(group.getType().getFieldIndex(path[path.length - 1]), 0);
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
