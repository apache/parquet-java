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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;

@Parameters(commandDescription = "Print the column sizes of a parquet file")
public class ColumnSizeCommand extends BaseCommand {

  public ColumnSizeCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>")
  String target;

  @Parameter(
      names = {"-c", "--column", "--columns"},
      description = "List of columns in the case sensitive dot format to be calculated, "
          + "for example a.b.c. If an input column is intermediate column, all "
          + "the child columns will be printed out. If no columns are set, all "
          + "the columns will be printed out.",
      required = false)
  List<String> columns;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(target != null, "A Parquet file is required.");

    Path inputFile = new Path(target);
    Map<String, Long> columnSizes = getColumnSizeInBytes(inputFile);
    Map<String, Float> columnRatio = getColumnRatio(columnSizes);

    // If user defined columns, only print out size for those columns
    if (columns != null && !columns.isEmpty()) {
      for (String inputColumn : columns) {
        long size = 0;
        float ratio = 0;
        for (String column : columnSizes.keySet()) {
          if (column.equals(inputColumn) || column.startsWith(inputColumn + ".")) {
            size += columnSizes.get(column);
            ratio += columnRatio.get(column);
          }
        }
        console.info(inputColumn + "->" + " Size In Bytes: " + size + " Size In Ratio: " + ratio);
      }
    } else {
      for (String column : columnSizes.keySet()) {
        console.info(column + "->" + " Size In Bytes: " + columnSizes.get(column) + " Size In Ratio: "
            + columnRatio.get(column));
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print every column size in byte and ratio for a Parquet file",
        "sample.parquet",
        "sample.parquet -c col_1",
        "sample.parquet --column col_2",
        "sample.parquet --columns col_1 col_2",
        "sample.parquet --columns col_1 col_2.sub_col_a");
  }

  // Make it public to allow some automation tools to call it
  public Map<String, Long> getColumnSizeInBytes(Path inputFile) throws IOException {
    Map<String, Long> colSizes = new HashMap<>();
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(new Configuration(), inputFile, ParquetMetadataConverter.NO_FILTER);

    for (BlockMetaData block : pmd.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        String colName = column.getPath().toDotString();
        colSizes.put(colName, column.getTotalSizeWithDecrypt() + colSizes.getOrDefault(colName, 0L));
      }
    }

    return colSizes;
  }

  // Make it public to allow some automation tools to call it
  public Map<String, Float> getColumnRatio(Map<String, Long> colSizes) {
    long totalSize = colSizes.values().stream().reduce(0L, Long::sum);
    Map<String, Float> colRatio = new HashMap<>();

    for (Map.Entry<String, Long> entry : colSizes.entrySet()) {
      colRatio.put(entry.getKey(), ((float) entry.getValue()) / ((float) totalSize));
    }

    return colRatio;
  }
}
