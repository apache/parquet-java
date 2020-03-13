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

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.Main;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnSizeCommand extends ArgsOnlyCommand {

  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to calculate teh column size" +
    "     [<column> ...] are the columns in the case sensitive dot format" +
    "     to be calculated, for example a.b.c. If no columns are input, all the" +
    "     columns will be printed out"
  };

  /**
   * Biggest number of columns we can calculate.
   */
  private static final int MAX_COL_NUM = 100;

  public ColumnSizeCommand() {
    super(1, 1 + MAX_COL_NUM);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Print out the size in bytes and percentage of column(s) in the input Parquet file";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);
    List<String> args = options.getArgList();
    Path inputFile = new Path(args.get(0));

    Map<String, Long> columnSizes = getColumnSizeInBytes(inputFile);
    Map<String, Float> columnPercentage = getColumnPercentage(columnSizes);

    if (args.size() > 1) {
      for (String inputColumn : args.subList(1, args.size())) {
        long size = 0;
        float percentage = 0;
        for (String column : columnSizes.keySet()) {
          if (column.startsWith(inputColumn)) {
            size += columnSizes.get(column);
            percentage += columnPercentage.get(column);
          }
        }
        Main.out.println(inputColumn + "->" + " Size In Bytes: " + size + " Size In Percentage: " + percentage);
      }
    } else {
      for (String column : columnSizes.keySet()) {
        Main.out.println(column + "->" + " Size In Bytes: " + columnSizes.get(column)
          + " Size In Percentage: " + columnPercentage.get(column));
      }
    }
  }

  // Make it public to allow some automation tools to call it
  public Map<String, Long> getColumnSizeInBytes(Path inputFile) throws IOException {
    Map<String, Long> colSizes = new HashMap<>();
    ParquetMetadata pmd = ParquetFileReader.readFooter(new Configuration(), inputFile, ParquetMetadataConverter.NO_FILTER);

    for (BlockMetaData block : pmd.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        String colName = column.getPath().toDotString();
        colSizes.put(colName, column.getTotalSize() + colSizes.getOrDefault(colName, 0L));
      }
    }

    return colSizes;
  }

  // Make it public to allow some automation tools to call it
  public Map<String, Float> getColumnPercentage(Map<String, Long> colSizes) {
    long totalSize = colSizes.values().stream().reduce(0L, Long::sum);
    Map<String, Float> colPercentage = new HashMap<>();

    for (Map.Entry<String, Long> entry : colSizes.entrySet()) {
      colPercentage.put(entry.getKey(), ((float) entry.getValue()) / ((float) totalSize));
    }

    return colPercentage;
  }
}

