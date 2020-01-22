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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.tools.Main;

/**
 * parquet-tools command to print column and offset indexes.
 */
public class ColumnIndexCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] { "<input>",
      "where <input> is the parquet file to print the column and offset indexes for" };

  public static final Options OPTIONS;
  static {
    OPTIONS = new Options();
    OPTIONS.addOption(Option.builder("c").longOpt("column").desc(
        "Shows the column/offset indexes for the given column only; " + "multiple columns shall be separated by commas")
        .hasArg().build());
    OPTIONS.addOption(Option.builder("r").longOpt("row-group")
        .desc("Shows the column/offset indexes for the given row-groups only; "
            + "multiple row-groups shall be speparated by commas; "
            + "row-groups are referenced by their indexes from 0")
        .hasArg().build());
    OPTIONS.addOption(Option.builder("i").longOpt("column-index")
        .desc("Shows the column indexes; " + "active by default unless -o is used").hasArg(false).build());
    OPTIONS.addOption(Option.builder("o").longOpt("offset-index")
        .desc("Shows the offset indexes; " + "active by default unless -i is used").hasArg(false).build());
  }

  public ColumnIndexCommand() {
    super(1, 1);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Prints the column and offset indexes of a Parquet file.";
  }

  @Override
  public Options getOptions() {
    return OPTIONS;
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    String[] args = options.getArgs();
    InputFile in = HadoopInputFile.fromPath(new Path(args[0]), new Configuration());
    PrintWriter out = new PrintWriter(Main.out, true);
    String rowGroupValue = options.getOptionValue("r");
    Set<String> indexes = new HashSet<>();
    if (rowGroupValue != null) {
      indexes.addAll(Arrays.asList(rowGroupValue.split("\\s*,\\s*")));
    }
    boolean showColumnIndex = options.hasOption("i");
    boolean showOffsetIndex = options.hasOption("o");
    if (!showColumnIndex && !showOffsetIndex) {
      showColumnIndex = true;
      showOffsetIndex = true;
    }

    try (ParquetFileReader reader = ParquetFileReader.open(in)) {
      boolean firstBlock = true;
      int rowGroupIndex = 0;
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        if (!indexes.isEmpty() && !indexes.contains(Integer.toString(rowGroupIndex))) {
          ++rowGroupIndex;
          continue;
        }
        if (!firstBlock) {
          out.println();
          firstBlock = false;
        }
        out.format("row group %d:%n", rowGroupIndex);
        for (ColumnChunkMetaData column : getColumns(block, options)) {
          String path = column.getPath().toDotString();
          if (showColumnIndex) {
            out.format("column index for column %s:%n", path);
            ColumnIndex columnIndex = reader.readColumnIndex(column);
            if (columnIndex == null) {
              out.println("NONE");
            } else {
              out.println(columnIndex);
            }
          }
          if (showOffsetIndex) {
            out.format("offset index for column %s:%n", path);
            OffsetIndex offsetIndex = reader.readOffsetIndex(column);
            if (offsetIndex == null) {
              out.println("NONE");
            } else {
              out.println(offsetIndex);
            }
          }
        }
        ++rowGroupIndex;
      }
    }
  }

  private static List<ColumnChunkMetaData> getColumns(BlockMetaData block, CommandLine options) {
    List<ColumnChunkMetaData> columns = block.getColumns();
    String pathValue = options.getOptionValue("c");
    if (pathValue == null) {
      return columns;
    }
    String[] paths = pathValue.split("\\s*,\\s*");
    Map<String, ColumnChunkMetaData> pathMap = new HashMap<>();
    for (ColumnChunkMetaData column : columns) {
      pathMap.put(column.getPath().toDotString(), column);
    }

    List<ColumnChunkMetaData> filtered = new ArrayList<>();
    for (String path : paths) {
      ColumnChunkMetaData column = pathMap.get(path);
      if (column != null) {
        filtered.add(column);
      }
    }
    return filtered;
  }

}
