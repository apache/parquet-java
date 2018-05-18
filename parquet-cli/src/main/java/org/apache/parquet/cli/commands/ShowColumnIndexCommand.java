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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * parquet-cli command to print column and offset indexes.
 */
@Parameters(commandDescription = "Prints the column and offset indexes of a Parquet file")
public class ShowColumnIndexCommand extends BaseCommand {
  public ShowColumnIndexCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>")
  List<String> files;

  @Parameter(names = { "-c", "--column" }, description = "Shows the column/offset indexes for the given column only")
  List<String> ColumnPaths;

  @Parameter(names = { "-b",
      "--block" }, description = "Shows the column/offset indexes for the given block (row-group) only; "
          + "blocks are referenced by their indexes from 0")
  List<String> blockIndexes;

  @Parameter(names = { "-i", "--column-index" }, description = "Shows the column indexes; "
      + "active by default unless -o is used")
  boolean showColumnIndex;

  @Parameter(names = { "-o", "--offset-index" }, description = "Shows the offset indexes; "
      + "active by default unless -i is used")
  boolean showOffsetIndex;

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Show only column indexes for column 'col' from a Parquet file",
        "-c col -i sample.parquet");
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(files != null && files.size() >= 1,
        "A Parquet file is required.");
    Preconditions.checkArgument(files.size() == 1,
        "Cannot process multiple Parquet files.");

    InputFile in = HadoopInputFile.fromPath(new Path(files.get(0)), new Configuration());
    if (!showColumnIndex && !showOffsetIndex) {
      showColumnIndex = showOffsetIndex = true;
    }

    try (ParquetFileReader reader = ParquetFileReader.open(in)) {
      boolean firstBlock = true;
      for (Entry<Integer, BlockMetaData> entry : getBlocks(reader.getFooter())) {
        if (!firstBlock) {
          console.info("");
        }
        firstBlock = false;
        console.info("row group {}:", entry.getKey());
        for (ColumnChunkMetaData column : getColumns(entry.getValue())) {
          String path = column.getPath().toDotString();
          if (showColumnIndex) {
            console.info("column index for column {}:", path);
            ColumnIndex columnIndex = reader.readColumnIndex(column);
            if (columnIndex == null) {
              console.info("NONE");
            } else {
              console.info(columnIndex.toString());
            }
          }
          if (showOffsetIndex) {
            console.info("offset index for column {}:", path);
            OffsetIndex offsetIndex = reader.readOffsetIndex(column);
            if (offsetIndex == null) {
              console.info("NONE");
            } else {
              console.info(offsetIndex.toString());
            }
          }
        }
      }
    }
    return 0;
  }

  // Returns the index-block pairs based on the arguments of --block
  private List<Entry<Integer, BlockMetaData>> getBlocks(ParquetMetadata meta) {
    List<BlockMetaData> blocks = meta.getBlocks();
    List<Entry<Integer, BlockMetaData>> pairs = new ArrayList<>();
    if (blockIndexes == null || blockIndexes.isEmpty()) {
      int index = 0;
      for (BlockMetaData block : blocks) {
        pairs.add(new AbstractMap.SimpleImmutableEntry<>(index++, block));
      }
    } else {
      for (String indexStr : blockIndexes) {
        int index = Integer.parseInt(indexStr);
        pairs.add(new AbstractMap.SimpleImmutableEntry<>(index, blocks.get(index)));
      }
    }
    return pairs;
  }

  private List<ColumnChunkMetaData> getColumns(BlockMetaData block) {
    List<ColumnChunkMetaData> columns = block.getColumns();
    if (ColumnPaths == null || ColumnPaths.isEmpty()) {
      return columns;
    }
    Map<String, ColumnChunkMetaData> pathMap = new HashMap<>();
    for (ColumnChunkMetaData column : columns) {
      pathMap.put(column.getPath().toDotString(), column);
    }

    List<ColumnChunkMetaData> filtered = new ArrayList<>();
    for (String path : ColumnPaths) {
      ColumnChunkMetaData column = pathMap.get(path);
      if (column != null) {
        filtered.add(column);
      }
    }
    return filtered;
  }

}
