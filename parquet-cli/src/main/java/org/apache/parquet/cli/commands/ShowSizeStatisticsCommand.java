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

import static org.apache.parquet.cli.Util.humanReadable;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.commons.text.TextStringBuilder;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;

@Parameters(commandDescription = "Print size statistics for a Parquet file")
public class ShowSizeStatisticsCommand extends BaseCommand {

  public ShowSizeStatisticsCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>")
  List<String> targets;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && !targets.isEmpty(), "A Parquet file is required.");
    Preconditions.checkArgument(targets.size() == 1, "Cannot process multiple Parquet files.");

    String source = targets.get(0);
    try (ParquetFileReader reader = ParquetFileReader.open(getConf(), qualifiedPath(source))) {
      ParquetMetadata footer = reader.getFooter();
      MessageType schema = footer.getFileMetaData().getSchema();

      console.info("\nFile path: {}", source);

      List<BlockMetaData> rowGroups = footer.getBlocks();
      for (int index = 0, n = rowGroups.size(); index < n; index++) {
        printRowGroupSizeStats(console, index, rowGroups.get(index), schema);
        console.info("");
      }
    }

    return 0;
  }

  private void printRowGroupSizeStats(Logger console, int index, BlockMetaData rowGroup, MessageType schema) {
    int maxColumnWidth = Math.max(
        "column".length(),
        rowGroup.getColumns().stream()
            .map(col -> col.getPath().toString().length())
            .max(Integer::compare)
            .orElse(0));

    console.info(String.format("\nRow group %d\n%s", index, new TextStringBuilder(80).appendPadding(80, '-')));

    String formatString = String.format("%%-%ds %%-15s %%-40s %%-40s", maxColumnWidth);
    console.info(
        String.format(formatString, "column", "unencoded bytes", "rep level histogram", "def level histogram"));

    for (ColumnChunkMetaData column : rowGroup.getColumns()) {
      printColumnSizeStats(console, column, schema, maxColumnWidth);
    }
  }

  private void printColumnSizeStats(Logger console, ColumnChunkMetaData column, MessageType schema, int columnWidth) {
    SizeStatistics stats = column.getSizeStatistics();

    if (stats != null && stats.isValid()) {
      String unencodedBytes = stats.getUnencodedByteArrayDataBytes().isPresent()
          ? humanReadable(stats.getUnencodedByteArrayDataBytes().get())
          : "-";
      List<Long> repLevels = stats.getRepetitionLevelHistogram();
      String repLevelsString = (repLevels != null && !repLevels.isEmpty()) ? repLevels.toString() : "-";
      List<Long> defLevels = stats.getDefinitionLevelHistogram();
      String defLevelsString = (defLevels != null && !defLevels.isEmpty()) ? defLevels.toString() : "-";
      String formatString = String.format("%%-%ds %%-15s %%-40s %%-40s", columnWidth);
      console.info(
          String.format(formatString, column.getPath(), unencodedBytes, repLevelsString, defLevelsString));
    } else {
      String formatString = String.format("%%-%ds %%-15s %%-40s %%-40s", columnWidth);
      console.info(String.format(formatString, column.getPath(), "-", "-", "-"));
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList("# Show size statistics for a Parquet file", "sample.parquet");
  }
}
