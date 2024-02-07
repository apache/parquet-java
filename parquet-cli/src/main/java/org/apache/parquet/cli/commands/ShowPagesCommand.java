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

import static org.apache.parquet.cli.Util.columnName;
import static org.apache.parquet.cli.Util.descriptor;
import static org.apache.parquet.cli.Util.encodingAsString;
import static org.apache.parquet.cli.Util.humanReadable;
import static org.apache.parquet.cli.Util.minMaxAsString;
import static org.apache.parquet.cli.Util.primitive;
import static org.apache.parquet.cli.Util.shortCodec;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.TextStringBuilder;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.rawpages.RawPagesReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.Page;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;

@Parameters(commandDescription = "Print page summaries for a Parquet file")
public class ShowPagesCommand extends BaseCommand {

  public ShowPagesCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>")
  List<String> targets;

  @Parameter(
      names = {"-c", "--column", "--columns"},
      description = "List of columns")
  List<String> columns;

  @Parameter(
      names = {"-r", "--raw"},
      description = "List the original Thrift page headers and file offsets")
  boolean raw = false;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() >= 1, "A Parquet file is required.");
    Preconditions.checkArgument(targets.size() == 1, "Cannot process multiple Parquet files.");

    // Even though the implementation is separated the functionality is logically related so placed in the same
    // command.
    if (raw) {
      try (RawPagesReader reader =
          new RawPagesReader(HadoopInputFile.fromPath(qualifiedPath(targets.get(0)), getConf()), columns)) {
        reader.listPages(console);
      }
      return 0;
    }

    String source = targets.get(0);
    try (ParquetFileReader reader = ParquetFileReader.open(getConf(), qualifiedPath(source))) {
      MessageType schema = reader.getFileMetaData().getSchema();
      Map<ColumnDescriptor, PrimitiveType> columns = Maps.newLinkedHashMap();
      if (this.columns == null || this.columns.isEmpty()) {
        for (ColumnDescriptor descriptor : schema.getColumns()) {
          columns.put(descriptor, primitive(schema, descriptor.getPath()));
        }
      } else {
        for (String column : this.columns) {
          columns.put(descriptor(column, schema), primitive(column, schema));
        }
      }

      CompressionCodecName codec =
          reader.getRowGroups().get(0).getColumns().get(0).getCodec();
      // accumulate formatted lines to print by column
      Map<String, List<String>> formatted = Maps.newLinkedHashMap();
      PageFormatter formatter = new PageFormatter();
      PageReadStore pageStore;
      int rowGroupNum = 0;
      while ((pageStore = reader.readNextRowGroup()) != null) {
        for (ColumnDescriptor descriptor : columns.keySet()) {
          List<String> lines = formatted.get(columnName(descriptor));
          if (lines == null) {
            lines = Lists.newArrayList();
            formatted.put(columnName(descriptor), lines);
          }

          formatter.setContext(rowGroupNum, columns.get(descriptor), codec);
          PageReader pages = pageStore.getPageReader(descriptor);

          DictionaryPage dict = pages.readDictionaryPage();
          if (dict != null) {
            lines.add(formatter.format(dict));
          }
          DataPage page;
          while ((page = pages.readPage()) != null) {
            lines.add(formatter.format(page));
          }
        }
        rowGroupNum += 1;
      }

      // TODO: Show total column size and overall size per value in the column summary line
      for (String columnName : formatted.keySet()) {
        console.info(String.format(
            "\nColumn: %s\n%s", columnName, new TextStringBuilder(80).appendPadding(80, '-')));
        console.info(formatter.getHeader());
        for (String line : formatted.get(columnName)) {
          console.info(line);
        }
        console.info("");
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList("# Show pages for column 'col' from a Parquet file", "-c col sample.parquet");
  }

  private class PageFormatter implements DataPage.Visitor<String> {
    private int rowGroupNum;
    private int pageNum;
    private PrimitiveType type;
    private String shortCodec;

    String getHeader() {
      return String.format(
          "  %-6s %-5s %-4s %-7s %-10s %-10s %-8s %-7s %s",
          "page", "type", "enc", "count", "avg size", "size", "rows", "nulls", "min / max");
    }

    void setContext(int rowGroupNum, PrimitiveType type, CompressionCodecName codec) {
      this.rowGroupNum = rowGroupNum;
      this.pageNum = 0;
      this.type = type;
      this.shortCodec = shortCodec(codec);
    }

    String format(Page page) {
      String formatted = "";
      if (page instanceof DictionaryPage) {
        formatted = printDictionaryPage((DictionaryPage) page);
      } else if (page instanceof DataPage) {
        formatted = ((DataPage) page).accept(this);
      }
      pageNum += 1;
      return formatted;
    }

    private String printDictionaryPage(DictionaryPage dict) {
      // TODO: the compressed size of a dictionary page is lost in Parquet
      dict.getUncompressedSize();
      long totalSize = dict.getCompressedSize();
      int count = dict.getDictionarySize();
      float perValue = ((float) totalSize) / count;
      String enc = encodingAsString(dict.getEncoding(), true);
      if (pageNum == 0) {
        return String.format(
            "%3d-D    %-5s %s %-2s %-7d %-10s %-10s",
            rowGroupNum, "dict", shortCodec, enc, count, humanReadable(perValue), humanReadable(totalSize));
      } else {
        return String.format(
            "%3d-%-3d  %-5s %s %-2s %-7d %-10s %-10s",
            rowGroupNum,
            pageNum,
            "dict",
            shortCodec,
            enc,
            count,
            humanReadable(perValue),
            humanReadable(totalSize));
      }
    }

    @Override
    public String visit(DataPageV1 page) {
      String enc = encodingAsString(page.getValueEncoding(), false);
      long totalSize = page.getCompressedSize();
      int count = page.getValueCount();
      String numNulls = page.getStatistics().isNumNullsSet()
          ? Long.toString(page.getStatistics().getNumNulls())
          : "";
      float perValue = ((float) totalSize) / count;
      String minMax = minMaxAsString(page.getStatistics());
      return String.format(
          "%3d-%-3d  %-5s %s %-2s %-7d %-10s %-10s %-8s %-7s %s",
          rowGroupNum,
          pageNum,
          "data",
          shortCodec,
          enc,
          count,
          humanReadable(perValue),
          humanReadable(totalSize),
          "",
          numNulls,
          minMax);
    }

    @Override
    public String visit(DataPageV2 page) {
      String enc = encodingAsString(page.getDataEncoding(), false);
      long totalSize = page.getCompressedSize();
      int count = page.getValueCount();
      int numRows = page.getRowCount();
      int numNulls = page.getNullCount();
      float perValue = ((float) totalSize) / count;
      String minMax = minMaxAsString(page.getStatistics());
      String compression = (page.isCompressed() ? shortCodec : "_");
      return String.format(
          "%3d-%-3d  %-5s %s %-2s %-7d %-10s %-10s %-8d %-7s %s",
          rowGroupNum,
          pageNum,
          "data",
          compression,
          enc,
          count,
          humanReadable(perValue),
          humanReadable(totalSize),
          numRows,
          numNulls,
          minMax);
    }
  }
}
