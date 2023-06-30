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
package org.apache.parquet.cli.rawpages;

import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

import java.io.IOException;

import org.apache.parquet.cli.util.RawUtils;
import org.apache.parquet.format.CliUtils;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;

public class RawPagesReader implements AutoCloseable {

  private final SeekableInputStream input;
  private final FileMetaData footer;

  public RawPagesReader(InputFile file) throws IOException {
    long fileLen = file.getLength();

    if (fileLen < MAGIC.length + 4 + MAGIC.length) {
      throw new RuntimeException("Not a Parquet file (length is too low: " + fileLen + ")");
    }

    input = file.newStream();
    footer = RawUtils.readFooter(input, fileLen);
  }

  public void listPages(Logger console) throws IOException {
    for (int i = 0, n = footer.getRow_groupsSize(); i < n; ++i) {
      RowGroup rowGroup = footer.getRow_groups().get(i);
      for (ColumnChunk columnChunk : rowGroup.getColumns()) {
        ColumnMetaData metaData = columnChunk.getMeta_data();
        String path = String.join(".", metaData.getPath_in_schema());
        long totalSize = metaData.getTotal_compressed_size();
        long dictOffset = metaData.getDictionary_page_offset();
        long seekTo = metaData.getData_page_offset();
        console.info(
          "Start of chunk (rowGroup: {}, columnName: {}, dictPageOffset: {}, dataPageOffset: {}, numValues: {}, totalSize: {})",
          i, path, metaData.isSetDictionary_page_offset() ? dictOffset : "-", seekTo, metaData.getNum_values(),
          totalSize);
        if (metaData.isSetDictionary_page_offset() && dictOffset > 0 && dictOffset < seekTo) {
          seekTo = metaData.getDictionary_page_offset();
        }
        input.seek(seekTo);
        long endPos = seekTo + totalSize;
        int pageIndex = 0;
        for (long offset = input.getPos(); offset < endPos; offset = input.getPos()) {
          PageHeader pageHeader = Util.readPageHeader(input);
          console.info("Page {}. (offset: {}, headerSize: {})\n{}", pageIndex++, offset, (input.getPos() - offset),
            RawUtils.prettifyJson(CliUtils.toJson(pageHeader)));
          input.skip(pageHeader.getCompressed_page_size());
        }
        if (input.getPos() != endPos) {
          console.warn("!!! Current file offset does not match with the total size of the chunk in the footer: {}",
            input.getPos());
        } else {
          console.info("End of chunk (offset: {})", (endPos - 1));
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    input.close();
  }
}
