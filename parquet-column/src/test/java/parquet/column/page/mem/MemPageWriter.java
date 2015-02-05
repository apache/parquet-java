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
package parquet.column.page.mem;

import static parquet.Log.DEBUG;
import static parquet.bytes.BytesInput.copy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.DictionaryPage;
import parquet.column.page.DataPage;
import parquet.column.page.PageWriter;
import parquet.column.statistics.Statistics;
import parquet.io.ParquetEncodingException;

public class MemPageWriter implements PageWriter {
  private static final Log LOG = Log.getLog(MemPageWriter.class);

  private final List<DataPage> pages = new ArrayList<DataPage>();
  private DictionaryPage dictionaryPage;
  private long memSize = 0;
  private long totalValueCount = 0;

  @Override
  public void writePage(BytesInput bytesInput, int valueCount, Statistics statistics, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding)
      throws IOException {
    if (valueCount == 0) {
      throw new ParquetEncodingException("illegal page of 0 values");
    }
    memSize += bytesInput.size();
    pages.add(new DataPageV1(BytesInput.copy(bytesInput), valueCount, (int)bytesInput.size(), statistics, rlEncoding, dlEncoding, valuesEncoding));
    totalValueCount += valueCount;
    if (DEBUG) LOG.debug("page written for " + bytesInput.size() + " bytes and " + valueCount + " records");
  }

  @Override
  public void writePageV2(int rowCount, int nullCount, int valueCount,
      BytesInput repetitionLevels, BytesInput definitionLevels,
      Encoding dataEncoding, BytesInput data, Statistics<?> statistics) throws IOException {
    if (valueCount == 0) {
      throw new ParquetEncodingException("illegal page of 0 values");
    }
    long size = repetitionLevels.size() + definitionLevels.size() + data.size();
    memSize += size;
    pages.add(DataPageV2.uncompressed(rowCount, nullCount, valueCount, copy(repetitionLevels), copy(definitionLevels), dataEncoding, copy(data), statistics));
    totalValueCount += valueCount;
    if (DEBUG) LOG.debug("page written for " + size + " bytes and " + valueCount + " records");

  }

  @Override
  public long getMemSize() {
    return memSize;
  }

  public List<DataPage> getPages() {
    return pages;
  }

  public DictionaryPage getDictionaryPage() {
    return dictionaryPage;
  }

  public long getTotalValueCount() {
    return totalValueCount;
  }

  @Override
  public long allocatedSize() {
    // this store keeps only the bytes written
    return memSize;
  }

  @Override
  public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
    if (this.dictionaryPage != null) {
      throw new ParquetEncodingException("Only one dictionary page per block");
    }
    this.memSize += dictionaryPage.getBytes().size();
    this.dictionaryPage = dictionaryPage.copy();
    if (DEBUG) LOG.debug("dictionary page written for " + dictionaryPage.getBytes().size() + " bytes and " + dictionaryPage.getDictionarySize() + " records");
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s %,d bytes", prefix, memSize);

  }

}
