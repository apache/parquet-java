/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.page.mem;

import static parquet.Log.DEBUG;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import parquet.Log;
import parquet.column.page.DataPage;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageWriter;
import parquet.io.ParquetEncodingException;


public class MemPageWriter implements PageWriter {
  private static final Log LOG = Log.getLog(MemPageWriter.class);

  private final List<DataPage> pages = new ArrayList<DataPage>();
  private DictionaryPage dictionaryPage;
  private long memSize = 0;
  private long totalValueCount = 0;


  @Override
  public void writeDataPage(DataPage dataPage)
      throws IOException {
    if (dataPage.getValueCount() == 0) {
      throw new ParquetEncodingException("illegal page of 0 values");
    }
    memSize += dataPage.getBytes().size();
    pages.add(dataPage.copy());
    totalValueCount += dataPage.getValueCount();
    if (DEBUG) LOG.debug("page written for " + dataPage.getBytes().size() + " bytes and " + dataPage.getValueCount() + " records");
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
