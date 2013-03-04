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
package parquet.column.mem;

import static parquet.Log.DEBUG;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.io.ParquetEncodingException;


public class MemPageWriter implements PageWriter {
  private static final Log LOG = Log.getLog(MemPageWriter.class);

  private final List<Page> pages = new ArrayList<Page>();
  private long memSize = 0;
  private long totalValueCount = 0;


  @Override
  public void writePage(BytesInput bytesInput, int valueCount, Encoding encoding) throws IOException {
    if (valueCount == 0) {
      throw new ParquetEncodingException("illegal page of 0 values");
    }
    memSize += bytesInput.size();
    pages.add(new Page(BytesInput.copy(bytesInput), valueCount, (int)bytesInput.size(), encoding));
    totalValueCount += valueCount;
    if (DEBUG) LOG.debug("page written for " + bytesInput.size() + " bytes and " + valueCount + " records");
  }

  @Override
  public long getMemSize() {
    return memSize;
  }

  public List<Page> getPages() {
    return pages;
  }

  public long getTotalValueCount() {
    return totalValueCount;
  }

  @Override
  public long allocatedSize() {
    // this store keeps only the bytes written
    return memSize;
  }

}
