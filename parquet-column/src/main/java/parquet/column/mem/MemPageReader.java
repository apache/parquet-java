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

import java.util.Iterator;

import parquet.Log;
import parquet.io.ParquetDecodingException;


public class MemPageReader implements PageReader {
  private static final Log LOG = Log.getLog(MemPageReader.class);

  private long totalValueCount;
  private Iterator<Page> pages;

  public MemPageReader(long totalValueCount, Iterator<Page> pages) {
    super();
    if (pages == null) {
      throw new NullPointerException("pages");
    }
    this.totalValueCount = totalValueCount;
    this.pages = pages;
  }

  @Override
  public long getTotalValueCount() {
    return totalValueCount;
  }

  @Override
  public Page readPage() {
    if (pages.hasNext()) {
      Page next = pages.next();
      if (DEBUG) LOG.debug("read page " + next);
      return next;
    } else {
      throw new ParquetDecodingException("after last page");
    }
  }

}
