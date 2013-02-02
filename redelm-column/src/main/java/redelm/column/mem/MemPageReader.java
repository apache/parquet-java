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
package redelm.column.mem;

import static redelm.Log.DEBUG;

import java.util.Iterator;

import redelm.Log;

public class MemPageReader implements PageReader {
  private static final Log LOG = Log.getLog(MemPageReader.class);

  private int totalValueCount;
  private Iterator<Page> pages;

  public MemPageReader(int totalValueCount, Iterator<Page> pages) {
    super();
    this.totalValueCount = totalValueCount;
    this.pages = pages;
  }

  @Override
  public int getTotalValueCount() {
    return totalValueCount;
  }

  @Override
  public Page readPage() {
    if (pages.hasNext()) {
      Page next = pages.next();
      if (DEBUG) LOG.debug("read page " + next);
      return next;
    } else {
      throw new RuntimeException("after last page");
    }
  }

}
