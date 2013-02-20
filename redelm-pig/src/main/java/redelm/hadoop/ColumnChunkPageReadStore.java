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
package redelm.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redelm.Log;
import redelm.column.ColumnDescriptor;
import redelm.column.mem.Page;
import redelm.column.mem.PageReadStore;
import redelm.column.mem.PageReader;
import redelm.hadoop.CodecFactory.BytesDecompressor;

public class ColumnChunkPageReadStore implements PageReadStore {
  private static final Log LOG = Log.getLog(ColumnChunkPageReadStore.class);

  static final class ColumnChunkPageReader implements PageReader {

    private final BytesDecompressor decompressor;
    private final long valueCount;
    private final List<Page> pages = new ArrayList<Page>();
    private int currentPage = 0;

    ColumnChunkPageReader(BytesDecompressor decompressor, long valueCount) {
      this.decompressor = decompressor;
      this.valueCount = valueCount;
    }

    public void addPage(Page page) {
      pages.add(page);
    }

    @Override
    public long getTotalValueCount() {
      return valueCount;
    }

    @Override
    public Page readPage() {
      if (currentPage == pages.size()) {
        return null;
      } else {
        try {
          Page compressedPage= pages.get(currentPage);
          ++ currentPage;
          return new Page(
              decompressor.decompress(compressedPage.getBytes(), compressedPage.getUncompressedSize()),
              compressedPage.getValueCount(),
              compressedPage.getUncompressedSize());
        } catch (IOException e) {
          throw new RuntimeException(e); // TODO: cleanup
        }
      }
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageReader> readers = new HashMap<ColumnDescriptor, ColumnChunkPageReader>();
  private final long rowCount;

  public ColumnChunkPageReadStore(long rowCount) {
    this.rowCount = rowCount;
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor path) {
    if (!readers.containsKey(path)) {
      throw new IllegalArgumentException(path + " is not in the store");
    }
    return readers.get(path);
  }

  void addColumn(ColumnDescriptor path, ColumnChunkPageReader reader) {
    if (readers.put(path, reader) != null) {
      throw new RuntimeException(path+ " was added twice");
    }
  }

}
