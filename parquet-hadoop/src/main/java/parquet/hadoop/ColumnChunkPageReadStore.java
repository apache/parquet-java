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
package parquet.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.mem.Page;
import parquet.column.mem.PageReadStore;
import parquet.column.mem.PageReader;
import parquet.hadoop.CodecFactory.BytesDecompressor;

/**
 * TODO: should this actually be called RowGroupImpl or something?
 * The name is kind of confusing since it references three different "entities"
 * in our format: columns, chunks, and pages
 * 
 */
class ColumnChunkPageReadStore implements PageReadStore {
  private static final Log LOG = Log.getLog(ColumnChunkPageReadStore.class);

  /**
   * PageReader for a single column chunk. A column chunk contains
   * several pages, which are yielded one by one in order.
   *
   * This implementation is provided with a list of pages, each of which
   * is decompressed and passed through. 
   */
  static final class ColumnChunkPageReader implements PageReader {

    private final BytesDecompressor decompressor;
    private final long valueCount;
    private final List<Page> compressedPages;

    ColumnChunkPageReader(BytesDecompressor decompressor, List<Page> compressedPages) {
      this.decompressor = decompressor;
      this.compressedPages = new LinkedList<Page>(compressedPages);
      int count = 0;
      for (Page p : compressedPages) {
        count += p.getValueCount();
      }
      this.valueCount = count;
    }

    @Override
    public long getTotalValueCount() {
      return valueCount;
    }

    @Override
    public Page readPage() {
      if (compressedPages.isEmpty()) {
        return null;
      }
      Page compressedPage = compressedPages.remove(0); 
      try {
        return new Page(
            decompressor.decompress(compressedPage.getBytes(), compressedPage.getUncompressedSize()),
            compressedPage.getValueCount(),
            compressedPage.getUncompressedSize(),
            compressedPage.getEncoding());
      } catch (IOException e) {
        throw new RuntimeException(e); // TODO: cleanup
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
