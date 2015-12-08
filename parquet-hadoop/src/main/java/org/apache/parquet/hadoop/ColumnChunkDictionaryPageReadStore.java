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
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.Ints;
import org.apache.parquet.Log;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.DictionaryPageReader;
import org.apache.parquet.hadoop.CodecFactory.BytesDecompressor;

class ColumnChunkDictionaryPageReadStore implements DictionaryPageReadStore {
  /**
   * DictionaryPageReader for a single column chunk. A column chunk contains
   * several pages, first of which could be a dictionary page.
   *
   * This implementation is provided with compressed dictionary page
   */
  static final class ColumnChunkDictionaryPageReader implements DictionaryPageReader {

    private final BytesDecompressor decompressor;
    private final DictionaryPage compressedDictionaryPage;

    ColumnChunkDictionaryPageReader(BytesDecompressor decompressor, DictionaryPage compressedDictionaryPage) {
      this.decompressor = decompressor;
      this.compressedDictionaryPage = compressedDictionaryPage;
    }

    @Override
    public int getDictionarySize() {
      return compressedDictionaryPage.getDictionarySize();
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (compressedDictionaryPage == null) {
        return null;
      }
      try {
        return new DictionaryPage(
            decompressor.decompress(compressedDictionaryPage.getBytes(), compressedDictionaryPage.getUncompressedSize()),
            compressedDictionaryPage.getDictionarySize(),
            compressedDictionaryPage.getEncoding());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkDictionaryPageReader> readers = new HashMap<ColumnDescriptor, ColumnChunkDictionaryPageReader>();

  public ColumnChunkDictionaryPageReadStore() {
  }

  @Override
  public DictionaryPageReader getDictionaryPageReader(ColumnDescriptor column) {
    if (!readers.containsKey(column)) {
      throw new IllegalArgumentException(column + " is not in the store: " + readers.keySet());
    }
    return readers.get(column);
  }

  void addColumn(ColumnDescriptor column, ColumnChunkDictionaryPageReader reader) {
    if (readers.put(column, reader) != null) {
      throw new RuntimeException(column + " was added twice");
    }
  }
}
