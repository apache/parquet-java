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
import java.util.Set;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

/**
 * A cache for caching indexes(including: ColumnIndex, OffsetIndex and BloomFilter)
 */
public interface IndexCache {

  enum CacheStrategy {
    NONE, /* No cache */
    PREFETCH_BLOCK /* Prefetch block indexes */
  }

  /**
   * Create an index cache for the given file reader
   *
   * @param fileReader        the file reader
   * @param columns           the columns that need to do cache
   * @param cacheStrategy     the cache strategy, supports NONE and PREFETCH_BLOCK
   * @param freeCacheAfterGet whether free the given index cache after calling the given get method
   * @return the index cache
   */
  static IndexCache create(
      ParquetFileReader fileReader,
      Set<ColumnPath> columns,
      CacheStrategy cacheStrategy,
      boolean freeCacheAfterGet) {
    if (cacheStrategy == CacheStrategy.NONE) {
      return new NoneIndexCache(fileReader);
    } else if (cacheStrategy == CacheStrategy.PREFETCH_BLOCK) {
      return new PrefetchIndexCache(fileReader, columns, freeCacheAfterGet);
    } else {
      throw new UnsupportedOperationException("Unknown cache strategy: " + cacheStrategy);
    }
  }

  /**
   * Set the current BlockMetadata
   */
  void setBlockMetadata(BlockMetaData currentBlockMetadata) throws IOException;

  /**
   * Get the ColumnIndex for the given column in the set row group.
   *
   * @param chunk the given column chunk
   * @return the ColumnIndex for the given column
   * @throws IOException if any I/O error occurs during get the ColumnIndex
   */
  ColumnIndex getColumnIndex(ColumnChunkMetaData chunk) throws IOException;

  /**
   * Get the OffsetIndex for the given column in the set row group.
   *
   * @param chunk the given column chunk
   * @return the OffsetIndex for the given column
   * @throws IOException if any I/O error occurs during get the OffsetIndex
   */
  OffsetIndex getOffsetIndex(ColumnChunkMetaData chunk) throws IOException;

  /**
   * Get the BloomFilter for the given column in the set row group.
   *
   * @param chunk the given column chunk
   * @return the BloomFilter for the given column
   * @throws IOException if any I/O error occurs during get the BloomFilter
   */
  BloomFilter getBloomFilter(ColumnChunkMetaData chunk) throws IOException;

  /**
   * Clean the cache
   */
  void clean();
}
