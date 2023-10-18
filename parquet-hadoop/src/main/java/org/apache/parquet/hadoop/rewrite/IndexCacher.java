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
package org.apache.parquet.hadoop.rewrite;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A cacher for caching file indexes(ColumnIndex, OffsetIndex, BloomFilter)
 */
class IndexCacher {
  private final ParquetFileReader fileReader;
  private final Set<ColumnPath> columnPathSet;
  private final boolean prefetchBlockAllIndexes;

  // Only used when cacheBlockIndexInOnce is true
  private Map<ColumnPath, ColumnIndex> columnIndexCache;
  private Map<ColumnPath, OffsetIndex> offsetIndexCache;
  private Map<ColumnPath, BloomFilter> bloomIndexCache;

  IndexCacher(
      ParquetFileReader fileReader,
      Set<ColumnPath> columnPathSet,
      boolean prefetchBlockAllIndexes) {
    this.fileReader = fileReader;
    this.columnPathSet = columnPathSet;
    this.prefetchBlockAllIndexes = prefetchBlockAllIndexes;
    if (prefetchBlockAllIndexes) {
      this.columnIndexCache = new HashMap<>();
      this.offsetIndexCache = new HashMap<>();
      this.bloomIndexCache = new HashMap<>();
    } else {
      this.columnIndexCache = null;
      this.offsetIndexCache = null;
      this.bloomIndexCache = null;
    }
  }

  void setCurrentBlockMetadata(BlockMetaData blockMetaData) throws IOException {
    if (prefetchBlockAllIndexes) {
      free();
      this.columnIndexCache = readAllColumnIndexes(blockMetaData);
      this.offsetIndexCache = readAllOffsetIndexes(blockMetaData);
      this.bloomIndexCache = readAllBloomFilters(blockMetaData);
    }
  }

  ColumnIndex getColumnIndex(ColumnChunkMetaData chunk) throws IOException {
    if (prefetchBlockAllIndexes) {
      return columnIndexCache.remove(chunk.getPath());
    }

    return fileReader.readColumnIndex(chunk);
  }

  OffsetIndex getOffsetIndex(ColumnChunkMetaData chunk) throws IOException {
    if (prefetchBlockAllIndexes) {
      return offsetIndexCache.remove(chunk.getPath());
    }

    return fileReader.readOffsetIndex(chunk);
  }

  BloomFilter getBloomFilter(ColumnChunkMetaData chunk) throws IOException {
    if (prefetchBlockAllIndexes) {
      return bloomIndexCache.remove(chunk.getPath());
    }

    return fileReader.readBloomFilter(chunk);
  }

  void free() {
    if (columnIndexCache != null) {
      columnIndexCache.clear();
      columnIndexCache = null;
    }

    if (offsetIndexCache != null) {
      offsetIndexCache.clear();
      offsetIndexCache = null;
    }

    if (bloomIndexCache != null) {
      bloomIndexCache.clear();
      bloomIndexCache = null;
    }
  }

  private Map<ColumnPath, ColumnIndex> readAllColumnIndexes(BlockMetaData blockMetaData) throws IOException {
    Map<ColumnPath, ColumnIndex> columnIndexMap = new HashMap<>(columnPathSet.size());
    for (ColumnChunkMetaData chunk : blockMetaData.getColumns()) {
      if (columnPathSet.contains(chunk.getPath())) {
        columnIndexMap.put(chunk.getPath(), fileReader.readColumnIndex(chunk));
      }
    }

    return columnIndexMap;
  }

  private Map<ColumnPath, OffsetIndex> readAllOffsetIndexes(BlockMetaData blockMetaData) throws IOException {
    Map<ColumnPath, OffsetIndex> offsetIndexMap = new HashMap<>(columnPathSet.size());
    for (ColumnChunkMetaData chunk : blockMetaData.getColumns()) {
      if (columnPathSet.contains(chunk.getPath())) {
        offsetIndexMap.put(chunk.getPath(), fileReader.readOffsetIndex(chunk));
      }
    }

    return offsetIndexMap;
  }

  private Map<ColumnPath, BloomFilter> readAllBloomFilters(BlockMetaData blockMetaData) throws IOException {
    Map<ColumnPath, BloomFilter> bloomFilterMap = new HashMap<>(columnPathSet.size());
    for (ColumnChunkMetaData chunk : blockMetaData.getColumns()) {
      if (columnPathSet.contains(chunk.getPath())) {
        bloomFilterMap.put(chunk.getPath(), fileReader.readBloomFilter(chunk));
      }
    }

    return bloomFilterMap;
  }
}
