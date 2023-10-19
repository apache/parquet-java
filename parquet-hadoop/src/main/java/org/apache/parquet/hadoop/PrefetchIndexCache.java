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

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
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
 * This index cache will prefetch those columns' indexes when calling {@link #setBlockMetadata(BlockMetaData)}.
 * <p>
 *
 * Note: the given index will be freed from the cache after calling the related get method.
 */
class PrefetchIndexCache implements IndexCache {
  private final ParquetFileReader fileReader;
  private final Set<ColumnPath> columns;

  private Map<ColumnPath, ColumnIndex> columnIndexCache;
  private Map<ColumnPath, OffsetIndex> offsetIndexCache;
  private Map<ColumnPath, BloomFilter> bloomIndexCache;

  PrefetchIndexCache(
      ParquetFileReader fileReader,
      Set<ColumnPath> columns) {
    this.fileReader = fileReader;
    this.columns = columns;
  }

  @Override
  public void setBlockMetadata(BlockMetaData currentBlockMetadata) throws IOException {
    this.columnIndexCache = readAllColumnIndexes(currentBlockMetadata);
    this.offsetIndexCache = readAllOffsetIndexes(currentBlockMetadata);
    this.bloomIndexCache = readAllBloomFilters(currentBlockMetadata);
  }

  @Override
  public ColumnIndex getColumnIndex(ColumnChunkMetaData chunk) throws IOException {
    ColumnPath columnPath = chunk.getPath();
    if (columns.contains(columnPath)) {
      Preconditions.checkState(
        columnIndexCache.containsKey(columnPath),
        "Not found cached ColumnIndex for column: %s with cache strategy: %s",
        columnPath.toDotString(),
        CacheStrategy.PRECACHE_BLOCK);
    }

    return columnIndexCache.remove(columnPath);
  }

  @Override
  public OffsetIndex getOffsetIndex(ColumnChunkMetaData chunk) throws IOException {
    ColumnPath columnPath = chunk.getPath();

    if (columns.contains(columnPath)) {
      Preconditions.checkState(
        offsetIndexCache.containsKey(columnPath),
        "Not found cached OffsetIndex for column: %s with cache strategy: %s",
        columnPath.toDotString(),
        CacheStrategy.PRECACHE_BLOCK);
    }

    return offsetIndexCache.remove(columnPath);
  }

  @Override
  public BloomFilter getBloomFilter(ColumnChunkMetaData chunk) throws IOException {
    ColumnPath columnPath = chunk.getPath();

    if (columns.contains(columnPath)) {
      Preconditions.checkState(
        bloomIndexCache.containsKey(columnPath),
        "Not found cached BloomFilter for column: %s with cache strategy: %s",
        columnPath.toDotString(),
        CacheStrategy.PRECACHE_BLOCK);
    }

    return bloomIndexCache.remove(columnPath);
  }

  @Override
  public void clean() {
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
    Map<ColumnPath, ColumnIndex> columnIndexMap = new HashMap<>(columns.size());
    for (ColumnChunkMetaData chunk : blockMetaData.getColumns()) {
      if (columns.contains(chunk.getPath())) {
        columnIndexMap.put(chunk.getPath(), fileReader.readColumnIndex(chunk));
      }
    }

    return columnIndexMap;
  }

  private Map<ColumnPath, OffsetIndex> readAllOffsetIndexes(BlockMetaData blockMetaData) throws IOException {
    Map<ColumnPath, OffsetIndex> offsetIndexMap = new HashMap<>(columns.size());
    for (ColumnChunkMetaData chunk : blockMetaData.getColumns()) {
      if (columns.contains(chunk.getPath())) {
        offsetIndexMap.put(chunk.getPath(), fileReader.readOffsetIndex(chunk));
      }
    }

    return offsetIndexMap;
  }

  private Map<ColumnPath, BloomFilter> readAllBloomFilters(BlockMetaData blockMetaData) throws IOException {
    Map<ColumnPath, BloomFilter> bloomFilterMap = new HashMap<>(columns.size());
    for (ColumnChunkMetaData chunk : blockMetaData.getColumns()) {
      if (columns.contains(chunk.getPath())) {
        bloomFilterMap.put(chunk.getPath(), fileReader.readBloomFilter(chunk));
      }
    }

    return bloomFilterMap;
  }
}
