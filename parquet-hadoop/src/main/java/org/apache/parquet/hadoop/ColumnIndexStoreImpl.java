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
import java.util.Set;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal implementation of {@link ColumnIndexStore}.
 */
class ColumnIndexStoreImpl implements ColumnIndexStore {

  private interface IndexStore {
    ColumnIndex getColumnIndex();

    OffsetIndex getOffsetIndex();
  }

  private class IndexStoreImpl implements IndexStore {
    private final ColumnChunkMetaData meta;
    private ColumnIndex columnIndex;
    private boolean columnIndexRead;
    private OffsetIndex offsetIndex;
    private boolean offsetIndexRead;

    IndexStoreImpl(ColumnChunkMetaData meta) {
      this.meta = meta;
    }

    @Override
    public ColumnIndex getColumnIndex() {
      if (!columnIndexRead) {
        try {
          columnIndex = reader.readColumnIndex(meta);
        } catch (IOException e) {
          // If the I/O issue still stands it will fail the reading later;
          // otherwise we fail the filtering only with a missing column index.
          logger.error("Unable to read column index for column {}", meta.getPath(), e);
        }
        columnIndexRead = true;
      }
      return columnIndex;
    }

    @Override
    public OffsetIndex getOffsetIndex() {
      if (!offsetIndexRead) {
        try {
          offsetIndex = reader.readOffsetIndex(meta);
        } catch (IOException e) {
          // If the I/O issue still stands it will fail the reading later;
          // otherwise we fail the filtering only with a missing offset index.
          logger.error("Unable to read offset index for column {}", meta.getPath(), e);
        }
        offsetIndexRead = true;
      }
      return offsetIndex;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(ColumnIndexStoreImpl.class);
  // Used for columns are not in this parquet file
  private static final IndexStore MISSING_INDEX_STORE = new IndexStore() {
    @Override
    public ColumnIndex getColumnIndex() {
      return null;
    }

    @Override
    public OffsetIndex getOffsetIndex() {
      return null;
    }
  };

  private final ParquetFileReader reader;
  private final Map<ColumnPath, IndexStore> store;

  ColumnIndexStoreImpl(ParquetFileReader reader, BlockMetaData block, Set<ColumnPath> paths) {
    // TODO[GS]: Offset index for every paths will be required; pre-read the consecutive ones at once?
    // TODO[GS]: Pre-read column index based on filter?
    this.reader = reader;
    Map<ColumnPath, IndexStore> store = new HashMap<>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      ColumnPath path = column.getPath();
      if (paths.contains(path)) {
        store.put(path, new IndexStoreImpl(column));
      }
    }
    this.store = store;
  }

  @Override
  public ColumnIndex getColumnIndex(ColumnPath column) {
    return store.getOrDefault(column, MISSING_INDEX_STORE).getColumnIndex();
  }

  @Override
  public OffsetIndex getOffsetIndex(ColumnPath column) {
    return store.getOrDefault(column, MISSING_INDEX_STORE).getOffsetIndex();
  }

}
