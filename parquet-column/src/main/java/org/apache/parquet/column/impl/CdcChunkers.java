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
package org.apache.parquet.column.impl;

import java.util.HashMap;
import java.util.Map;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.schema.MessageType;

/**
 * Internal: the content defined chunkers of one file, one per leaf column. Every row group's store is
 * made here and shares them, so chunk boundaries continue across row groups instead of restarting in
 * each, as in arrow-rs. Create one per file.
 */
public final class CdcChunkers {

  private final Map<ColumnDescriptor, CdcChunker> chunkers = new HashMap<>();

  /**
   * Makes the column write store of one row group of this file.
   *
   * @param props the writer properties
   * @param schema the schema of the row group
   * @param pageStore the page store of the row group
   * @param bloomFilterWriteStore the bloom filter store of the row group
   * @return the column write store
   */
  public ColumnWriteStore newColumnWriteStore(
      ParquetProperties props,
      MessageType schema,
      PageWriteStore pageStore,
      BloomFilterWriteStore bloomFilterWriteStore) {
    switch (props.getWriterVersion()) {
      case PARQUET_1_0:
        return new ColumnWriteStoreV1(schema, pageStore, bloomFilterWriteStore, props, this);
      case PARQUET_2_0:
        return new ColumnWriteStoreV2(schema, pageStore, bloomFilterWriteStore, props, this);
      default:
        throw new IllegalArgumentException("unknown version " + props.getWriterVersion());
    }
  }

  CdcChunker chunker(ColumnDescriptor path, CdcOptions options) {
    return chunkers.computeIfAbsent(path, p -> new CdcChunker(options, p));
  }
}
