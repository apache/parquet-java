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

import org.apache.parquet.Strings;
import org.apache.parquet.column.values.bloom.Bloom;
import org.apache.parquet.column.values.bloom.BloomDataReadStore;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BloomDataReader implements BloomDataReadStore {
  private final ParquetFileReader reader;
  private final Map<String, ColumnChunkMetaData> columns;
  private final Map<String, Bloom> cache = new HashMap<>();

  public BloomDataReader(ParquetFileReader fileReader, BlockMetaData block) {
    this.reader = fileReader;
    this.columns = new HashMap<>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      columns.put(column.getPath().toDotString(), column);
    }
  }

  @Override
  public Bloom readBloomData(ColumnDescriptor descriptor) {
    String dotPath = Strings.join(descriptor.getPath(), ".");
    ColumnChunkMetaData column = columns.get(dotPath);
    if (column == null) {
      throw new ParquetDecodingException(
        "Cannot load Bloom data, unknown column: " + dotPath);
    }

    if (cache.containsKey(dotPath)) {
      return cache.get(dotPath);
    }

    try {
      synchronized (cache) {
        if (!cache.containsKey(dotPath)) {
          Bloom bloom = reader.readBloomData(column);
          if (bloom == null) return null;
          cache.put(dotPath, bloom);
        }
      }

      return cache.get(dotPath);
    } catch (IOException e) {
      throw new ParquetDecodingException(
        "Failed to read Bloom data", e);
    }
  }
}
