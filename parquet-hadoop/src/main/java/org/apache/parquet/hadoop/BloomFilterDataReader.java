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
import org.apache.parquet.Strings;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterReader;
import org.apache.parquet.column.values.bloomfilter.BloomFilterUtility;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.ParquetDecodingException;
/**
 * A {@link BloomFilterReader} implementation that reads Bloom filter data from
 * an open {@link ParquetFileReader}.
 *
 */
public class BloomFilterDataReader implements BloomFilterReader {
  private final ParquetFileReader reader;
  private final Map<String, ColumnChunkMetaData> columns;
  private final Map<String, BloomFilter> cache = new HashMap<>();

  public BloomFilterDataReader(ParquetFileReader fileReader, BlockMetaData block) {
    this.reader = fileReader;
    this.columns = new HashMap<>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      columns.put(column.getPath().toDotString(), column);
    }
  }

  @Override
  public BloomFilter readBloomFilter(ColumnDescriptor descriptor) {
    String dotPath = Strings.join(descriptor.getPath(), ".");
    ColumnChunkMetaData column = columns.get(dotPath);
    if (column == null) {
      throw new ParquetDecodingException(
        "Cannot read Bloom filter data from meta data of column: " + dotPath);
    }
    if (cache.containsKey(dotPath)) {
      return cache.get(dotPath);
    }
    try {
      synchronized (cache) {
        if (!cache.containsKey(dotPath)) {
          BloomFilter bloomFilter = reader.readBloomFilter(column);
          if (bloomFilter == null) return null;
          cache.put(dotPath, bloomFilter);
        }
      }
      return cache.get(dotPath);
    } catch (IOException e) {
      throw new ParquetDecodingException(
        "Failed to read Bloom filter data", e);
    }
  }

  @Override
  public BloomFilterUtility buildBloomFilterUtility(ColumnDescriptor desc) {
    BloomFilter bloomFilterData = readBloomFilter(desc);

    String dotPath = Strings.join(desc.getPath(), ".");
    ColumnChunkMetaData meta = columns.get(dotPath);
    if (meta == null) {
      throw new ParquetDecodingException(
        "Cannot read Bloom filter data from meta data of column: " + dotPath);
    }

    switch (meta.getType()){
      case INT32:
        return new BloomFilterUtility.IntBloomFilter(bloomFilterData);
      case INT64:
        return new BloomFilterUtility.LongBloomFilter(bloomFilterData);
      case FLOAT:
        return new BloomFilterUtility.FloatBloomFilter(bloomFilterData);
      case DOUBLE:
        return new BloomFilterUtility.DoubleBloomFilter(bloomFilterData);
      case BINARY:
        return new BloomFilterUtility.BinaryBloomFilter(bloomFilterData);
      default:
        return null;
    }
  }

}
