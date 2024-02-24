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

import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.AdaptiveBlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter;
import org.apache.parquet.io.api.Binary;

// An internal class to collect column values to build column statistics and bloom filter.
class ColumnValueCollector {

  private final ColumnDescriptor path;
  private final BloomFilterWriter bloomFilterWriter;
  private final BloomFilter bloomFilter;
  private Statistics<?> statistics;
  private SizeStatistics.Builder sizeStatisticsBuilder;

  ColumnValueCollector(ColumnDescriptor path, BloomFilterWriter bloomFilterWriter, ParquetProperties props) {
    this.path = path;
    resetPageStatistics();

    this.bloomFilterWriter = bloomFilterWriter;
    if (bloomFilterWriter == null) {
      this.bloomFilter = null;
      return;
    }
    int maxBloomFilterSize = props.getMaxBloomFilterBytes();

    OptionalLong ndv = props.getBloomFilterNDV(path);
    OptionalDouble fpp = props.getBloomFilterFPP(path);
    // If user specify the column NDV, we construct Bloom filter from it.
    if (ndv.isPresent()) {
      int optimalNumOfBits = BlockSplitBloomFilter.optimalNumOfBits(ndv.getAsLong(), fpp.getAsDouble());
      this.bloomFilter = new BlockSplitBloomFilter(optimalNumOfBits / 8, maxBloomFilterSize);
    } else {
      if (props.getAdaptiveBloomFilterEnabled(path)) {
        int numCandidates = props.getBloomFilterCandidatesCount(path);
        this.bloomFilter =
            new AdaptiveBlockSplitBloomFilter(maxBloomFilterSize, numCandidates, fpp.getAsDouble(), path);
      } else {
        this.bloomFilter = new BlockSplitBloomFilter(maxBloomFilterSize, maxBloomFilterSize);
      }
    }
  }

  void resetPageStatistics() {
    this.statistics = Statistics.createStats(path.getPrimitiveType());
    this.sizeStatisticsBuilder = SizeStatistics.newBuilder(
        path.getPrimitiveType(), path.getMaxRepetitionLevel(), path.getMaxDefinitionLevel());
  }

  void writeNull(int repetitionLevel, int definitionLevel) {
    statistics.incrementNumNulls();
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
  }

  void write(boolean value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
  }

  void write(int value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  void write(long value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  void write(float value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  void write(double value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  void write(Binary value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel, value);
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  void finalizeColumnChunk() {
    if (bloomFilterWriter != null && bloomFilter != null) {
      bloomFilterWriter.writeBloomFilter(bloomFilter);
    }
  }

  Statistics<?> getStatistics() {
    return statistics;
  }

  SizeStatistics getSizeStatistics() {
    return sizeStatisticsBuilder.build();
  }
}
