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

import java.io.IOException;
import java.io.OutputStream;
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
  private final boolean statisticsEnabled;
  private final boolean sizeStatisticsEnabled;
  private BloomFilterWriter bloomFilterWriter;
  private BloomFilter bloomFilter;
  private Statistics<?> statistics;
  private SizeStatistics.Builder sizeStatisticsBuilder;

  ColumnValueCollector(ColumnDescriptor path, BloomFilterWriter bloomFilterWriter, ParquetProperties props) {
    this.path = path;
    this.statisticsEnabled = props.getStatisticsEnabled(path);
    this.sizeStatisticsEnabled = props.getSizeStatisticsEnabled(path);
    resetPageStatistics();
    initBloomFilter(bloomFilterWriter, props);
  }

  void resetPageStatistics() {
    this.statistics = statisticsEnabled
        ? Statistics.createStats(path.getPrimitiveType())
        : Statistics.noopStats(path.getPrimitiveType());
    this.sizeStatisticsBuilder = sizeStatisticsEnabled
        ? SizeStatistics.newBuilder(
            path.getPrimitiveType(), path.getMaxRepetitionLevel(), path.getMaxDefinitionLevel())
        : SizeStatistics.noopBuilder(
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
    bloomFilter.insertHash(bloomFilter.hash(value));
  }

  void write(long value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
    bloomFilter.insertHash(bloomFilter.hash(value));
  }

  void write(float value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
    bloomFilter.insertHash(bloomFilter.hash(value));
  }

  void write(double value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel);
    bloomFilter.insertHash(bloomFilter.hash(value));
  }

  void write(Binary value, int repetitionLevel, int definitionLevel) {
    statistics.updateStats(value);
    sizeStatisticsBuilder.add(repetitionLevel, definitionLevel, value);
    bloomFilter.insertHash(bloomFilter.hash(value));
  }

  void initBloomFilter(BloomFilterWriter bloomFilterWriter, ParquetProperties props) {
    this.bloomFilterWriter = bloomFilterWriter;
    if (bloomFilterWriter == null) {
      this.bloomFilter = new BloomFilter() {
        @Override
        public void writeTo(OutputStream out) throws IOException {}

        @Override
        public void insertHash(long hash) {}

        @Override
        public boolean findHash(long hash) {
          return false;
        }

        @Override
        public int getBitsetSize() {
          return 0;
        }

        @Override
        public long hash(int value) {
          return 0;
        }

        @Override
        public long hash(long value) {
          return 0;
        }

        @Override
        public long hash(double value) {
          return 0;
        }

        @Override
        public long hash(float value) {
          return 0;
        }

        @Override
        public long hash(Binary value) {
          return 0;
        }

        @Override
        public long hash(Object value) {
          return 0;
        }

        @Override
        public HashStrategy getHashStrategy() {
          return null;
        }

        @Override
        public Algorithm getAlgorithm() {
          return null;
        }

        @Override
        public Compression getCompression() {
          return null;
        }
      };
      return;
    }

    int maxBloomFilterSize = props.getMaxBloomFilterBytes();
    OptionalLong ndv = props.getBloomFilterNDV(path);
    OptionalDouble fpp = props.getBloomFilterFPP(path);
    // If user specify the column NDV, we construct Bloom filter from it.
    if (ndv.isPresent()) {
      int optimalNumOfBits = BlockSplitBloomFilter.optimalNumOfBits(ndv.getAsLong(), fpp.getAsDouble());
      this.bloomFilter = new BlockSplitBloomFilter(optimalNumOfBits / 8, maxBloomFilterSize);
    } else if (props.getAdaptiveBloomFilterEnabled(path)) {
      int numCandidates = props.getBloomFilterCandidatesCount(path);
      this.bloomFilter =
          new AdaptiveBlockSplitBloomFilter(maxBloomFilterSize, numCandidates, fpp.getAsDouble(), path);
    } else {
      this.bloomFilter = new BlockSplitBloomFilter(maxBloomFilterSize, maxBloomFilterSize);
    }
  }

  void finalizeColumnChunk() {
    if (bloomFilterWriter != null) {
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
