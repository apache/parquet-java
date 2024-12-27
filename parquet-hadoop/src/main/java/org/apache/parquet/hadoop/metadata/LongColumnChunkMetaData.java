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
package org.apache.parquet.hadoop.metadata;

import java.util.Set;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

class LongColumnChunkMetaData extends ColumnChunkMetaData {

  private final long firstDataPageOffset;
  private final long dictionaryPageOffset;
  private final long valueCount;
  private final long totalSize;
  private final long totalUncompressedSize;
  private final Statistics statistics;
  private final SizeStatistics sizeStatistics;

  private LongColumnChunkMetaData(Builder builder) {
    super(builder);
    this.firstDataPageOffset = builder.firstDataPageOffset;
    this.dictionaryPageOffset = builder.dictionaryPageOffset;
    this.valueCount = builder.valueCount;
    this.totalSize = builder.totalSize;
    this.totalUncompressedSize = builder.totalUncompressedSize;
    this.statistics = builder.statistics;
    this.sizeStatistics = builder.sizeStatistics;
  }

  /**
   * @return start of the column data offset
   */
  public long getFirstDataPageOffset() {
    return firstDataPageOffset;
  }

  /**
   * @return the location of the dictionary page if any
   */
  public long getDictionaryPageOffset() {
    return dictionaryPageOffset;
  }

  /**
   * @return count of values in this block of the column
   */
  public long getValueCount() {
    return valueCount;
  }

  /**
   * @return the totalUncompressedSize
   */
  public long getTotalUncompressedSize() {
    return totalUncompressedSize;
  }

  /**
   * @return the totalSize
   */
  public long getTotalSize() {
    return totalSize;
  }

  /**
   * @return the stats for this column
   */
  public Statistics getStatistics() {
    return statistics;
  }

  /**
   * @return the size stats for this column
   */
  @Override
  public SizeStatistics getSizeStatistics() {
    return sizeStatistics;
  }

  public static class Builder extends ColumnChunkMetaData.Builder<Builder> {
    private long firstDataPageOffset;
    private long dictionaryPageOffset;
    private long valueCount;
    private long totalSize;
    private long totalUncompressedSize;
    private Statistics statistics;
    private SizeStatistics sizeStatistics;
    private ColumnPath path;
    private PrimitiveType type;
    private CompressionCodecName codec;
    private Set<Encoding> encodings;

    public Builder withFirstDataPageOffset(long firstDataPageOffset) {
      this.firstDataPageOffset = firstDataPageOffset;
      return this;
    }

    public Builder withDictionaryPageOffset(long dictionaryPageOffset) {
      this.dictionaryPageOffset = dictionaryPageOffset;
      return this;
    }

    public Builder withValueCount(long valueCount) {
      this.valueCount = valueCount;
      return this;
    }

    public Builder withTotalSize(long totalSize) {
      this.totalSize = totalSize;
      return this;
    }

    public Builder withTotalUncompressedSize(long totalUncompressedSize) {
      this.totalUncompressedSize = totalUncompressedSize;
      return this;
    }

    public Builder withStatistics(Statistics statistics) {
      this.statistics = statistics;
      return this;
    }

    public Builder withSizeStatistics(SizeStatistics sizeStatistics) {
      this.sizeStatistics = sizeStatistics;
      return this;
    }

    public Builder withPath(ColumnPath path) {
      this.path = path;
      return this;
    }

    public Builder withPrimitiveType(PrimitiveType type) {
      this.type = type;
      return this;
    }

    public Builder withCodec(CompressionCodecName codec) {
      this.codec = codec;
      return this;
    }

    public Builder withEncodings(Set<Encoding> encodings) {
      this.encodings = encodings;
      return this;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public ColumnChunkMetaData build() {
      ColumnChunkProperties columnChunkProperties =
          ColumnChunkProperties.get(this.path, this.type, this.codec, this.encodings);
      this.withProperties(columnChunkProperties);
      return new LongColumnChunkMetaData(this);
    }
  }
}
