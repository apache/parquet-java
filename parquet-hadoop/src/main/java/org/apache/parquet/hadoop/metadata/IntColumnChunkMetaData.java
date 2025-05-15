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

class IntColumnChunkMetaData extends ColumnChunkMetaData {

  private final int firstDataPage;
  private final int dictionaryPageOffset;
  private final int valueCount;
  private final int totalSize;
  private final int totalUncompressedSize;
  private final Statistics statistics;
  private final SizeStatistics sizeStatistics;

  private IntColumnChunkMetaData(Builder builder) {
    super(builder);
    this.firstDataPage = builder.firstDataPage;
    this.dictionaryPageOffset = builder.dictionaryPageOffset;
    this.valueCount = builder.valueCount;
    this.totalSize = builder.totalSize;
    this.totalUncompressedSize = builder.totalUncompressedSize;
    this.statistics = builder.statistics;
    this.sizeStatistics = builder.sizeStatistics;
  }

  /**
   * turns the int back into a positive long
   *
   * @param value
   * @return
   */
  private long intToPositiveLong(int value) {
    return (long) value - Integer.MIN_VALUE;
  }

  /**
   * @return start of the column data offset
   */
  public long getFirstDataPageOffset() {
    return intToPositiveLong(firstDataPage);
  }

  /**
   * @return the location of the dictionary page if any
   */
  public long getDictionaryPageOffset() {
    return intToPositiveLong(dictionaryPageOffset);
  }

  /**
   * @return count of values in this block of the column
   */
  public long getValueCount() {
    return intToPositiveLong(valueCount);
  }

  /**
   * @return the totalUncompressedSize
   */
  public long getTotalUncompressedSize() {
    return intToPositiveLong(totalUncompressedSize);
  }

  /**
   * @return the totalSize
   */
  public long getTotalSize() {
    return intToPositiveLong(totalSize);
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
    private int firstDataPage;
    private int dictionaryPageOffset;
    private int valueCount;
    private int totalSize;
    private int totalUncompressedSize;
    private Statistics statistics;
    private SizeStatistics sizeStatistics;
    private ColumnPath path;
    private PrimitiveType type;
    private CompressionCodecName codec;
    private Set<Encoding> encodings;

    public Builder withFirstDataPage(int firstDataPage) {
      this.firstDataPage = firstDataPage;
      return this;
    }

    public Builder withDictionaryPageOffset(int dictionaryPageOffset) {
      this.dictionaryPageOffset = dictionaryPageOffset;
      return this;
    }

    public Builder withValueCount(int valueCount) {
      this.valueCount = valueCount;
      return this;
    }

    public Builder withTotalSize(int totalSize) {
      this.totalSize = totalSize;
      return this;
    }

    public Builder withTotalUncompressedSize(int totalUncompressedSize) {
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
      return new IntColumnChunkMetaData(this);
    }
  }
}
