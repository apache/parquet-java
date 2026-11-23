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
package org.apache.parquet.column;

import org.apache.parquet.column.impl.ContentDefinedChunker;

/**
 * EXPERIMENTAL: The settings of content defined chunking, as Arrow C++ and arrow-rs {@code CdcOptions},
 * with the same defaults. Sizes count values and levels before encoding, at the width Arrow holds them
 * in: an INT(16) value counts 2 bytes, although Parquet stores 4.
 *
 * @see ParquetProperties.Builder#withContentDefinedChunking(ContentDefinedChunkingOptions)
 */
public final class ContentDefinedChunkingOptions {

  /** 256 KiB minimum, 1 MiB maximum and normalization level 0. */
  public static final ContentDefinedChunkingOptions DEFAULT =
      new ContentDefinedChunkingOptions(256 * 1024, 1024 * 1024, 0);

  private final long minChunkSize;
  private final long maxChunkSize;
  private final int normLevel;

  /**
   * @param minChunkSize the minimum chunk size in bytes. The rolling hash will not be updated until this
   *     size is reached for each chunk. Note that all data sent through the hash function is counted
   *     towards the chunk size, including definition and repetition levels if present.
   * @param maxChunkSize the maximum chunk size in bytes. The chunker creates a new chunk whenever the
   *     chunk size exceeds this value. The chunk size distribution approximates a normal distribution
   *     between minChunkSize and maxChunkSize. The page size and row count limits of the writer still
   *     apply within a chunk.
   * @param normLevel the normalization level to center the chunk size around the average size more
   *     aggressively. Increasing the normalization level increases the probability of finding a chunk
   *     boundary, improving the deduplication ratio, but also increases the number of small chunks
   *     resulting in many small data pages. The default 0 provides a good balance between deduplication
   *     ratio and fragmentation. Use 1 or 2 to reach a higher deduplication ratio at the expense of
   *     fragmentation.
   * @throws IllegalArgumentException if the minimum is negative, the maximum not above it, or the range
   *     too narrow for the normalization level
   */
  public ContentDefinedChunkingOptions(long minChunkSize, long maxChunkSize, int normLevel) {
    ContentDefinedChunker.calculateMask(minChunkSize, maxChunkSize, normLevel);
    this.minChunkSize = minChunkSize;
    this.maxChunkSize = maxChunkSize;
    this.normLevel = normLevel;
  }

  public long getMinChunkSize() {
    return minChunkSize;
  }

  public long getMaxChunkSize() {
    return maxChunkSize;
  }

  public int getNormLevel() {
    return normLevel;
  }
}
