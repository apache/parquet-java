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

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.impl.ContentDefinedChunker;

/**
 * EXPERIMENTAL: Options of content defined chunking, which ends data pages at boundaries derived from
 * the values, so that files sharing a run of values share pages. As Arrow C++ and arrow-rs
 * {@code CdcOptions}, with the same defaults. Sizes count values and levels before encoding, at the
 * width Arrow holds them in: an INT(16) value counts 2 bytes, although Parquet stores 4.
 *
 * <p>With dictionary encoding, an edit that adds values renumbers the indices of values first seen
 * after it. As parquet-java chooses between dictionary and plain encoding on the first page of a column
 * chunk, which chunking can cut short, some column chunks fall back to plain encoding.
 *
 * @see ParquetProperties.Builder#withContentDefinedChunking(ContentDefinedChunkingOptions)
 */
public final class ContentDefinedChunkingOptions {

  /** 256 KiB minimum, 1 MiB maximum and normalization level 0. */
  public static final ContentDefinedChunkingOptions DEFAULT = builder().build();

  private final long minChunkSize;
  private final long maxChunkSize;
  private final int normLevel;

  private ContentDefinedChunkingOptions(Builder builder) {
    this.minChunkSize = builder.minChunkSize;
    this.maxChunkSize = builder.maxChunkSize;
    this.normLevel = builder.normLevel;
    Preconditions.checkArgument(
        minChunkSize >= 0, "Invalid content defined chunking minimum chunk size (negative): %s", minChunkSize);
    Preconditions.checkArgument(
        maxChunkSize > minChunkSize,
        "Invalid content defined chunking size range: maximum chunk size (%s) must be greater than minimum chunk size (%s)",
        maxChunkSize,
        minChunkSize);
    // Rejected here rather than once a writer has opened its file
    ContentDefinedChunker.calculateMask(this);
  }

  /**
   * @return the minimum chunk size in bytes
   */
  public long getMinChunkSize() {
    return minChunkSize;
  }

  /**
   * @return the maximum chunk size in bytes
   */
  public long getMaxChunkSize() {
    return maxChunkSize;
  }

  /**
   * @return the normalization level of the rolling hash mask
   */
  public int getNormLevel() {
    return normLevel;
  }

  @Override
  public String toString() {
    return "ContentDefinedChunkingOptions{minChunkSize=" + minChunkSize + ", maxChunkSize=" + maxChunkSize
        + ", normLevel=" + normLevel + '}';
  }

  /**
   * @return a builder holding the default options
   */
  public static Builder builder() {
    return new Builder();
  }

  /** EXPERIMENTAL: Builds {@link ContentDefinedChunkingOptions}. */
  public static class Builder {
    private long minChunkSize = 256 * 1024L;
    private long maxChunkSize = 1024 * 1024L;
    private int normLevel = 0;

    private Builder() {}

    /**
     * Set the minimum chunk size in bytes, 256 KiB by default.
     *
     * @param minChunkSize the minimum chunk size in bytes
     * @return this builder for method chaining
     */
    public Builder withMinChunkSize(long minChunkSize) {
      this.minChunkSize = minChunkSize;
      return this;
    }

    /**
     * Set the maximum chunk size in bytes, 1 MiB by default.
     *
     * @param maxChunkSize the maximum chunk size in bytes
     * @return this builder for method chaining
     */
    public Builder withMaxChunkSize(long maxChunkSize) {
      this.maxChunkSize = maxChunkSize;
      return this;
    }

    /**
     * Set the normalization level of the rolling hash mask, 0 by default. Raising it makes chunk
     * sizes more uniform at the cost of more small pages.
     *
     * @param normLevel the normalization level
     * @return this builder for method chaining
     */
    public Builder withNormLevel(int normLevel) {
      this.normLevel = normLevel;
      return this;
    }

    /**
     * @return the options
     * @throws IllegalArgumentException if the minimum is negative, the maximum not above it, or the
     *     range too narrow for the normalization level
     */
    public ContentDefinedChunkingOptions build() {
      return new ContentDefinedChunkingOptions(this);
    }
  }
}
