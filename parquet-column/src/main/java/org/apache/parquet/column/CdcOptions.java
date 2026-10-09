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
import org.apache.parquet.internal.column.chunking.RollingHashMask;

/**
 * EXPERIMENTAL: The size envelope and normalization level of content defined chunking (CDC), which
 * ends data pages at boundaries derived from the column's values, so that files sharing a run of
 * values share byte-identical pages. Sizes are measured on values and levels before encoding.
 * Dictionary ids follow the order values first appear in, so an edit that adds new values renumbers
 * the later ones within its row group: columns of many distinct values deduplicate best without
 * dictionary encoding.
 *
 * <p>The settings and defaults match {@code CdcOptions} in Arrow C++ and arrow-rs, and so do the
 * chunk boundaries for the same physical values. Arrow types converted before writing, such as
 * narrow integers, coerced timestamps and decimals, are hashed differently there. Chunk boundaries
 * continue across row groups, as in arrow-rs; Arrow C++ restarts them in every row group, so its
 * chunks match only in the first.
 *
 * @see ParquetProperties.Builder#withContentDefinedChunking(CdcOptions)
 */
public final class CdcOptions {

  /** 256 KiB minimum, 1 MiB maximum and normalization level 0. */
  public static final CdcOptions DEFAULT = builder().build();

  private final long minChunkSize;
  private final long maxChunkSize;
  private final int normLevel;

  private CdcOptions(Builder builder) {
    this.minChunkSize = builder.minChunkSize;
    this.maxChunkSize = builder.maxChunkSize;
    this.normLevel = builder.normLevel;
    // Validate now rather than when the first column writer is built.
    RollingHashMask.calculate(minChunkSize, maxChunkSize, normLevel);
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
    return "CdcOptions{minChunkSize=" + minChunkSize + ", maxChunkSize=" + maxChunkSize + ", normLevel=" + normLevel
        + '}';
  }

  /**
   * @return a builder holding the default options
   */
  public static Builder builder() {
    return new Builder();
  }

  /** EXPERIMENTAL: Builds {@link CdcOptions}. */
  public static class Builder {
    private long minChunkSize = 256 * 1024L;
    private long maxChunkSize = 1024 * 1024L;
    private int normLevel = 0;

    private Builder() {}

    /**
     * Set the minimum chunk size in bytes, 256 KiB by default. The rolling hash is not updated
     * until a chunk reaches this size, so no chunk is shorter but a file's last; pages can be, where
     * a row group or a page limit ends one.
     *
     * @param minChunkSize the minimum chunk size in bytes
     * @return this builder for method chaining
     */
    public Builder withMinChunkSize(long minChunkSize) {
      Preconditions.checkArgument(
          minChunkSize >= 0,
          "Invalid content defined chunking minimum chunk size (negative): %s",
          minChunkSize);
      this.minChunkSize = minChunkSize;
      return this;
    }

    /**
     * Set the maximum chunk size in bytes, 1 MiB by default. A chunk ends when it reaches this size,
     * whatever the rolling hash says. {@link ParquetProperties.Builder#withPageSize(int)} separately
     * limits the page size; below this it splits chunks into more pages, in the same places after
     * an edit, so it does not cost deduplication.
     *
     * @param maxChunkSize the maximum chunk size in bytes
     * @return this builder for method chaining
     */
    public Builder withMaxChunkSize(long maxChunkSize) {
      Preconditions.checkArgument(
          maxChunkSize > 0,
          "Invalid content defined chunking maximum chunk size (not positive): %s",
          maxChunkSize);
      this.maxChunkSize = maxChunkSize;
      return this;
    }

    /**
     * Set the normalization level of the rolling hash mask, 0 by default. Raising it makes a
     * boundary more likely, which tightens the chunk size distribution and improves deduplication
     * at the cost of more small pages; lowering it does the reverse. Values outside
     * {@code [-3, 3]} are not useful.
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
     * @throws IllegalArgumentException if the maximum chunk size is not greater than the minimum,
     *     or the envelope is too narrow for the normalization level
     */
    public CdcOptions build() {
      return new CdcOptions(this);
    }
  }
}
