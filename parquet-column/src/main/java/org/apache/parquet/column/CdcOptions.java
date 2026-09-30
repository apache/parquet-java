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

import java.util.Objects;
import org.apache.parquet.Preconditions;
import org.apache.parquet.internal.column.chunking.RollingHashMask;

/**
 * EXPERIMENTAL: How to chunk, when content defined chunking (CDC) is enabled: the size envelope and
 * the normalization level.
 *
 * <p>Content defined chunking ends a data page at a boundary derived from the content of the
 * column rather than from a size or row count, so that files sharing a run of values also share
 * byte-identical data pages and a content addressable storage (CAS) system can deduplicate them.
 * The sizes here are measured on the logical values, before any encoding or compression, and
 * everything fed to the rolling hash counts towards them, definition and repetition levels
 * included.
 *
 * <p>Shaped after {@code parquet::CdcOptions} in Arrow C++ and {@code CdcOptions} in arrow-rs: the
 * same three settings with the same defaults, because chunk boundaries deduplicate across
 * implementations only while the settings agree as well as the algorithm does.
 *
 * <p>Boundaries deduplicate against those two for the types all three hash the same bytes for:
 * {@code BOOLEAN}, {@code INT32}, {@code INT64}, {@code FLOAT}, {@code DOUBLE},
 * {@code BYTE_ARRAY} and {@code FIXED_LEN_BYTE_ARRAY} written from the corresponding Arrow type.
 * They do not where Arrow hashes something other than what Parquet stores, which happens because
 * Arrow chunks its own values and converts them afterwards:
 *
 * <ul>
 *   <li>{@code int8}, {@code int16}, {@code uint8} and {@code uint16}, hashed there as one or two
 *       bytes and stored here as {@code INT32}. ({@code uint32} and {@code uint64} are a
 *       reinterpretation of the same bytes, so those do agree.)
 *   <li>Timestamps Arrow converts on the way out -- to the unit {@code coerce_timestamps} names,
 *       nanoseconds to microseconds when it writes Parquet format 1.0 or 2.4, and seconds to
 *       milliseconds always -- and {@code INT96}, hashed there as the eight-byte timestamp the
 *       twelve-byte value is built from.
 *   <li>Decimals, hashed there as Arrow's own little-endian 16- or 32-byte buffer.
 *   <li>{@code date64} and {@code time32(s)}, both converted before writing. ({@code half_float}
 *       is stored as the same two little-endian bytes Arrow hashes, so it does agree.)
 *   <li>Arrow arrays that are themselves dictionary encoded, where Arrow hashes the four-byte
 *       indices rather than the values. This is the Arrow array's encoding, not Parquet's.
 * </ul>
 *
 * <p>Arrow C++ refuses to chunk outside its Arrow write path, so it never chunks those columns at
 * all. This writer has a single entry point and chunks them anyway, on boundaries of its own.
 *
 * <p>Instances are immutable and are built through {@link #builder()}. A size envelope that cannot
 * produce a usable rolling hash mask is rejected there, so an instance is valid by construction.
 *
 * <p>This API is experimental and may change or be removed in a future release.
 *
 * @see ParquetProperties.Builder#withContentDefinedChunking(CdcOptions)
 */
public final class CdcOptions {

  /** The options used when content defined chunking is enabled without any of its own. */
  public static final CdcOptions DEFAULT = builder().build();

  private final long minChunkSize;
  private final long maxChunkSize;
  private final int normLevel;

  private CdcOptions(Builder builder) {
    this.minChunkSize = builder.minChunkSize;
    this.maxChunkSize = builder.maxChunkSize;
    this.normLevel = builder.normLevel;
    // Deriving the mask is what validates the envelope, so an unusable one is rejected here
    // rather than when the first column writer is built.
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CdcOptions)) {
      return false;
    }
    CdcOptions other = (CdcOptions) o;
    return minChunkSize == other.minChunkSize && maxChunkSize == other.maxChunkSize && normLevel == other.normLevel;
  }

  @Override
  public int hashCode() {
    return Objects.hash(minChunkSize, maxChunkSize, normLevel);
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
     * Set the smallest chunk, in bytes.
     *
     * <p>The rolling hash is not updated until a chunk reaches this size, so no chunk is shorter
     * than it.
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
     * Set the largest chunk, in bytes.
     *
     * <p>A new chunk starts whenever the current one reaches this size, whatever the rolling hash
     * says. Note that {@link ParquetProperties.Builder#withPageSize(int)} is a related but separate
     * limit on the size of a page <i>after</i> encoding: setting it below the maximum chunk size
     * does not reduce how well chunking deduplicates, but it does produce more, smaller pages.
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
     * Set the normalization level of the rolling hash mask.
     *
     * <p>Raising it makes a boundary more likely, which tightens the chunk size distribution around
     * the average and improves the deduplication ratio at the cost of more small pages; lowering it
     * does the reverse. Values outside {@code [-3, 3]} are not useful.
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
     * @throws IllegalArgumentException if the size envelope cannot produce a usable rolling hash
     *     mask
     */
    public CdcOptions build() {
      return new CdcOptions(this);
    }
  }
}
