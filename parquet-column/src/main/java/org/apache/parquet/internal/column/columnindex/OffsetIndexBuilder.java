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
package org.apache.parquet.internal.column.columnindex;

import java.util.Formatter;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/**
 * Builder implementation to create {@link OffsetIndex} objects during writing a parquet file.
 */
public class OffsetIndexBuilder {

  private static class OffsetIndexImpl implements OffsetIndex {
    private long[] offsets;
    private int[] compressedPageSizes;
    private long[] firstRowIndexes;

    @Override
    public String toString() {
      try (Formatter formatter = new Formatter()) {
        formatter.format("%-10s  %20s  %16s  %20s\n", "", "offset", "compressed size", "first row index");
        for (int i = 0, n = offsets.length; i < n; ++i) {
          formatter.format("page-%-5d  %20d  %16d  %20d\n", i, offsets[i], compressedPageSizes[i], firstRowIndexes[i]);
        }
        return formatter.toString();
      }
    }

    @Override
    public int getPageCount() {
      return offsets.length;
    }

    @Override
    public long getOffset(int pageIndex) {
      return offsets[pageIndex];
    }

    @Override
    public int getCompressedPageSize(int pageIndex) {
      return compressedPageSizes[pageIndex];
    }

    @Override
    public long getFirstRowIndex(int pageIndex) {
      return firstRowIndexes[pageIndex];
    }
    
    @Override
    public short getPageOrdinal(int pageIndex) {
      if (pageIndex > Short.MAX_VALUE) {
        throw new RuntimeException("Page ordinal exceeds limit " + pageIndex);
      }
      return (short) pageIndex;
    }
  }

  private static final OffsetIndexBuilder NO_OP_BUILDER = new OffsetIndexBuilder() {
    @Override
    public void add(int compressedPageSize, long rowCount) {
    }

    @Override
    public void add(long offset, int compressedPageSize, long rowCount) {
    }
  };

  private final LongList offsets = new LongArrayList();
  private final IntList compressedPageSizes = new IntArrayList();
  private final LongList firstRowIndexes = new LongArrayList();
  private long previousOffset;
  private int previousPageSize;
  private long previousRowIndex;
  private long previousRowCount;

  /**
   * @return a no-op builder that does not collect values and therefore returns {@code null} at {@link #build(long)}
   */
  public static OffsetIndexBuilder getNoOpBuilder() {
    return NO_OP_BUILDER;
  }

  /**
   * @return an {@link OffsetIndexBuilder} instance to build an {@link OffsetIndex} object
   */
  public static OffsetIndexBuilder getBuilder() {
    return new OffsetIndexBuilder();
  }

  private OffsetIndexBuilder() {
  }

  /**
   * Adds the specified parameters to this builder. Used by the writers to building up {@link OffsetIndex} objects to be
   * written to the Parquet file.
   *
   * @param compressedPageSize
   *          the size of the page (including header)
   * @param rowCount
   *          the number of rows in the page
   */
  public void add(int compressedPageSize, long rowCount) {
    add(previousOffset + previousPageSize, compressedPageSize, previousRowIndex + previousRowCount);
    previousRowCount = rowCount;
  }

  /**
   * Adds the specified parameters to this builder. Used by the metadata converter to building up {@link OffsetIndex}
   * objects read from the Parquet file.
   *
   * @param offset
   *          the offset of the page in the file
   * @param compressedPageSize
   *          the size of the page (including header)
   * @param firstRowIndex
   *          the index of the first row in the page (within the row group)
   */
  public void add(long offset, int compressedPageSize, long firstRowIndex) {
    previousOffset = offset;
    offsets.add(offset);
    previousPageSize = compressedPageSize;
    compressedPageSizes.add(compressedPageSize);
    previousRowIndex = firstRowIndex;
    firstRowIndexes.add(firstRowIndex);
  }

  /**
   * Builds the offset index. Used by the metadata converter to building up {@link OffsetIndex}
   * objects read from the Parquet file.
   *
   * @return the newly created offset index or {@code null} if the {@link OffsetIndex} object would be empty
   */
  public OffsetIndex build() {
    return build(0);
  }

  /**
   * Builds the offset index. Used by the writers to building up {@link OffsetIndex} objects to be
   * written to the Parquet file.
   *
   * @param firstPageOffset
   *          the actual offset in the file to be used to translate all the collected offsets
   * @return the newly created offset index or {@code null} if the {@link OffsetIndex} object would be empty
   */
  public OffsetIndex build(long firstPageOffset) {
    if (compressedPageSizes.isEmpty()) {
      return null;
    }
    long[] offsets = this.offsets.toLongArray();
    if (firstPageOffset != 0) {
      for (int i = 0, n = offsets.length; i < n; ++i) {
        offsets[i] += firstPageOffset;
      }
    }
    OffsetIndexImpl offsetIndex = new OffsetIndexImpl();
    offsetIndex.offsets = offsets;
    offsetIndex.compressedPageSizes = compressedPageSizes.toIntArray();
    offsetIndex.firstRowIndexes = firstRowIndexes.toLongArray();

    return offsetIndex;
  }

}

