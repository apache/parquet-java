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

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

/**
 * Cached, per-physical-column-chunk page state used by the Approach 2 (micro-row-group)
 * reader path.
 *
 * <p>An Approach 2 file represents the row groups in its footer as <em>logical</em>
 * micro-row-groups that share a common physical column chunk per column. Multiple
 * {@link org.apache.parquet.hadoop.metadata.BlockMetaData} entries point at overlapping
 * page ranges in their {@link OffsetIndex} sidecars, and a single physical {@link DataPage}
 * may straddle the boundary between adjacent micro-row-groups.
 *
 * <p>This class is the in-memory model of that physical column chunk after IO + page-header
 * parse. It holds:
 * <ul>
 *   <li>The compressed {@link DictionaryPage} (or {@code null} if the column has none).</li>
 *   <li>The full, ordered list of compressed {@link DataPage}s for the chunk.</li>
 *   <li>The OffsetIndex that describes those pages with <em>file-absolute</em>
 *       {@code first_row_index} values.</li>
 *   <li>The {@link ColumnChunkMetaData} from the first physical row group in the group,
 *       used for codec lookup and other shared properties.</li>
 * </ul>
 *
 * <p>Sharing across micro-row-groups: each visiting {@code readNextRowGroup()} call hands
 * the caller a fresh {@link org.apache.parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader}
 * built from a <em>slice</em> of this source's page list (only the pages that overlap the
 * visiting block's row range). The {@code DataPage} objects themselves are shared by
 * reference; their {@link org.apache.parquet.bytes.BytesInput} payloads are re-readable
 * because they wrap underlying {@code ByteBuffer}s rather than consuming a single stream.
 *
 * <p>The compressed dictionary page is shared by reference too. Each micro-row-group's
 * column reader re-decompresses and re-decodes it independently &mdash; that is the
 * known prototype-grade inefficiency called out in the Approach 2 plan; a production
 * implementation would memoize the decoded {@link org.apache.parquet.column.Dictionary}.
 *
 * <p>Instances are constructed at file open by {@link ParquetFileReader} when an
 * Approach 2 file is detected (any block returns {@code true} from
 * {@link org.apache.parquet.hadoop.metadata.BlockMetaData#isApproach2()}).
 */
final class PhysicalChunkPageSource {

  /**
   * Column chunk metadata from the first physical row group in the group. Used by the
   * caller to resolve codec, encoding stats, and (when present) the compressed
   * dictionary page's location during IO planning. Note that
   * {@link ColumnChunkMetaData#isPhysicallyShared()} returns {@code true} on this
   * instance; callers must not use {@link ColumnChunkMetaData#getStartingPos()} or
   * {@link ColumnChunkMetaData#getTotalSize()} for IO planning.
   */
  private final ColumnChunkMetaData metadata;

  /**
   * The full ordered list of compressed data pages for this physical column chunk.
   * Each {@link DataPage} carries a re-readable {@link org.apache.parquet.bytes.BytesInput}.
   */
  private final List<DataPage> compressedPages;

  /**
   * The compressed dictionary page, or {@code null} if this column chunk has none.
   */
  private final DictionaryPage compressedDictionaryPage;

  /**
   * OffsetIndex covering every page in {@link #compressedPages}. {@code firstRowIndex(i)}
   * values are file-absolute under Approach 2.
   */
  private final OffsetIndex absoluteOffsetIndex;

  PhysicalChunkPageSource(
      ColumnChunkMetaData metadata,
      List<DataPage> compressedPages,
      DictionaryPage compressedDictionaryPage,
      OffsetIndex absoluteOffsetIndex) {
    this.metadata = metadata;
    this.compressedPages = List.copyOf(compressedPages);
    this.compressedDictionaryPage = compressedDictionaryPage;
    this.absoluteOffsetIndex = absoluteOffsetIndex;
  }

  ColumnChunkMetaData getMetadata() {
    return metadata;
  }

  DictionaryPage getCompressedDictionaryPage() {
    return compressedDictionaryPage;
  }

  OffsetIndex getAbsoluteOffsetIndex() {
    return absoluteOffsetIndex;
  }

  int getPageCount() {
    return compressedPages.size();
  }

  /**
   * A slice of {@link PhysicalChunkPageSource} pages restricted to those that overlap an
   * absolute row range. The {@link OffsetIndex} returned by {@link #getOffsetIndex()} is
   * indexed 0..{@code pages.size()-1} and matches {@link #getPages()} entry-for-entry, so
   * a {@code ColumnChunkPageReader} built from this slice can index either by ordinal
   * without confusion.
   */
  static final class PageSlice {
    private final List<DataPage> pages;
    private final OffsetIndex offsetIndex;

    private PageSlice(List<DataPage> pages, OffsetIndex offsetIndex) {
      this.pages = pages;
      this.offsetIndex = offsetIndex;
    }

    List<DataPage> getPages() {
      return pages;
    }

    OffsetIndex getOffsetIndex() {
      return offsetIndex;
    }
  }

  /**
   * Returns the slice of pages whose absolute row span intersects the closed range
   * {@code [fromAbsoluteRow, toAbsoluteRow]}. The returned slice's {@link OffsetIndex}
   * is a 0-based view into the surviving subset, so callers can pass it to
   * {@code ColumnChunkPageReader} without further translation.
   *
   * <p>A page is included if any row index in its {@code [firstRowIndex, lastRowIndex]}
   * span falls inside {@code [fromAbsoluteRow, toAbsoluteRow]}. The OffsetIndex must be
   * non-null and its entries must be sorted by {@code firstRowIndex}.
   *
   * @param fromAbsoluteRow inclusive lower bound, file-absolute
   * @param toAbsoluteRow   inclusive upper bound, file-absolute
   * @return the filtered page slice, possibly empty but never {@code null}
   */
  PageSlice sliceForRowRange(long fromAbsoluteRow, long toAbsoluteRow) {
    if (toAbsoluteRow < fromAbsoluteRow) {
      return new PageSlice(List.of(), new SlicedOffsetIndex(absoluteOffsetIndex, new int[0]));
    }
    if (absoluteOffsetIndex == null) {
      throw new IllegalStateException(
          "Approach 2 column chunk has no OffsetIndex; cannot slice pages by row range");
    }
    final int n = absoluteOffsetIndex.getPageCount();
    final long upperRowSentinel = toAbsoluteRow + 1L;
    final List<DataPage> resultPages = new ArrayList<>();
    final List<Integer> keptIndexes = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      final long pageFirst = absoluteOffsetIndex.getFirstRowIndex(i);
      // Last row of page i is firstRow(i+1) - 1 when there is a next page; for the final
      // page the source has no easy upper bound, so we treat it as open-ended and trust
      // the SynchronizingColumnReader to stop at toAbsoluteRow.
      final long pageLast = (i + 1 < n) ? absoluteOffsetIndex.getFirstRowIndex(i + 1) - 1L : Long.MAX_VALUE;
      if (pageLast < fromAbsoluteRow) {
        continue;
      }
      if (pageFirst >= upperRowSentinel) {
        break;
      }
      resultPages.add(compressedPages.get(i));
      keptIndexes.add(i);
    }
    int[] indexMap = new int[keptIndexes.size()];
    for (int k = 0; k < indexMap.length; k++) {
      indexMap[k] = keptIndexes.get(k);
    }
    return new PageSlice(resultPages, new SlicedOffsetIndex(absoluteOffsetIndex, indexMap));
  }

  /**
   * A read-only view onto another {@link OffsetIndex} that exposes only the entries at
   * positions listed in {@code indexMap}. Used to align the sliced page list with a fresh
   * {@code ColumnChunkPageReader}, which addresses its OffsetIndex by sequential page
   * ordinal.
   */
  private static final class SlicedOffsetIndex implements OffsetIndex {
    private final OffsetIndex source;
    private final int[] indexMap;

    SlicedOffsetIndex(OffsetIndex source, int[] indexMap) {
      this.source = source;
      this.indexMap = indexMap;
    }

    @Override
    public int getPageCount() {
      return indexMap.length;
    }

    @Override
    public long getOffset(int pageIndex) {
      return source.getOffset(indexMap[pageIndex]);
    }

    @Override
    public int getCompressedPageSize(int pageIndex) {
      return source.getCompressedPageSize(indexMap[pageIndex]);
    }

    @Override
    public long getFirstRowIndex(int pageIndex) {
      return source.getFirstRowIndex(indexMap[pageIndex]);
    }

    @Override
    public long getLastRowIndex(int pageIndex, long totalRowCount) {
      int srcIdx = indexMap[pageIndex];
      int nextIdx = srcIdx + 1;
      return (nextIdx >= source.getPageCount() ? totalRowCount : source.getFirstRowIndex(nextIdx)) - 1L;
    }

    @Override
    public int getPageOrdinal(int pageIndex) {
      return source.getPageOrdinal(indexMap[pageIndex]);
    }

    @Override
    public java.util.Optional<Long> getUnencodedByteArrayDataBytes(int pageIndex) {
      return source.getUnencodedByteArrayDataBytes(indexMap[pageIndex]);
    }
  }
}
