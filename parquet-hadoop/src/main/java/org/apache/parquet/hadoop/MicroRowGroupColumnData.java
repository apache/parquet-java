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

import java.util.List;
import java.util.Set;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;

/**
 * Description of one physical column chunk for the Approach 2 (micro-row-group) writer
 * path. Carries the buffered, page-encoded bytes for the entire physical chunk plus
 * everything {@link ParquetFileWriter#writeMicroRowGroups} needs to (a) write the chunk
 * to disk in one shot and (b) slice it into K logical {@link
 * org.apache.parquet.hadoop.metadata.BlockMetaData} entries with sentinel
 * {@code data_page_offset == -1} and per-block absolute-row-index OffsetIndex sidecars.
 *
 * <p>Package-private and immutable. Produced by {@link
 * ColumnChunkPageWriteStore#drainForMicroRowGroups(int[])} once the in-memory column
 * store has finished accumulating pages for a logical record-batch flush.
 */
final class MicroRowGroupColumnData {
  final ColumnDescriptor descriptor;
  final CompressionCodecName codec;
  /** Dictionary page, or {@code null} if this column chunk has none. */
  final DictionaryPage dictionaryPage;
  /** Page headers + compressed bodies concatenated in page order. Written verbatim to disk. */
  final BytesInput concatenatedPageBytes;
  final long totalUncompressedSize;
  final long totalCompressedSize;
  final long totalValueCount;
  /**
   * Pre-populated builder (via the two-arg {@code add(compressedPageSize, rowCount)} variant)
   * carrying per-page sizes and row counts. {@link ParquetFileWriter#writeMicroRowGroups}
   * feeds it to {@code writeColumnChunk} for the shared chunk and then re-walks it via
   * {@link OffsetIndexBuilder#getPageCount()} / {@link OffsetIndexBuilder#getRowCount(int)}
   * / {@link OffsetIndexBuilder#getCompressedPageSize(int)} to compute per-micro-block
   * page slices.
   */
  final OffsetIndexBuilder offsetIndexBuilder;

  final Set<Encoding> rlEncodings;
  final Set<Encoding> dlEncodings;
  final List<Encoding> dataEncodings;

  MicroRowGroupColumnData(
      ColumnDescriptor descriptor,
      CompressionCodecName codec,
      DictionaryPage dictionaryPage,
      BytesInput concatenatedPageBytes,
      long totalUncompressedSize,
      long totalCompressedSize,
      long totalValueCount,
      OffsetIndexBuilder offsetIndexBuilder,
      Set<Encoding> rlEncodings,
      Set<Encoding> dlEncodings,
      List<Encoding> dataEncodings) {
    this.descriptor = descriptor;
    this.codec = codec;
    this.dictionaryPage = dictionaryPage;
    this.concatenatedPageBytes = concatenatedPageBytes;
    this.totalUncompressedSize = totalUncompressedSize;
    this.totalCompressedSize = totalCompressedSize;
    this.totalValueCount = totalValueCount;
    this.offsetIndexBuilder = offsetIndexBuilder;
    this.rlEncodings = rlEncodings;
    this.dlEncodings = dlEncodings;
    this.dataEncodings = dataEncodings;
  }
}
