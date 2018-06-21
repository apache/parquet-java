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

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

/**
 * Internal utility class to help at column index based filtering.
 */
class ColumnIndexFilterUtils {
  static class ConsecutivePart {
    private final long offset;
    private long length;

    private ConsecutivePart(long offset, int length) {
      this.offset = offset;
      this.length = length;
    }

    long getOffset() {
      return offset;
    }

    long getLength() {
      return length;
    }

    private boolean increment(long offset, int length) {
      if (this.offset + this.length == offset) {
        this.length += length;
        return true;
      } else {
        return false;
      }
    }
  }

  static OffsetIndex filterOffsetIndex(OffsetIndex offsetIndex, RowRanges rowRanges, long allRowCount) {
    OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
    for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
      long from = offsetIndex.getFirstRowIndex(i);
      long to = i + 1 < n ? offsetIndex.getFirstRowIndex(i + 1) - 1 : allRowCount - 1;
      if (rowRanges.isConnected(from, to)) {
        builder.add(offsetIndex.getOffset(i), offsetIndex.getCompressedPageSize(i), from);
      }
    }
    return builder.build();
  }

  static List<ConsecutivePart> calculateConsecutiveParts(OffsetIndex offsetIndex, ColumnChunkMetaData cm,
      long firstPageOffset) {
    List<ConsecutivePart> parts = new ArrayList<>();
    int n = offsetIndex.getPageCount();
    if (n > 0) {
      ConsecutivePart lastPart = null;

      // Add part for the dictionary page if required
      long rowGroupOffset = cm.getStartingPos();
      if (rowGroupOffset < firstPageOffset) {
        lastPart = new ConsecutivePart(rowGroupOffset, (int) (firstPageOffset - rowGroupOffset));
        parts.add(lastPart);
      }

      for (int i = 0; i < n; ++i) {
        long offset = offsetIndex.getOffset(i);
        int length = offsetIndex.getCompressedPageSize(i);
        if (lastPart == null || !lastPart.increment(offset, length)) {
          lastPart = new ConsecutivePart(offset, length);
          parts.add(lastPart);
        }
      }
    }
    return parts;
  }
}
