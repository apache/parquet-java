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
package org.apache.parquet.column.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.IntPredicate;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.column.page.mem.MemPageWriter;
import org.apache.parquet.schema.MessageType;

final class ChunkingTestSupport {

  private ChunkingTestSupport() {}

  static CdcOptions options(long min, long max, int normLevel) {
    return CdcOptions.builder()
        .withMinChunkSize(min)
        .withMaxChunkSize(max)
        .withNormLevel(normLevel)
        .build();
  }

  /** A reproducible value stream; the same seed is the same data on any JVM. */
  static long[] newLongs(int count, long seed) {
    return new Random(seed).longs(count).toArray();
  }

  /** The generator the golden boundary vectors are built from. */
  static long[] lcg(int count) {
    long[] values = new long[count];
    long x = 0x243F6A8885A308D3L;
    for (int i = 0; i < count; ++i) {
      x = 6364136223846793005L * x + 1442695040888963407L;
      values[i] = x;
    }
    return values;
  }

  /** {@code original} with {@code inserted} spliced in at {@code at}, the edit under test. */
  static long[] insert(long[] original, int at, long[] inserted) {
    long[] result = new long[original.length + inserted.length];
    System.arraycopy(original, 0, result, 0, at);
    System.arraycopy(inserted, 0, result, at, inserted.length);
    System.arraycopy(original, at, result, at + inserted.length, original.length - at);
    return result;
  }

  /** Properties with the position-based page limits lifted, so every page boundary is the chunker's. */
  static ParquetProperties unboundedProps(CdcOptions options) {
    return ParquetProperties.builder()
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withPageSize(64 * 1024 * 1024)
        .withDictionaryEncoding(false)
        .withContentDefinedChunking(options)
        .build();
  }

  /**
   * Writes {@code rows} records through real {@link ColumnWriteStore}s, {@code writeRecord} writing
   * record {@code i} to the schema's only column, and returns the pages that come out. A new store,
   * made with the same {@code props}, takes over at each of {@code rowGroupStarts}, as for a new row
   * group.
   */
  static List<DataPage> writePages(
      MessageType schema,
      ParquetProperties props,
      int rows,
      ObjIntConsumer<ColumnWriter> writeRecord,
      int... rowGroupStarts) {
    ColumnDescriptor path = schema.getColumns().get(0);
    List<DataPage> pages = new ArrayList<>();
    int record = 0;
    for (int end : IntStream.concat(IntStream.of(rowGroupStarts), IntStream.of(rows))
        .toArray()) {
      MemPageStore pageStore = new MemPageStore(end - record);
      ColumnWriteStore store = props.newColumnWriteStore(schema, pageStore);
      ColumnWriter writer = store.getColumnWriter(path);
      for (; record < end; ++record) {
        writeRecord.accept(writer, record);
        store.endRecord();
      }
      store.flush();
      pages.addAll(((MemPageWriter) pageStore.getPageWriter(path)).getPages());
    }
    return pages;
  }

  /**
   * Value counts per page, as {@code ChunkingColumnWriter} counts them: a boundary closes the page
   * before the triplet that triggered it.
   */
  static List<Integer> chunkSizes(int count, IntPredicate offer) {
    List<Integer> sizes = new ArrayList<>();
    int current = 0;
    for (int i = 0; i < count; ++i) {
      if (offer.test(i) && current > 0) {
        sizes.add(current);
        current = 0;
      }
      current++;
    }
    if (current > 0) {
      sizes.add(current);
    }
    return sizes;
  }

  static List<Integer> valueCounts(List<DataPage> pages) {
    return pages.stream().map(DataPage::getValueCount).collect(Collectors.toList());
  }

  /** How many leading elements the two lists agree on. */
  static int sharedPrefix(List<?> before, List<?> after) {
    int i = 0;
    while (i < before.size() && i < after.size() && before.get(i).equals(after.get(i))) {
      i++;
    }
    return i;
  }

  /** How many trailing elements they agree on, beyond the {@code prefix} already counted. */
  static int sharedSuffix(List<?> before, List<?> after, int prefix) {
    int i = 0;
    while (i < before.size() - prefix
        && i < after.size() - prefix
        && before.get(before.size() - 1 - i).equals(after.get(after.size() - 1 - i))) {
      i++;
    }
    return i;
  }
}
