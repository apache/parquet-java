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
import java.util.Arrays;
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

public final class ChunkingTestUtil {

  /**
   * The page value counts pyarrow writes for an {@code optional list<int64>} column of 200 000
   * {@link #lcg} rows, chunked with a 64 to 256 KiB envelope: a null list where a value is divisible
   * by 11, otherwise as many elements as its remainder by 4, each a shift of it by 16 bits more.
   */
  public static final List<Integer> OPTIONAL_LIST_OF_INT64_PAGES = Arrays.asList(
      13336, 9928, 13972, 11483, 10503, 11533, 15500, 9599, 14956, 11109, 10846, 14956, 13858, 15861, 13510,
      16720, 11116, 10562, 9536, 11052, 12880, 12392, 10121, 14988, 13928, 10761, 11464);

  private ChunkingTestUtil() {}

  public static CdcOptions options(long min, long max, int normLevel) {
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
  public static long[] lcg(int count) {
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
   * record {@code i} to the schema's only column, and returns the pages that come out. A new store
   * takes over at each of {@code rowGroupStarts}, continuing the chunking as a new row group of the
   * same file does.
   */
  static List<DataPage> writePages(
      MessageType schema,
      ParquetProperties props,
      int rows,
      ObjIntConsumer<ColumnWriter> writeRecord,
      int... rowGroupStarts) {
    ColumnDescriptor path = schema.getColumns().get(0);
    List<ContentDefinedChunker> chunkers = props.isContentDefinedChunkingEnabled()
        ? ContentDefinedChunker.forColumns(schema, props.getContentDefinedChunkingOptions())
        : null;
    List<DataPage> pages = new ArrayList<>();
    int record = 0;
    for (int end : IntStream.concat(IntStream.of(rowGroupStarts), IntStream.of(rows))
        .toArray()) {
      MemPageStore pageStore = new MemPageStore(end - record);
      // No bloom filters are enabled, so the store never asks for a writer.
      ColumnWriteStore store = props.newColumnWriteStore(schema, pageStore, column -> null, chunkers);
      ColumnWriter writer = store.getColumnWriter(path);
      for (; record < end; ++record) {
        writeRecord.accept(writer, record);
        store.endRecord();
      }
      store.flush();
      pages.addAll(((MemPageWriter) pageStore.getPageWriter(path)).getPages());
      store.close();
    }
    return pages;
  }

  /**
   * Value counts per page, as {@code ChunkingColumnWriter} counts them: a boundary closes the page
   * before the triplet that triggered it.
   */
  static List<Integer> chunkSizes(int count, IntPredicate roll) {
    List<Integer> sizes = new ArrayList<>();
    int current = 0;
    for (int i = 0; i < count; ++i) {
      if (roll.test(i) && current > 0) {
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
}
