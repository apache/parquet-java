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

import static org.apache.parquet.column.impl.ChunkingTestUtil.chunkSizes;
import static org.apache.parquet.column.impl.ChunkingTestUtil.newLongs;
import static org.apache.parquet.column.impl.ChunkingTestUtil.options;
import static org.apache.parquet.column.impl.ChunkingTestUtil.unboundedProps;
import static org.apache.parquet.column.impl.ChunkingTestUtil.valueCountDifferences;
import static org.apache.parquet.column.impl.ChunkingTestUtil.valueCounts;
import static org.apache.parquet.column.impl.ChunkingTestUtil.writePages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ContentDefinedChunkingOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ChunkingTestUtil.Page;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

public class TestColumnWriterChunking {

  private static final ContentDefinedChunkingOptions OPTIONS = options(4 * 1024, 16 * 1024, 0);
  private static final int EDIT = 50;

  private static final MessageType REQUIRED = MessageTypeParser.parseMessageType("message m { required int64 v; }");
  private static final MessageType REQUIRED_BINARY =
      MessageTypeParser.parseMessageType("message m { required binary v; }");
  private static final MessageType LIST = MessageTypeParser.parseMessageType("message m { repeated int64 v; }");

  /** A value wider than the maximum chunk size asks for a boundary before itself on an empty page. */
  @Test
  public void boundaryOnFirstValueOfPageWritesNoEmptyPage() {
    List<DataPage> pages = writePages(REQUIRED_BINARY, unboundedProps(options(0, 32, 0)), 4, (writer, i) -> {
      byte[] wide = new byte[64];
      Arrays.fill(wide, (byte) i);
      writer.write(Binary.fromConstantByteArray(wide), 0, 0);
    });

    assertThat(valueCounts(pages)).containsExactly(1, 1, 1, 1);
  }

  @Test
  public void storeWithoutTheChunkersOfTheFileIsRejected() {
    ParquetProperties props = unboundedProps(OPTIONS);
    String message = "Content defined chunking needs the chunkers of the file";

    assertThatThrownBy(() -> props.newColumnWriteStore(REQUIRED, new MemPageStore(1), column -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> props.newColumnWriteStore(REQUIRED, new MemPageStore(1)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> new ColumnWriteStoreV1(new MemPageStore(1), props))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> new ColumnWriteStoreV2(REQUIRED, new MemPageStore(1), column -> null, props, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
  }

  @ParameterizedTest
  @EnumSource(value = PrimitiveTypeName.class, mode = EnumSource.Mode.EXCLUDE, names = "INT96")
  public void everyTypeGoesThroughTheChunker(PrimitiveTypeName type) {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required "
        + type.name().toLowerCase() + (type == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ? "(8)" : "") + " v; }");
    long[] values = newLongs(60_000, 13);
    ContentDefinedChunker chunker =
        new ContentDefinedChunker(schema.getColumns().get(0), unboundedProps(OPTIONS));
    List<Integer> expected = chunkSizes(values.length, i -> startsNewChunk(chunker, type, values[i]));

    assertThat(valueCounts(writePages(
            schema, unboundedProps(OPTIONS), values.length, (writer, i) -> write(writer, type, values[i]))))
        .hasSizeGreaterThan(1)
        .containsExactlyElementsOf(expected);
  }

  /** Each row group starts a record before or at a chunk boundary, where a restarted chunker misses it. */
  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void rowGroupsContinueTheChunking(WriterVersion version) {
    ParquetProperties props = ParquetProperties.copy(unboundedProps(OPTIONS))
        .withWriterVersion(version)
        .build();
    long[] values = newLongs(20_000, 29);
    ObjIntConsumer<ColumnWriter> record = (writer, i) -> {
      int length = (int) (values[i] & 3);
      if (length == 0) {
        writer.writeNull(0, 0);
      }
      for (int j = 0; j < length; ++j) {
        writer.write(values[i] + j, j == 0 ? 0 : 1, 1);
      }
    };
    int[] recordStarts = new int[values.length + 1];
    for (int i = 0; i < values.length; ++i) {
      recordStarts[i + 1] = recordStarts[i] + Math.max(1, (int) (values[i] & 3));
    }
    Set<Integer> pageEnds = pageEnds(writePages(LIST, props, values.length, record));
    Set<Integer> rowGroupStarts = new TreeSet<>();
    for (int end : pageEnds) {
      int chunkStart = Arrays.binarySearch(recordStarts, end);
      if (chunkStart < values.length) {
        rowGroupStarts.add(chunkStart - 1);
        rowGroupStarts.add(chunkStart);
      }
    }
    Set<Integer> expected = new TreeSet<>(pageEnds);
    rowGroupStarts.forEach(start -> expected.add(recordStarts[start]));

    assertThat(rowGroupStarts).hasSizeGreaterThan(20);
    assertThat(pageEnds(writePages(
            LIST,
            props,
            values.length,
            record,
            rowGroupStarts.stream().mapToInt(Integer::intValue).toArray())))
        .containsExactlyElementsOf(expected);
  }

  /** Each null counts the two bytes of its definition level. */
  @Test
  public void nullsChunkWithinTheSizeEnvelope() {
    MessageType optional = MessageTypeParser.parseMessageType("message m { optional int64 v; }");
    List<Integer> counts = valueCounts(
        writePages(optional, unboundedProps(options(64, 256, 0)), 4096, (writer, i) -> writer.writeNull(0, 0)));

    assertThat(counts.stream().mapToInt(Integer::intValue).sum()).isEqualTo(4096);
    assertThat(counts.subList(0, counts.size() - 1))
        .hasSizeGreaterThan(30)
        .allSatisfy(count -> assertThat(count).isBetween(32, 128));
  }

  @Test
  public void dictionaryEncodedPagesEndWherePlainOnesDo() {
    long[] values = newLongs(100_000, 43);
    ObjIntConsumer<ColumnWriter> record =
        (writer, i) -> writer.write(Binary.fromString("v" + Long.remainderUnsigned(values[i], 100)), 0, 0);
    List<DataPage> plain = writePages(REQUIRED_BINARY, unboundedProps(OPTIONS), values.length, record);
    ParquetProperties props = ParquetProperties.copy(unboundedProps(OPTIONS))
        .withDictionaryEncoding(true)
        .build();
    List<DataPage> dictionary = writePages(REQUIRED_BINARY, props, values.length, record);

    assertThat(dictionary)
        .hasSizeGreaterThan(5)
        .allSatisfy(page -> assertThat(
                ((DataPageV1) page).getValueEncoding().usesDictionary())
            .isTrue());
    assertThat(valueCounts(dictionary)).containsExactlyElementsOf(valueCounts(plain));
  }

  /**
   * A chunk that ends inside a record, here at the maximum chunk size, ends the page at the next record
   * start instead of being lost.
   */
  @Test
  public void chunkEndingInsideARecordEndsThePageAtTheNextRecordStart() {
    List<Integer> counts = valueCounts(writePages(LIST, unboundedProps(options(0, 64, 0)), 50, (writer, i) -> {
      for (int k = 0; k < 100; k++) {
        writer.write((long) i * 100 + k, k == 0 ? 0 : 1, 1);
      }
    }));

    assertThat(counts).hasSize(50).containsOnly(100);
  }

  /** The row count limit is counted from the page start, so it cuts inside chunks and after boundaries. */
  @Test
  public void listPagesEndAtRecordStarts() {
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(100)
        .withContentDefinedChunking(OPTIONS)
        .build();
    long[] values = newLongs(10_000, 31);
    List<Integer> counts = valueCounts(writePages(LIST, props, values.length, (writer, i) -> {
      writer.write(values[i], 0, 1);
      writer.write(values[i] + 1, 1, 1);
      writer.write(values[i] + 2, 1, 1);
    }));

    assertThat(counts)
        .allSatisfy(count -> assertThat(count % 3).isZero())
        .allSatisfy(count -> assertThat(count).isLessThanOrEqualTo(300))
        .contains(300);
    assertThat(counts.subList(0, counts.size() - 1))
        .filteredOn(count -> count < 300)
        .as("pages the chunker ends")
        .hasSizeGreaterThan(5);
  }

  /**
   * Without chunking the column writers end no page, also before a null: the store checks the page
   * size every 1000 records.
   */
  @Test
  public void withoutChunkingOnlyTheStoreEndsPages() {
    MessageType optional = MessageTypeParser.parseMessageType("message m { optional int64 v; }");
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageSize(1024)
        .withMinRowCountForPageSizeCheck(1000)
        .withMaxRowCountForPageSizeCheck(1000)
        .estimateRowCountForPageSizeCheck(false)
        .build();
    ObjIntConsumer<ColumnWriter> record = (writer, i) -> {
      if (i % 2 == 0) {
        writer.writeNull(0, 0);
      } else {
        writer.write((long) i, 0, 1);
      }
    };

    assertThat(valueCounts(writePages(optional, props, 10_000, record)))
        .hasSize(10)
        .containsOnly(1000);
  }

  /**
   * A page limit reached where a chunk ends still consumes the boundary, so that every later chunk
   * ends where it would without the limit; the row count limit counts from the start of each page,
   * also of a page a chunk started.
   */
  @Test
  public void pageLimitsMoveNoChunkBoundary() {
    long[] values = newLongs(30_000, 41);
    ObjIntConsumer<ColumnWriter> record = (writer, i) -> writer.write(values[i], 0, 0);
    ParquetProperties limited = ParquetProperties.copy(unboundedProps(OPTIONS))
        .withPageRowCountLimit(7)
        .withMinRowCountForPageSizeCheck(1)
        .build();
    Set<Integer> chunkEnds = pageEnds(writePages(REQUIRED, unboundedProps(OPTIONS), values.length, record));
    List<Integer> expected = new ArrayList<>();
    int start = 0;
    for (int end : chunkEnds) {
      for (int cut = start + 7; cut < end; cut += 7) {
        expected.add(cut);
      }
      expected.add(end);
      start = end;
    }

    assertThat(chunkEnds).hasSizeGreaterThan(20);
    assertThat(pageEnds(writePages(REQUIRED, limited, values.length, record)))
        .containsExactlyElementsOf(expected);
  }

  /**
   * Content defined chunking leaves the page limits as they are: with chunks too large to ever end, the
   * pages are those written without it.
   */
  @ParameterizedTest
  @CsvSource({"100, 1073741823, 67108864", "2147483647, 4500, 67108864", "2147483647, 1073741823, 4096"})
  public void pageLimitsApplyAsWithoutChunking(int rowCountLimit, int valueCountThreshold, int pageSize) {
    ParquetProperties.Builder limits = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(rowCountLimit)
        .withPageValueCountThreshold(valueCountThreshold)
        .withPageSize(pageSize)
        .withMinRowCountForPageSizeCheck(1000)
        .withMaxRowCountForPageSizeCheck(1000)
        .estimateRowCountForPageSizeCheck(false);
    long[] values = newLongs(30_000, 51);
    ObjIntConsumer<ColumnWriter> record = (writer, i) -> writer.write(values[i], 0, 0);
    List<Integer> without = valueCounts(writePages(REQUIRED, limits.build(), values.length, record));

    assertThat(without).hasSizeGreaterThan(2);
    assertThat(valueCounts(writePages(
            REQUIRED,
            limits.withContentDefinedChunking(options(1 << 30, 1L << 31, 0))
                .build(),
            values.length,
            record)))
        .containsExactlyElementsOf(without);
  }

  /**
   * An edit changes one run of pages at each place it edits, which holds the edited values more or
   * fewer, and leaves every other page byte for byte, as Arrow C++ checks it; in particular the first
   * and last pages, unless the edit is there.
   */
  @ParameterizedTest
  @EnumSource(Column.class)
  public void editChangesOnlyThePagesAroundIt(Column column) throws IOException {
    long[] seeds = newLongs(column.records, 37);
    List<Page> before = column.pages(seeds);

    for (Edit edit : Edit.values()) {
      List<Page> after = column.pages(edit.apply(seeds));

      assertThat(valueCountDifferences(before, after))
          .as("value count of each changed run of pages after %s", edit)
          .containsExactlyElementsOf(edit.differences(seeds, column::values));
      if (!edit.editsFirstPage()) {
        assertThat(after.get(0)).as("first page after %s", edit).isEqualTo(before.get(0));
      }
      if (!edit.editsLastPage()) {
        assertThat(after.get(after.size() - 1))
            .as("last page after %s", edit)
            .isEqualTo(before.get(before.size() - 1));
      }
    }
  }

  private static void write(ColumnWriter writer, PrimitiveTypeName type, long value) {
    switch (type) {
      case INT32 -> writer.write((int) value, 0, 0);
      case INT64 -> writer.write(value, 0, 0);
      case FLOAT -> writer.write(Float.intBitsToFloat((int) value), 0, 0);
      case DOUBLE -> writer.write(Double.longBitsToDouble(value), 0, 0);
      case BOOLEAN -> writer.write((value & 1) == 0, 0, 0);
      default -> writer.write(Binary.fromConstantByteArray(BytesUtils.longToBytes(value)), 0, 0);
    }
  }

  private static boolean startsNewChunk(ContentDefinedChunker chunker, PrimitiveTypeName type, long value) {
    return switch (type) {
      case INT32 -> chunker.startsNewChunk((int) value, 0, 0);
      case INT64 -> chunker.startsNewChunk(value, 0, 0);
      case FLOAT -> chunker.startsNewChunk(Float.intBitsToFloat((int) value), 0, 0);
      case DOUBLE -> chunker.startsNewChunk(Double.longBitsToDouble(value), 0, 0);
      case BOOLEAN -> chunker.startsNewChunk((value & 1) == 0, 0, 0);
      default -> chunker.startsNewChunk(Binary.fromConstantByteArray(BytesUtils.longToBytes(value)), 0, 0);
    };
  }

  /** Where each page ends, counted in levels from the first. */
  private static Set<Integer> pageEnds(List<DataPage> pages) {
    Set<Integer> ends = new TreeSet<>();
    int end = 0;
    for (DataPage page : pages) {
      end += page.getValueCount();
      ends.add(end);
    }
    return ends;
  }

  private static List<Page> pages(List<DataPage> pages) throws IOException {
    List<Page> compared = new ArrayList<>();
    for (DataPage page : pages) {
      compared.add(new Page(
          Binary.fromConstantByteArray(((DataPageV1) page).getBytes().toByteArray()), page.getValueCount()));
    }
    return compared;
  }

  /** A column of each shape, a record written from each seed. */
  private enum Column {
    INT32(200_000, "required int32 v;", (writer, seed) -> writer.write((int) seed, 0, 0)),
    OPTIONAL_DOUBLE(100_000, "optional double v;", (writer, seed) -> {
      if (seed % 5 == 0) {
        writer.writeNull(0, 0);
      } else {
        writer.write(seed / 3.0, 0, 1);
      }
    }),
    BOOLEAN(900_000, "required boolean v;", (writer, seed) -> writer.write((seed & 1) == 0, 0, 0)),
    OPTIONAL_BINARY(75_000, "optional binary v;", (writer, seed) -> {
      if (seed % 5 == 0) {
        writer.writeNull(0, 0);
      } else {
        writer.write(Binary.fromString(Long.toString(seed, 36)), 0, 1);
      }
    }),
    FIXED_LEN_BYTE_ARRAY(
        50_000,
        "required fixed_len_byte_array(16) v;",
        (writer, seed) -> writer.write(
            Binary.fromConstantByteArray(ByteBuffer.allocate(16)
                .putLong(seed)
                .putLong(~seed)
                .array()),
            0,
            0)),
    LIST(
        60_000,
        "optional group l (LIST) { repeated group list { optional int32 element; } }",
        (writer, seed) -> writeList(writer, seed, 3)),
    LIST_OF_GROUPS(
        60_000,
        "optional group l (LIST) { repeated group list { optional group element { optional int32 f0; } } }",
        (writer, seed) -> writeList(writer, seed, 4));

    private final int records;
    private final MessageType schema;
    private final ObjLongConsumer<ColumnWriter> record;

    Column(int records, String fields, ObjLongConsumer<ColumnWriter> record) {
      this.records = records;
      this.schema = MessageTypeParser.parseMessageType("message m { " + fields + " }");
      this.record = record;
    }

    List<Page> pages(long[] seeds) throws IOException {
      return TestColumnWriterChunking.pages(writePages(
          schema,
          unboundedProps(options(1024, 4096, 0)),
          seeds.length,
          (writer, i) -> record.accept(writer, seeds[i])));
    }

    /** How many values, nulls and levels of empty lists included, the records of these seeds have. */
    int values(long[] seeds) {
      return valueCounts(writePages(
              schema,
              ParquetProperties.builder().build(),
              seeds.length,
              (writer, i) -> record.accept(writer, seeds[i])))
          .stream()
          .mapToInt(Integer::intValue)
          .sum();
    }

    /** A null or empty list, or one of one to six elements, some of them null at every level. */
    private static void writeList(ColumnWriter writer, long seed, int maxDef) {
      int kind = (int) (seed >>> 61);
      if (kind < 2) {
        writer.writeNull(0, kind);
      }
      for (int j = 0; j < kind - 1; ++j) {
        int def = Math.max(2, maxDef - (int) ((seed >>> (4 * j)) & 3));
        if (def == maxDef) {
          writer.write((int) (seed >>> (8 * j)), j == 0 ? 0 : 1, def);
        } else {
          writer.writeNull(j == 0 ? 0 : 1, def);
        }
      }
    }
  }

  /** Edits at fractions of a column, each deleting {@code EDIT} records, inserting as many, or both. */
  private enum Edit {
    INSERT(true, false, 0.5),
    DELETE(false, true, 0.5),
    UPDATE(true, true, 0.5),
    PREPEND(true, false, 0),
    APPEND(true, false, 1),
    INSERT_TWICE(true, false, 1 / 3.0, 2 / 3.0),
    DELETE_TWICE(false, true, 1 / 3.0, 2 / 3.0),
    UPDATE_TWICE(true, true, 1 / 3.0, 2 / 3.0);

    private static final long[] INSERTED = newLongs(EDIT, 41);

    private final boolean inserts;
    private final boolean deletes;
    private final double[] at;

    Edit(boolean inserts, boolean deletes, double... at) {
      this.inserts = inserts;
      this.deletes = deletes;
      this.at = at;
    }

    long[] apply(long[] seeds) {
      long[] edited = seeds;
      // The later place first, so that the earlier one keeps its index.
      for (int k = at.length - 1; k >= 0; --k) {
        int place = (int) (at[k] * seeds.length);
        edited = Stream.of(
                Arrays.copyOfRange(edited, 0, place),
                inserts ? INSERTED : new long[0],
                Arrays.copyOfRange(edited, deletes ? place + EDIT : place, edited.length))
            .flatMapToLong(Arrays::stream)
            .toArray();
      }
      return edited;
    }

    /** How many values more the column holds at each place this edits, in column order. */
    List<Integer> differences(long[] seeds, ToIntFunction<long[]> values) {
      List<Integer> differences = new ArrayList<>();
      for (double fraction : at) {
        int place = (int) (fraction * seeds.length);
        differences.add((inserts ? values.applyAsInt(INSERTED) : 0)
            - (deletes ? values.applyAsInt(Arrays.copyOfRange(seeds, place, place + EDIT)) : 0));
      }
      return differences;
    }

    boolean editsFirstPage() {
      return at[0] == 0;
    }

    boolean editsLastPage() {
      return at[at.length - 1] == 1;
    }
  }
}
