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

import static org.apache.parquet.column.impl.ChunkingTestSupport.chunkSizes;
import static org.apache.parquet.column.impl.ChunkingTestSupport.insert;
import static org.apache.parquet.column.impl.ChunkingTestSupport.newLongs;
import static org.apache.parquet.column.impl.ChunkingTestSupport.options;
import static org.apache.parquet.column.impl.ChunkingTestSupport.sharedPrefix;
import static org.apache.parquet.column.impl.ChunkingTestSupport.sharedSuffix;
import static org.apache.parquet.column.impl.ChunkingTestSupport.unboundedProps;
import static org.apache.parquet.column.impl.ChunkingTestSupport.valueCounts;
import static org.apache.parquet.column.impl.ChunkingTestSupport.writePages;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Covers {@link ChunkingColumnWriter} through a real {@link ColumnWriteStore} and the pages it writes. */
public class TestCdcWrite {

  private static final CdcOptions OPTIONS = options(4 * 1024, 16 * 1024, 0);

  private static final MessageType REQUIRED = MessageTypeParser.parseMessageType("message m { required int64 v; }");
  private static final MessageType REQUIRED_BINARY =
      MessageTypeParser.parseMessageType("message m { required binary v; }");
  private static final MessageType LIST = MessageTypeParser.parseMessageType("message m { repeated int64 v; }");

  /**
   * A value wider than the maximum chunk size asks for a boundary before itself even as the first
   * value of a page, which {@link ColumnWriterBase#writePage()} would reject as empty. Arrow C++
   * filters out the same empty chunk.
   */
  @Test
  public void aBoundaryOnTheFirstValueOfAPageDoesNotWriteAnEmptyOne() {
    List<DataPage> pages = writePages(REQUIRED_BINARY, unboundedProps(options(0, 32, 0)), 4, (writer, i) -> {
      byte[] wide = new byte[64];
      Arrays.fill(wide, (byte) i);
      writer.write(Binary.fromConstantByteArray(wide), 0, 0);
    });

    assertThat(valueCounts(pages)).containsExactly(1, 1, 1, 1);
  }

  /** {@link ChunkingColumnWriter} hooks each {@code write} overload separately. */
  @ParameterizedTest
  @MethodSource("primitiveColumns")
  public void everyPrimitiveTypeGoesThroughTheChunker(String label, MessageType schema) {
    // Pages must fall where a chunker offered the same typed values puts its boundaries: a hook that
    // skips the chunker, cuts after the value, or offers another type moves them.
    ColumnDescriptor path = schema.getColumns().get(0);
    PrimitiveTypeName type = path.getPrimitiveType().getPrimitiveTypeName();
    long[] values = newLongs(60_000, 13);
    CdcChunker chunker = new CdcChunker(OPTIONS, path);
    List<Integer> expected = chunkSizes(values.length, i -> offer(chunker, type, values[i]));

    assertThat(expected).hasSizeGreaterThan(1);
    assertThat(valueCounts(writePages(
            schema, unboundedProps(OPTIONS), values.length, (writer, i) -> write(writer, type, values[i]))))
        .containsExactlyElementsOf(expected);
  }

  static Stream<Arguments> primitiveColumns() {
    return Stream.of(
        Arguments.of("int32", MessageTypeParser.parseMessageType("message m { required int32 v; }")),
        Arguments.of("int64", REQUIRED),
        Arguments.of("float", MessageTypeParser.parseMessageType("message m { required float v; }")),
        Arguments.of("double", MessageTypeParser.parseMessageType("message m { required double v; }")),
        Arguments.of("boolean", MessageTypeParser.parseMessageType("message m { required boolean v; }")),
        Arguments.of("binary", REQUIRED_BINARY),
        Arguments.of(
            "fixed_len_byte_array",
            MessageTypeParser.parseMessageType("message m { required fixed_len_byte_array(8) v; }")));
  }

  /** Writes {@code value} as the physical type {@code type}. */
  private static void write(ColumnWriter writer, PrimitiveTypeName type, long value) {
    switch (type) {
      case INT32:
        writer.write((int) value, 0, 0);
        break;
      case INT64:
        writer.write(value, 0, 0);
        break;
      case FLOAT:
        writer.write(Float.intBitsToFloat((int) value), 0, 0);
        break;
      case DOUBLE:
        writer.write(Double.longBitsToDouble(value), 0, 0);
        break;
      case BOOLEAN:
        writer.write((value & 1) == 0, 0, 0);
        break;
      default:
        writer.write(Binary.fromConstantByteArray(BytesUtils.longToBytes(value)), 0, 0);
    }
  }

  private static boolean offer(CdcChunker chunker, PrimitiveTypeName type, long value) {
    switch (type) {
      case INT32:
        return chunker.offer((int) value, 0, 0);
      case INT64:
        return chunker.offer(value, 0, 0);
      case FLOAT:
        return chunker.offer(Float.intBitsToFloat((int) value), 0, 0);
      case DOUBLE:
        return chunker.offer(Double.longBitsToDouble(value), 0, 0);
      case BOOLEAN:
        return chunker.offer((value & 1) == 0, 0, 0);
      default:
        return chunker.offer(Binary.fromConstantByteArray(BytesUtils.longToBytes(value)), 0, 0);
    }
  }

  /** The page limits count from the page start in the writer, so each column must keep one. */
  @Test
  public void aStoreHandsOutOneChunkingWriterPerColumn() {
    ColumnWriteStore store = unboundedProps(OPTIONS).newColumnWriteStore(REQUIRED, new MemPageStore(1));
    ColumnDescriptor path = REQUIRED.getColumns().get(0);
    assertThat(store.getColumnWriter(path)).isSameAs(store.getColumnWriter(path));
  }

  /**
   * The page size limit still cuts inside a chunk, but counted from the page start as in Arrow C++,
   * so it cuts in the same places after an edit. Narrow values, whose default sized chunks run to
   * several pages of the default size, keep deduplicating; checked on the store's row-count schedule
   * instead, they shared no page at all.
   */
  @Test
  public void thePageSizeLimitCutsInTheSamePlacesAfterAnEdit() throws IOException {
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withContentDefinedChunking(CdcOptions.DEFAULT)
        .build();
    long[] original = newLongs(1_500_000, 21);
    List<Binary> before = pageBytes(narrowPages(props, original));
    // Properties of its own, as for another file: they hold the chunking state.
    List<Binary> after = pageBytes(
        narrowPages(ParquetProperties.copy(props).build(), insert(original, 10_000, newLongs(100, 22))));

    Set<Binary> beforeSet = new HashSet<>(before);
    assertThat(before).hasSizeGreaterThan(5);
    assertThat(after.stream().filter(beforeSet::contains).count())
        .as("pages of the edited column that are also in the original")
        .isGreaterThan(after.size() / 2);
  }

  /** The row count limit still applies inside a chunk, at exactly the limit, as in Arrow C++. */
  @Test
  public void theRowCountLimitAppliesInsideAChunk() {
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withContentDefinedChunking(CdcOptions.DEFAULT)
        .build();
    assertThat(valueCounts(narrowPages(props, newLongs(200_000, 23))))
        .hasSizeGreaterThan(5)
        .allSatisfy(
            count -> assertThat(count).isLessThanOrEqualTo(ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT))
        .contains(ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT);
  }

  /**
   * A row group's store continues the chunking where the previous one, made with the same properties,
   * left it: row groups add page breaks but move no boundary. Here they start a record before and at
   * every chunk boundary, where a chunker that restarts its hash, match run, pending match or size
   * misses the boundary or ends the next chunk elsewhere.
   */
  @Test
  public void eachRowGroupContinuesTheChunkingOfThePrevious() {
    long[] values = newLongs(20_000, 29);
    // Lists of zero to three values, so that matches inside a record carry over to the next.
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
    Set<Integer> pageEnds = pageEnds(writePages(LIST, unboundedProps(OPTIONS), values.length, record));
    Set<Integer> rowGroupStarts = new TreeSet<>();
    for (int end : pageEnds) {
      int chunkStart = Arrays.binarySearch(recordStarts, end);
      if (chunkStart < values.length) {
        rowGroupStarts.add(chunkStart - 1);
        rowGroupStarts.add(chunkStart);
      }
    }
    assertThat(rowGroupStarts).hasSizeGreaterThan(20);

    Set<Integer> expected = new TreeSet<>(pageEnds);
    rowGroupStarts.forEach(start -> expected.add(recordStarts[start]));
    assertThat(pageEnds(writePages(
            LIST,
            unboundedProps(OPTIONS),
            values.length,
            record,
            rowGroupStarts.stream().mapToInt(Integer::intValue).toArray())))
        .containsExactlyElementsOf(expected);
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

  /** On a list column the row count limit ends pages at record starts only. */
  @Test
  public void theRowCountLimitEndsPagesAtRecordStarts() {
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(100)
        .withContentDefinedChunking(options(1 << 30, 1L << 31, 0))
        .build();
    assertThat(valueCounts(writePages(LIST, props, 1000, (writer, i) -> {
          writer.write((long) i, 0, 1);
          writer.write((long) i, 1, 1);
          writer.write((long) i, 1, 1);
        })))
        .containsExactly(300, 300, 300, 300, 300, 300, 300, 300, 300, 300);
  }

  /**
   * The size limits are checked at the first record start after each batch of 1024 levels, as in
   * Arrow C++, so a page value count limit of 5000 ends pages at 5120 values.
   */
  @Test
  public void theSizeLimitsAreCheckedOncePerBatch() {
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withPageValueCountThreshold(5000)
        .withContentDefinedChunking(options(1 << 30, 1L << 31, 0))
        .build();
    assertThat(valueCounts(writePages(REQUIRED, props, 4 * 5120, (writer, i) -> writer.write((long) i, 0, 0))))
        .containsExactly(5120, 5120, 5120, 5120);
  }

  /**
   * The size limit applies inside a chunk too, checked once per batch of 1024 values as in Arrow
   * C++: 100-byte values in chunks of up to 8 MiB still come out in pages of about the 1 MiB limit,
   * except those that end where a chunk does.
   */
  @Test
  public void thePageSizeLimitAppliesInsideAChunk() {
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withContentDefinedChunking(options(4 << 20, 8 << 20, 0))
        .build();
    byte[] value = new byte[100];
    List<DataPage> pages = writePages(
        REQUIRED_BINARY,
        props,
        100_000,
        (writer, i) -> writer.write(Binary.fromConstantByteArray(value), 0, 0));

    // No page runs more than one batch past the limit, and inside chunks the limit, not the chunker,
    // ends most of them.
    int threshold = props.getPageSizeThreshold();
    assertThat(pages).allSatisfy(page -> assertThat(page.getUncompressedSize())
        .isLessThanOrEqualTo(threshold + 1024 * (value.length + 4)));
    assertThat(pages)
        .filteredOn(page -> page.getUncompressedSize() >= threshold)
        .hasSizeGreaterThan(5);
  }

  /** One- and two-byte values: the narrow case, whose chunks run to the most pages. */
  private static List<DataPage> narrowPages(ParquetProperties props, long[] values) {
    return writePages(REQUIRED_BINARY, props, values.length, (writer, i) -> {
      byte[] bytes = BytesUtils.longToBytes(values[i]);
      writer.write(Binary.fromConstantByteArray(bytes, 0, (values[i] & 1) == 0 ? 1 : 2), 0, 0);
    });
  }

  private static List<Binary> pageBytes(List<DataPage> pages) throws IOException {
    List<Binary> bytes = new ArrayList<>();
    for (DataPage page : pages) {
      bytes.add(
          Binary.fromConstantByteArray(((DataPageV1) page).getBytes().toByteArray()));
    }
    return bytes;
  }

  /** Records edited. */
  private static final int EDIT = 50;

  /** A column of each shape, its records written from a seed each. */
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

    List<Binary> pageBytes(long[] seeds) throws IOException {
      return TestCdcWrite.pageBytes(writePages(
          schema,
          unboundedProps(options(1024, 4096, 0)),
          seeds.length,
          (writer, i) -> record.accept(writer, seeds[i])));
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

  private enum Edit {
    INSERT(seeds -> insert(seeds, seeds.length / 2, newLongs(EDIT, 41))),
    DELETE(seeds -> LongStream.concat(
            Arrays.stream(seeds, 0, seeds.length / 2),
            Arrays.stream(seeds, seeds.length / 2 + EDIT, seeds.length))
        .toArray()),
    UPDATE(seeds -> {
      long[] updated = seeds.clone();
      System.arraycopy(newLongs(EDIT, 41), 0, updated, seeds.length / 2, EDIT);
      return updated;
    }),
    PREPEND(seeds -> insert(seeds, 0, newLongs(EDIT, 41))),
    APPEND(seeds -> insert(seeds, seeds.length, newLongs(EDIT, 41)));

    private final UnaryOperator<long[]> apply;

    Edit(UnaryOperator<long[]> apply) {
      this.apply = apply;
    }
  }

  /**
   * An edit of a few records changes only the pages around it, in every shape of column: every page
   * before it and all but a few after it are byte for byte as before, so they deduplicate. That a few
   * change is the algorithm's, as in Arrow C++: the chunking realigns only once a chunk ends in the
   * same place again, as the eight matches that end one count from where it began.
   */
  @ParameterizedTest
  @EnumSource(Column.class)
  public void anEditChangesOnlyThePagesAroundIt(Column column) throws IOException {
    long[] original = newLongs(column.records, 37);
    List<Binary> before = column.pageBytes(original);
    assertThat(before).hasSizeGreaterThan(350);
    for (Edit edit : Edit.values()) {
      List<Binary> after = column.pageBytes(edit.apply.apply(original));
      int prefix = sharedPrefix(before, after);
      int suffix = sharedSuffix(before, after, prefix);
      assertThat(Math.max(before.size(), after.size()) - prefix - suffix)
          .as("pages changed of %s by %s", before.size(), edit)
          .isLessThanOrEqualTo(20);
    }
  }

  @Test
  public void onlyChunkingRealignsAfterAnInsertion() throws IOException {
    // Both writers share the pages before the insertion; only chunking shares any after it. Page
    // bytes rather than value counts, because a position-based writer's later pages keep their
    // counts but shift their contents.
    long[] original = newLongs(60_000, 11);
    long[] edited = insert(original, 25_000, newLongs(300, 42));

    assertThat(sharedBeyondThePrefix(boundedPageBytes(original, false), boundedPageBytes(edited, false)))
        .as("a position-based writer realigns nothing after an insertion")
        .isZero();

    List<Binary> chunkedBefore = boundedPageBytes(original, true);
    assertThat(chunkedBefore).hasSizeGreaterThan(10);
    assertThat(sharedBeyondThePrefix(chunkedBefore, boundedPageBytes(edited, true)))
        .as("chunking shares pages from after the insertion too")
        .isPositive();
  }

  /** How many of {@code before}'s pages past the common leading run reappear anywhere in {@code after}. */
  private static long sharedBeyondThePrefix(List<Binary> before, List<Binary> after) {
    int prefix = sharedPrefix(before, after);
    return before.subList(prefix, before.size()).stream()
        .filter(after::contains)
        .count();
  }

  /** Every page's bytes, with a page size small enough that the position-based limits make many pages. */
  private static List<Binary> boundedPageBytes(long[] values, boolean chunking) throws IOException {
    ParquetProperties props = ParquetProperties.builder()
        .withPageSize(16 * 1024)
        .withMinRowCountForPageSizeCheck(1)
        .withDictionaryEncoding(false)
        .withContentDefinedChunking(OPTIONS)
        .withContentDefinedChunkingEnabled(chunking)
        .build();

    return pageBytes(writePages(REQUIRED, props, values.length, (writer, i) -> writer.write(values[i], 0, 0)));
  }
}
