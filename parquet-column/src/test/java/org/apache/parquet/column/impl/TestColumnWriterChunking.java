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
import static org.apache.parquet.column.impl.ChunkingTestUtil.insert;
import static org.apache.parquet.column.impl.ChunkingTestUtil.newLongs;
import static org.apache.parquet.column.impl.ChunkingTestUtil.options;
import static org.apache.parquet.column.impl.ChunkingTestUtil.unboundedProps;
import static org.apache.parquet.column.impl.ChunkingTestUtil.valueCounts;
import static org.apache.parquet.column.impl.ChunkingTestUtil.writePages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.util.stream.LongStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestColumnWriterChunking {

  private static final CdcOptions OPTIONS = options(4 * 1024, 16 * 1024, 0);
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
    List<Integer> expected = chunkSizes(values.length, i -> roll(chunker, type, values[i]));

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
    assertThat(counts.subList(0, counts.size() - 1)).hasSizeGreaterThan(30).allSatisfy(count -> assertThat(count)
        .isBetween(32, 128));
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

    assertThat(dictionary).hasSizeGreaterThan(5).allSatisfy(page -> assertThat(
            ((DataPageV1) page).getValueEncoding().usesDictionary())
        .isTrue());
    assertThat(valueCounts(dictionary)).containsExactlyElementsOf(valueCounts(plain));
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

  @Test
  public void valueCountLimitAppliesInsideAChunk() {
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withPageValueCountThreshold(5000)
        .withContentDefinedChunking(options(1 << 30, 1L << 31, 0))
        .build();

    assertThat(valueCounts(writePages(REQUIRED, props, 4 * 5000, (writer, i) -> writer.write((long) i, 0, 0))))
        .containsExactly(5000, 5000, 5000, 5000);
  }

  @Test
  public void pageSizeLimitAppliesInsideAChunk() {
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
    int threshold = props.getPageSizeThreshold();

    assertThat(pages)
        .allSatisfy(page -> assertThat(page.getUncompressedSize()).isLessThanOrEqualTo(threshold + 104))
        .filteredOn(page -> page.getUncompressedSize() >= threshold)
        .hasSizeGreaterThan(5);
  }

  /** Narrow values, whose default sized chunks run to several pages of the default size. */
  @Test
  public void pageSizeLimitCutsInTheSamePlacesAfterAnEdit() throws IOException {
    ParquetProperties props = ParquetProperties.builder()
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withContentDefinedChunking(CdcOptions.DEFAULT)
        .build();
    long[] original = newLongs(1_500_000, 21);
    List<Binary> before = pageBytes(narrowPages(props, original));
    List<Binary> after = pageBytes(narrowPages(props, insert(original, 10_000, newLongs(100, 22))));

    assertThat(before).hasSizeGreaterThan(5);
    assertThat(after).filteredOn(new HashSet<>(before)::contains).hasSizeGreaterThan(after.size() / 2);
  }

  /** All but a few pages of an edited column are pages of the original, also between two edits. */
  @ParameterizedTest
  @EnumSource(Column.class)
  public void editChangesOnlyThePagesAroundIt(Column column) throws IOException {
    long[] seeds = newLongs(column.records, 37);
    List<Binary> before = column.pageBytes(seeds);
    Set<Binary> original = new HashSet<>(before);

    assertThat(before).hasSizeGreaterThan(350);
    for (Edit edit : Edit.values()) {
      assertThat(column.pageBytes(edit.apply(seeds)))
          .filteredOn(page -> !original.contains(page))
          .as("new pages after %s", edit)
          .hasSizeLessThanOrEqualTo(20 * edit.at.length);
    }
  }

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

  private static boolean roll(ContentDefinedChunker chunker, PrimitiveTypeName type, long value) {
    switch (type) {
      case INT32:
        chunker.roll((int) value, 0, 0);
        break;
      case INT64:
        chunker.roll(value, 0, 0);
        break;
      case FLOAT:
        chunker.roll(Float.intBitsToFloat((int) value), 0, 0);
        break;
      case DOUBLE:
        chunker.roll(Double.longBitsToDouble(value), 0, 0);
        break;
      case BOOLEAN:
        chunker.roll((value & 1) == 0, 0, 0);
        break;
      default:
        chunker.roll(Binary.fromConstantByteArray(BytesUtils.longToBytes(value)), 0, 0);
    }
    return chunker.needNewChunk();
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

    List<Binary> pageBytes(long[] seeds) throws IOException {
      return TestColumnWriterChunking.pageBytes(writePages(
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

  private interface EditAt {
    long[] apply(long[] seeds, int at);
  }

  /** Edits of {@code EDIT} records at fractions of a column. */
  private enum Edit {
    INSERT(Edit::insertAt, 0.5),
    DELETE(Edit::deleteAt, 0.5),
    UPDATE(Edit::updateAt, 0.5),
    PREPEND(Edit::insertAt, 0),
    APPEND(Edit::insertAt, 1),
    INSERT_TWICE(Edit::insertAt, 1 / 3.0, 2 / 3.0),
    DELETE_TWICE(Edit::deleteAt, 1 / 3.0, 2 / 3.0),
    UPDATE_TWICE(Edit::updateAt, 1 / 3.0, 2 / 3.0);

    private final EditAt edit;
    private final double[] at;

    Edit(EditAt edit, double... at) {
      this.edit = edit;
      this.at = at;
    }

    long[] apply(long[] seeds) {
      long[] edited = seeds;
      // The later place first, so that the earlier one keeps its index.
      for (int k = at.length - 1; k >= 0; --k) {
        edited = edit.apply(edited, (int) (at[k] * seeds.length));
      }
      return edited;
    }

    private static long[] insertAt(long[] seeds, int at) {
      return insert(seeds, at, newLongs(EDIT, 41));
    }

    private static long[] deleteAt(long[] seeds, int at) {
      return LongStream.concat(Arrays.stream(seeds, 0, at), Arrays.stream(seeds, at + EDIT, seeds.length))
          .toArray();
    }

    private static long[] updateAt(long[] seeds, int at) {
      long[] updated = seeds.clone();
      System.arraycopy(newLongs(EDIT, 41), 0, updated, at, EDIT);
      return updated;
    }
  }
}
