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

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.column.impl.ContentDefinedChunker.calculateMask;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.parquet.FixedBinaryTestUtils;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ContentDefinedChunkingOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.assertj.core.util.diff.DiffUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Arrow C++'s {@code parquet/chunker_internal_test.cc}: the chunk size parameters and the rolling hash
 * mask, and, through the column write stores, pages weigh between the minimum and maximum chunk size,
 * and an edit changes only the pages around it.
 */
public class TestContentDefinedChunker {

  private static final String MASK_OUT_OF_RANGE = "The content defined chunking mask must be between 1 and 63 bits";

  @ParameterizedTest
  @CsvSource({
    "262144, 1048576, 0",
    "0, 32, 0",
    "32, 64, 0",
    "1048576, 2097152, 0",
    "1073741824, 2147483648, 0",
    "1, 65, 1",
    "0, 128, 2"
  })
  public void chunkSizeParametersAreAccepted(long min, long max, int normLevel) {
    assertThatCode(() -> new ContentDefinedChunkingOptions(min, max, normLevel))
        .doesNotThrowAnyException();
  }

  /** With norm level 0, 1 or 2 the maximum must exceed the minimum by at least 32, 64 or 128 bytes. */
  @ParameterizedTest
  @CsvSource({
    "0, -1, 0",
    "1024, 512, 0",
    "-1, 0, 0",
    "0, 0, 0",
    "0, 16, 0",
    "-16, -16, 0",
    "16, 0, 0",
    "32, 32, 0",
    "32, 48, 0",
    "1, -1, 1",
    "-1, 1, 1",
    "1, 1, 1",
    "1, 32, 1",
    "1, 33, 1",
    "0, 123, 2"
  })
  public void chunkSizeParametersAreRejected(long min, long max, int normLevel) {
    assertThatThrownBy(() -> new ContentDefinedChunkingOptions(min, max, normLevel))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("content defined chunking");
  }

  @Test
  public void rollingHashMaskCalculation() {
    long min = 256 * 1024;
    long max = 1024 * 1024;
    assertThat(calculateMask(min, max, 0)).isEqualTo(0xFFFE000000000000L);
    assertThat(calculateMask(min, max, 1)).isEqualTo(0xFFFC000000000000L);
    assertThat(calculateMask(min, max, 2)).isEqualTo(0xFFF8000000000000L);
    assertThat(calculateMask(min, max, 3)).isEqualTo(0xFFF0000000000000L);
    assertThat(calculateMask(min, max, -1)).isEqualTo(0xFFFF000000000000L);

    // The mask bits must be between 1 and 63 once adjusted for the 8 gear hash tables
    assertThatThrownBy(() -> calculateMask(0, 16, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(MASK_OUT_OF_RANGE);
    assertThat(calculateMask(0, 32, 0)).isEqualTo(0x8000000000000000L);
    assertThatThrownBy(() -> calculateMask(0, 32, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(MASK_OUT_OF_RANGE);
    assertThat(calculateMask(0, 64, 0)).isEqualTo(0xC000000000000000L);
    assertThat(calculateMask(0, 16, -1)).isEqualTo(0x8000000000000000L);

    // Unrealistic, but checks the validation
    assertThatThrownBy(() -> calculateMask(128, 384, -60))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(MASK_OUT_OF_RANGE);
    assertThat(calculateMask(128, 384, -59)).isEqualTo(0xFFFFFFFFFFFFFFFEL);

    // Unlike Arrow C++, the average chunk size does not overflow
    assertThat(calculateMask(256 * 1024, Long.MAX_VALUE, 0)).isEqualTo(0xFFFFFFFFFFFFFFC0L);
  }

  /**
   * Arrow C++'s WriteSingleColumnParquetFile: a column write store made without the chunkers of the
   * file, which every row group of the file must continue, is rejected.
   */
  @Test
  public void storeWithoutTheChunkersOfTheFileIsRejected() {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required int32 number; }");
    ParquetProperties props =
        ParquetProperties.builder().withContentDefinedChunking().build();
    String message = "Content defined chunking needs the chunkers of the file";

    assertThatThrownBy(() -> props.newColumnWriteStore(schema, new MemPageStore(1), column -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> props.newColumnWriteStore(schema, new MemPageStore(1)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> new ColumnWriteStoreV1(new MemPageStore(1), props))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> new ColumnWriteStoreV2(schema, new MemPageStore(1), column -> null, props, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> props.newColumnWriteStore(schema, new MemPageStore(1), column -> null, List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Expected a chunker per column");
  }

  /** Arrow C++'s TestCDCSingleRowGroup, for each of its column types that Parquet stores apart. */
  @Nested
  @ParameterizedClass
  @MethodSource("cases")
  class SingleRowGroup {

    private static final int PART_SIZE = 128 * 1024;
    private static final int EDIT_SIZE = 128;
    private static final ContentDefinedChunkingOptions OPTIONS =
        new ContentDefinedChunkingOptions(4 * 1024, 16 * 1024, 0);
    private static final int ROW_GROUP_LENGTH = 1024 * 1024;

    private final Case param;
    private final Table part1, part2, part3, part4, part5, part6, part7;

    SingleRowGroup(Case param) {
      this.param = param;
      // The chunk sizes are constant, so the record count follows from the size of a record, a hand
      // picked one for variable length and nested types
      int bytesPerRecord = param.field() instanceof Primitive field && field.type().byteWidth > 0
          ? field.type().byteWidth + (field.nullable() ? 2 : 0)
          : 16;
      int partLength = PART_SIZE / bytesPerRecord;
      int editLength = EDIT_SIZE / bytesPerRecord;
      List<Field> fields = List.of(param.field());
      part1 = generateTable(fields, partLength, 0);
      part2 = generateTable(fields, editLength, 1);
      part3 = generateTable(fields, partLength, partLength);
      part4 = generateTable(fields, editLength, 2);
      part5 = generateTable(fields, partLength, 2L * partLength);
      part6 = generateTable(fields, editLength, 3);
      part7 = generateTable(fields, editLength, 4);
    }

    static Stream<Case> cases() {
      return Stream.of(
          // Boolean
          new Case(new Primitive("f0", Type.BOOLEAN, false), PARQUET_1_0),
          // Numeric
          new Case(new Primitive("f0", Type.UINT8, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.UINT16, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.UINT32, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.UINT64, true), PARQUET_1_0),
          new Case(new Primitive("f0", Type.INT8, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.INT16, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.INT32, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.INT64, true), PARQUET_1_0),
          new Case(new Primitive("f0", Type.FLOAT16, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.FLOAT, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.DOUBLE, true), PARQUET_1_0),
          new Case(new Primitive("f0", Type.DECIMAL_18_6, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.DECIMAL_40_6, false), PARQUET_1_0),
          // Binary-like
          new Case(new Primitive("f0", Type.STRING, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.BINARY, true), PARQUET_1_0),
          new Case(new Primitive("f0", Type.FIXED_LEN_BYTE_ARRAY, true), PARQUET_1_0),
          // Temporal
          new Case(new Primitive("f0", Type.DATE, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.TIME_MILLIS, true), PARQUET_1_0),
          new Case(new Primitive("f0", Type.TIME_NANOS, false), PARQUET_1_0),
          new Case(new Primitive("f0", Type.TIMESTAMP_NANOS, true), PARQUET_1_0),
          // Nested types
          new Case(new ListOf("f0", new Primitive("element", Type.INT16, true), false), PARQUET_1_0),
          new Case(new ListOf("f0", new Primitive("element", Type.INT32, true), true), PARQUET_1_0),
          new Case(new ListOf("f0", new Primitive("element", Type.STRING, true), true), PARQUET_1_0),
          new Case(new StructOf("f0", new Primitive("f0", Type.INT32, true), false), PARQUET_1_0),
          new Case(new StructOf("f0", new Primitive("f0", Type.DOUBLE, true), true), PARQUET_1_0),
          new Case(
              new ListOf(
                  "f0", new StructOf("element", new Primitive("f0", Type.INT32, true), true), false),
              PARQUET_1_0),
          // V2 data pages
          new Case(new Primitive("f0", Type.BINARY, false), PARQUET_2_0),
          new Case(new ListOf("f0", new Primitive("element", Type.STRING, true), true), PARQUET_2_0));
    }

    @Test
    public void deleteOnce() throws IOException {
      List<Page> original = write(Table.concat(part1, part2, part3));
      List<Page> modified = write(Table.concat(part1, part3));
      // A single row group, so no other page changes
      assertPageLengthDifferences(original, modified, 0, 0, 1, editLength(part2));
      // The pages realign after the edit, so the last one is the same
      assertThat(modified).endsWith(original.get(original.size() - 1));
    }

    @Test
    public void deleteTwice() throws IOException {
      List<Page> original = write(Table.concat(part1, part2, part3, part4, part5));
      List<Page> modified = write(Table.concat(part1, part3, part5));
      // part2 and part4 have as many values
      assertPageLengthDifferences(original, modified, 0, 0, 2, editLength(part2));
      assertThat(modified).endsWith(original.get(original.size() - 1));
    }

    @Test
    public void updateOnce() throws IOException {
      List<Page> original = write(Table.concat(part1, part2, part3));
      List<Page> modified = write(Table.concat(part1, part4, part3));
      assertPageLengthDifferences(original, modified, 1);
      assertThat(modified).endsWith(original.get(original.size() - 1));
    }

    @Test
    public void updateTwice() throws IOException {
      List<Page> original = write(Table.concat(part1, part2, part3, part4, part5));
      List<Page> modified = write(Table.concat(part1, part6, part3, part7, part5));
      assertPageLengthDifferences(original, modified, 2);
      assertThat(modified).endsWith(original.get(original.size() - 1));
    }

    @Test
    public void insertOnce() throws IOException {
      List<Page> original = write(Table.concat(part1, part3));
      List<Page> modified = write(Table.concat(part1, part2, part3));
      assertPageLengthDifferences(original, modified, 0, 1, 0, editLength(part2));
      assertThat(modified).endsWith(original.get(original.size() - 1));
    }

    @Test
    public void insertTwice() throws IOException {
      List<Page> original = write(Table.concat(part1, part3, part5));
      List<Page> modified = write(Table.concat(part1, part2, part3, part4, part5));
      assertPageLengthDifferences(original, modified, 0, 2, 0, editLength(part2));
      assertThat(modified).endsWith(original.get(original.size() - 1));
    }

    @Test
    public void prepend() throws IOException {
      List<Page> original = write(Table.concat(part1, part2, part3));
      List<Page> modified = write(Table.concat(part4, part1, part2, part3));
      // As many pages or more at the start, and no later page larger
      assertThat(modified).hasSizeGreaterThanOrEqualTo(original.size());
      assertPageLengthDifferences(original, modified, 0, 1, 0, editLength(part4));
      assertThat(modified).endsWith(original.get(original.size() - 1));
    }

    @Test
    public void append() throws IOException {
      List<Page> original = write(Table.concat(part1, part2, part3));
      List<Page> modified = write(Table.concat(part1, part2, part3, part4));
      // More pages, or a larger last page; every other page is the same
      assertThat(modified).hasSizeGreaterThanOrEqualTo(original.size());
      assertThat(modified.subList(0, original.size() - 1))
          .containsExactlyElementsOf(original.subList(0, original.size() - 1));
      assertThat(modified.get(original.size() - 1).length())
          .isGreaterThanOrEqualTo(original.get(original.size() - 1).length());
    }

    @Test
    public void emptyTable() throws IOException {
      Table empty = generateTable(List.of(param.field()), 0, 0);
      WrittenTable written = writeTable(empty, OPTIONS, ROW_GROUP_LENGTH, false, param.version());

      // A single row group without data pages
      assertThat(pageLengths(written, 0)).containsExactly(List.of());
    }

    /** Dictionary encoding, which Arrow C++ runs each test with too, ends the same pages. */
    @Test
    public void dictionaryEncodingMovesNoPageEnd() throws IOException {
      Table table = Table.concat(part1, part2, part3);
      List<Integer> plain = write(table).stream().map(Page::length).toList();

      assertThat(pageLengths(writeTable(table, OPTIONS, ROW_GROUP_LENGTH, true, param.version()), 0))
          .containsExactly(plain);
    }

    /**
     * Writes the table into a single row group of more than one page, and checks that the values of its
     * pages weigh between the minimum and maximum chunk size.
     */
    private List<Page> write(Table table) throws IOException {
      List<List<Page>> pages = pages(writeTable(table, OPTIONS, ROW_GROUP_LENGTH, false, param.version()), 0);
      assertThat(pages).hasSize(1);
      assertThat(pages.get(0)).as("pages of the row group").hasSizeGreaterThan(1);
      assertContentDefinedChunkSizes(table, pages.get(0));
      return pages.get(0);
    }

    /**
     * Arrow C++'s AssertContentDefinedChunkSizes, for primitive columns only: with chunks of 4 to 16
     * KiB, required int32 pages of 1024, 4096 and 100 values (4 KiB, 16 KiB, 400 B) pass, as only the
     * last page may be under the minimum.
     */
    private void assertContentDefinedChunkSizes(Table table, List<Page> pages) {
      if (!(param.field() instanceof Primitive field)) {
        return;
      }
      int offset = 0;
      for (int page = 0; page < pages.size(); page++) {
        int length = pages.get(page).length();
        long size = 0;
        for (Group row : table.rows().subList(offset, offset + length)) {
          if (row.getFieldRepetitionCount(field.name()) > 0) {
            size += field.type() == Type.BOOLEAN
                ? 1
                : field.type().byteWidth > 0
                    ? field.type().byteWidth
                    : row.getBinary(field.name(), 0).length();
          }
          // Of nullable columns, the definition level too
          size += field.nullable() ? 2 : 0;
        }
        offset += length;
        if (page < pages.size() - 1) {
          assertThat(size)
              .as("chunk size of page %s", page)
              .isBetween(OPTIONS.getMinChunkSize(), OPTIONS.getMaxChunkSize());
        } else {
          assertThat(size).as("chunk size of the last page").isLessThanOrEqualTo(OPTIONS.getMaxChunkSize());
        }
      }
      assertThat(offset).as("values of the pages").isEqualTo(table.rows().size());
    }

    /** How many levels an edit adds or removes: the lists null, [], [a] and [b, c, d] are 1 + 1 + 1 + 3. */
    private int editLength(Table edit) {
      if (!(param.field() instanceof ListOf field)) {
        return edit.rows().size();
      }
      int length = 0;
      for (Group row : edit.rows()) {
        int elements = row.getFieldRepetitionCount(field.name()) == 0
            ? 0
            : row.getGroup(field.name(), 0).getFieldRepetitionCount("list");
        length += Math.max(1, elements);
      }
      return length;
    }
  }

  /** Arrow C++'s TestCDCMultipleRowGroups. */
  @Nested
  class MultipleRowGroups {

    private static final int PART_LENGTH = 128 * 1024;
    private static final int EDIT_LENGTH = 128;
    private static final int ROW_GROUP_LENGTH = 64 * 1024;
    private static final ContentDefinedChunkingOptions OPTIONS =
        new ContentDefinedChunkingOptions(4 * 1024, 16 * 1024, 0);

    private final List<Field> fields = List.of(
        new Primitive("int32", Type.INT32, true),
        new Primitive("float64", Type.DOUBLE, true),
        new Primitive("bool", Type.BOOLEAN, false));
    private final Table part1 = generateTable(fields, PART_LENGTH, 0);
    private final Table part2 = generateTable(fields, PART_LENGTH, 2);
    private final Table part3 = generateTable(fields, PART_LENGTH, 4);
    private final Table edit1 = generateTable(fields, EDIT_LENGTH, 1);
    private final Table edit2 = generateTable(fields, EDIT_LENGTH, 3);
    private final Table edit3 = generateTable(fields, EDIT_LENGTH, 5);

    /**
     * An insertion makes a larger difference in the row group it is in, and a smaller one at its end, as
     * the row groups have a fixed length; the later row groups shift likewise, and the last one grows.
     */
    @Test
    public void insertOnce() throws IOException {
      WrittenTable original = write(Table.concat(part1, edit1, part2, part3));
      WrittenTable modified = write(Table.concat(part1, edit1, edit2, part2, part3));

      for (int column = 0; column < fields.size(); column++) {
        List<List<Page>> originalPages = pages(original, column);
        List<List<Page>> modifiedPages = pages(modified, column);
        assertThat(originalPages).hasSize(7);
        assertThat(modifiedPages).hasSize(7);
        // Each part is two row groups, and the first is not edited
        assertThat(modifiedPages.subList(0, 2)).containsExactlyElementsOf(originalPages.subList(0, 2));
        for (int i = 2; i < 6; i++) {
          assertPageLengthDifferences(originalPages.get(i), modifiedPages.get(i), 0, 1, 1, EDIT_LENGTH);
        }
        assertPageLengthDifferences(originalPages.get(6), modifiedPages.get(6), 0, 1, 0, EDIT_LENGTH);
      }
    }

    /** A deletion makes a smaller difference, and the shift a larger one at the end of each row group. */
    @Test
    public void deleteOnce() throws IOException {
      WrittenTable original = write(Table.concat(part1, edit1, part2, part3, edit2));
      WrittenTable modified = write(Table.concat(part1, part2, part3, edit2));

      for (int column = 0; column < fields.size(); column++) {
        List<List<Page>> originalPages = pages(original, column);
        List<List<Page>> modifiedPages = pages(modified, column);
        assertThat(originalPages).hasSize(7);
        assertThat(modifiedPages).hasSize(7);
        assertThat(modifiedPages.subList(0, 2)).containsExactlyElementsOf(originalPages.subList(0, 2));
        for (int i = 2; i < 6; i++) {
          assertPageLengthDifferences(originalPages.get(i), modifiedPages.get(i), 0, 1, 1, EDIT_LENGTH);
        }
        assertPageLengthDifferences(originalPages.get(6), modifiedPages.get(6), 0, 0, 1, EDIT_LENGTH);
      }
    }

    /** An update shifts nothing, so it changes pages of one row group at most. */
    @Test
    public void updateOnce() throws IOException {
      WrittenTable original = write(Table.concat(part1, edit1, part2, part3, edit2));
      WrittenTable modified = write(Table.concat(part1, edit3, part2, part3, edit2));

      for (int column = 0; column < fields.size(); column++) {
        List<List<Page>> originalPages = pages(original, column);
        List<List<Page>> modifiedPages = pages(modified, column);
        assertThat(originalPages).hasSize(7);
        assertThat(modifiedPages).hasSize(7);
        assertThat(modifiedPages.subList(0, 2)).containsExactlyElementsOf(originalPages.subList(0, 2));
        assertPageLengthDifferences(originalPages.get(2), modifiedPages.get(2), 1);
        assertThat(modifiedPages.subList(3, 7)).containsExactlyElementsOf(originalPages.subList(3, 7));
      }
    }

    @Test
    public void append() throws IOException {
      WrittenTable original = write(Table.concat(part1, edit1, part2, part3));
      WrittenTable modified = write(Table.concat(part1, edit1, part2, part3, edit2));

      for (int column = 0; column < fields.size(); column++) {
        List<List<Page>> originalPages = pages(original, column);
        List<List<Page>> modifiedPages = pages(modified, column);
        assertThat(originalPages).hasSize(7);
        assertThat(modifiedPages).hasSize(7);
        assertThat(modifiedPages.subList(0, 6)).containsExactlyElementsOf(originalPages.subList(0, 6));
        // Only the last row group grows, and of it only the last page
        List<Page> originalLast = originalPages.get(6);
        List<Page> modifiedLast = modifiedPages.get(6);
        assertThat(modifiedLast).hasSameSizeAs(originalLast);
        assertThat(modifiedLast.subList(0, modifiedLast.size() - 1))
            .containsExactlyElementsOf(originalLast.subList(0, originalLast.size() - 1));
        assertThat(modifiedLast.get(modifiedLast.size() - 1).length())
            .isGreaterThan(originalLast.get(originalLast.size() - 1).length());
      }
    }

    /**
     * Splitting the data into row groups moves no page boundary but adds one at the end of each row
     * group: if the pages of a file of one row group end at rows 100, 250 and 400, then with row groups
     * of 200 rows they end at rows 100, 200, 250 and 400.
     */
    @Test
    public void independentOfRowGroupBoundaries() throws IOException {
      Table table = Table.concat(part1, part2, part3);
      int rows = table.rows().size();
      int rowGroupLength = rows / 6;
      WrittenTable single = writeTable(table, OPTIONS, rows, false, PARQUET_1_0);
      WrittenTable multi = writeTable(table, OPTIONS, rowGroupLength, false, PARQUET_1_0);

      assertThat(pageLengths(single, 0)).hasSize(1);
      assertThat(pageLengths(multi, 0)).hasSize(6);
      List<Integer> rowGroupEnds = new ArrayList<>();
      for (int end = rowGroupLength; end < rows; end += rowGroupLength) {
        rowGroupEnds.add(end);
      }
      for (int column = 0; column < fields.size(); column++) {
        List<Integer> chunkEnds = pageEnds(single, column);
        List<Integer> ends = pageEnds(multi, column);

        assertThat(chunkEnds)
            .as("pages of the single row group of column %s", column)
            .hasSizeGreaterThan(1);
        assertThat(ends).as("chunk ends of column %s", column).containsAll(chunkEnds);
        assertThat(ends).as("row group ends of column %s", column).containsAll(rowGroupEnds);
        assertThat(ends)
            .as("other page ends of column %s", column)
            .allMatch(end -> chunkEnds.contains(end) || rowGroupEnds.contains(end));
      }
    }

    /**
     * The page lengths a build of Arrow C++'s main branch, which has the unreleased apache/arrow#51685,
     * writes for this table: {@code WriteTable} with row groups of 64Ki rows, {@code CdcOptions{64 KiB,
     * 256 KiB, 0}}, V1 pages, no dictionary or compression, and no page size or rows per page limit.
     */
    @Test
    public void pageLengthsMatchArrowCpp() throws IOException {
      WrittenTable written = writeTable(
          Table.concat(part1, edit1, part2, part3),
          new ContentDefinedChunkingOptions(64 * 1024, 256 * 1024, 0),
          ROW_GROUP_LENGTH,
          false,
          PARQUET_1_0);

      assertThat(pageLengths(written, 0))
          .as("int32")
          .containsExactly(
              List.of(28446, 22751, 14339),
              List.of(8214, 23968, 20647, 12707),
              List.of(14647, 33235, 17654),
              List.of(2454, 29854, 20647, 12581),
              List.of(14645, 33235, 17656),
              List.of(2452, 29854, 20647, 12583),
              List.of(128));
      assertThat(pageLengths(written, 1))
          .as("float64")
          .containsExactly(
              List.of(13867, 13300, 9396, 10801, 15077, 3095),
              List.of(14371, 15918, 20443, 14285, 519),
              List.of(10507, 13212, 12970, 10801, 15077, 2969),
              List.of(14497, 15918, 20443, 14285, 393),
              List.of(13991, 13300, 9396, 10801, 15077, 2971),
              List.of(14495, 15918, 20443, 14285, 395),
              List.of(128));
      assertThat(pageLengths(written, 2))
          .as("bool")
          .containsExactly(
              List.of(65536),
              List.of(65536),
              List.of(16580, 48956),
              List.of(65536),
              List.of(16578, 48958),
              List.of(65536),
              List.of(128));
    }

    private WrittenTable write(Table table) throws IOException {
      WrittenTable written = writeTable(table, OPTIONS, ROW_GROUP_LENGTH, false, PARQUET_1_0);
      for (int column = 0; column < fields.size(); column++) {
        assertThat(pageLengths(written, column).get(0))
            .as("pages of the first row group of column %s", column)
            .hasSizeGreaterThan(1);
      }
      return written;
    }
  }

  /**
   * As {@link MultipleRowGroups#pageLengthsMatchArrowCpp}, for a nullable list of strings and NaNs of
   * differing payloads in one row group, with {@code CdcOptions{1 KiB, 2 KiB, -1}}, so that the NaN
   * pages of 256 values end at the maximum chunk size.
   */
  @Test
  public void pageLengthsOfListsAndNaNsMatchArrowCpp() throws IOException {
    Table table = generateTable(
        List.of(
            new ListOf("strings", new Primitive("element", Type.STRING, true), true),
            new Primitive("nan", Type.NAN, false)),
        2048,
        0);
    WrittenTable written = writeTable(
        table,
        new ContentDefinedChunkingOptions(1024, 2048, -1),
        table.rows().size(),
        false,
        PARQUET_1_0);

    assertThat(pageLengths(written, 0))
        .as("strings")
        .containsExactly(List.of(
            116, 60, 115, 87, 114, 63, 114, 63, 108, 120, 84, 116, 60, 115, 57, 120, 66, 108, 72, 86, 106,
            116, 88, 116, 70, 116, 79, 117, 84, 111, 81, 111, 33));
    assertThat(pageLengths(written, 1))
        .as("nan")
        .containsExactly(List.of(224, 233, 256, 217, 242, 256, 210, 221, 189));
  }

  /**
   * A chunk may end before the first value of a page, which leaves no page to end: 16-byte values end a
   * chunk of at most 16 bytes before each value, so row groups of 5 values make 5 pages of 1 value.
   */
  @Test
  public void chunkEndingBeforeAPageStartsWritesNoEmptyPage() throws IOException {
    Table table = generateTable(List.of(new Primitive("f0", Type.FIXED_LEN_BYTE_ARRAY, false)), 10, 0);
    WrittenTable written = writeTable(table, new ContentDefinedChunkingOptions(0, 16, -1), 5, false, PARQUET_1_0);

    assertThat(pageLengths(written, 0)).containsExactly(List.of(1, 1, 1, 1, 1), List.of(1, 1, 1, 1, 1));
  }

  /**
   * Arrow C++'s WriteTableToBuffer: writes the rows into a page store per row group, at least one, through
   * column write stores continuing the chunking of one set of chunkers, as a file writer does, and checks
   * that the rows read back are those written. Without page limits, pages end only at chunk and row group
   * ends. The edit tests leave dictionary encoding off, as an edit would renumber the values after it.
   */
  private static WrittenTable writeTable(
      Table table,
      ContentDefinedChunkingOptions options,
      int rowGroupLength,
      boolean dictionary,
      WriterVersion version) {
    ParquetProperties props = ParquetProperties.builder()
        .withContentDefinedChunking(options)
        .withDictionaryEncoding(dictionary)
        .withWriterVersion(version)
        .withPageSize(Integer.MAX_VALUE)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .build();
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(table.schema());
    List<ContentDefinedChunker> chunkers = ContentDefinedChunker.forColumns(table.schema(), options);
    List<MemPageStore> rowGroups = new ArrayList<>();
    int start = 0;
    do {
      List<Group> rows = table.rows()
          .subList(
              start, Math.min(start + rowGroupLength, table.rows().size()));
      MemPageStore pageStore = new MemPageStore(rows.size());
      ColumnWriteStore store = props.newColumnWriteStore(table.schema(), pageStore, column -> null, chunkers);
      RecordConsumer consumer = columnIO.getRecordWriter(store);
      GroupWriter writer = new GroupWriter(consumer, table.schema());
      rows.forEach(writer::write);
      consumer.flush();
      store.flush();

      // The record reader takes no empty row group
      if (!rows.isEmpty()) {
        RecordReader<Group> reader =
            columnIO.getRecordReader(pageStore, new GroupRecordConverter(table.schema()));
        for (Group row : rows) {
          assertThat(reader.read()).as("row read back").asString().isEqualTo(row.toString());
        }
      }
      rowGroups.add(pageStore);
      start += rowGroupLength;
    } while (start < table.rows().size());
    return new WrittenTable(table.schema(), rowGroups);
  }

  /** The row groups of a table written into memory, and its schema. */
  record WrittenTable(MessageType schema, List<MemPageStore> rowGroups) {}

  /** A data page: how many values it holds, and its levels and values as written, shown as 1069@3fa2b1c. */
  record Page(int length, Binary content) {

    @Override
    public String toString() {
      return length + "@" + Integer.toHexString(content.hashCode());
    }
  }

  /**
   * Arrow C++'s GetColumnParquetInfo: the data pages of a column, in each row group, none of them
   * empty.
   */
  private static List<List<Page>> pages(WrittenTable written, int column) throws IOException {
    ColumnDescriptor path = written.schema().getColumns().get(column);
    List<List<Page>> rowGroups = new ArrayList<>();
    for (MemPageStore rowGroup : written.rowGroups()) {
      List<Page> pages = new ArrayList<>();
      PageReader pageReader = rowGroup.getPageReader(path);
      for (long values = 0; values < pageReader.getTotalValueCount(); ) {
        DataPage page = pageReader.readPage();
        assertThat(page.getValueCount()).as("values of a data page").isPositive();
        BytesInput content = page instanceof DataPageV2 v2
            ? BytesInput.concat(v2.getRepetitionLevels(), v2.getDefinitionLevels(), v2.getData())
            : ((DataPageV1) page).getBytes();
        pages.add(new Page(page.getValueCount(), Binary.fromConstantByteArray(content.toByteArray())));
        values += page.getValueCount();
      }
      rowGroups.add(pages);
    }
    return rowGroups;
  }

  /** The value counts of the data pages of a column, in each row group. */
  private static List<List<Integer>> pageLengths(WrittenTable written, int column) throws IOException {
    return pages(written, column).stream()
        .map(rowGroup -> rowGroup.stream().map(Page::length).toList())
        .toList();
  }

  /** Arrow C++'s GetPageEnds: where the pages of a column end, counted in values across its row groups. */
  private static List<Integer> pageEnds(WrittenTable written, int column) throws IOException {
    List<Integer> ends = new ArrayList<>();
    int end = 0;
    for (List<Integer> rowGroup : pageLengths(written, column)) {
      for (int length : rowGroup) {
        end += length;
        ends.add(end);
      }
    }
    return ends;
  }

  /**
   * Arrow C++'s AssertPageLengthDifferences: deleting 128 values turns [100, 200, 300] into [100, 72,
   * 300], so no run keeps its size, none grows and one shrinks, by the edit length.
   */
  private static void assertPageLengthDifferences(
      List<Page> original,
      List<Page> modified,
      int equalDiffs,
      int largerDiffs,
      int smallerDiffs,
      int editLength) {
    List<Integer> differences = lengthDifferences(original, modified);
    String description = "length difference of each changed run of pages from " + original + " to " + modified;

    assertThat(differences)
        .as(description)
        .filteredOn(difference -> difference == 0)
        .hasSize(equalDiffs);
    assertThat(differences)
        .as(description)
        .filteredOn(difference -> difference > 0)
        .hasSize(largerDiffs);
    assertThat(differences)
        .as(description)
        .filteredOn(difference -> difference < 0)
        .hasSize(smallerDiffs);
    assertThat(differences)
        .as(description)
        .allSatisfy(difference -> assertThat(Math.abs(difference)).isIn(0, editLength));
  }

  /**
   * Arrow C++'s AssertPageLengthDifferences for updates, which shift no value: [100, 200, 300] to [100,
   * 150, 50, 300] is one run, holding as many values. Arrow C++ allows fewer runs, as an update moving
   * no page end leaves the page lengths alone.
   */
  private static void assertPageLengthDifferences(List<Page> original, List<Page> modified, int equalDiffs) {
    assertThat(lengthDifferences(original, modified))
        .as("length difference of each changed run of pages from %s to %s", original, modified)
        .hasSize(equalDiffs)
        .allMatch(difference -> difference == 0);
  }

  /**
   * Arrow C++'s FindDifferences, by page content rather than length, as a new page may hold as many
   * values as an unchanged one: the values each run of changed pages gains, 10 from [2, 3] to [7, 8].
   * Unlike Arrow C++, an inserted run is not merged with a later deleted one.
   */
  private static List<Integer> lengthDifferences(List<Page> original, List<Page> modified) {
    return DiffUtils.diff(original, modified).getDeltas().stream()
        .map(delta -> delta.getRevised().getLines().stream()
                .mapToInt(Page::length)
                .sum()
            - delta.getOriginal().getLines().stream()
                .mapToInt(Page::length)
                .sum())
        .toList();
  }

  /** Arrow C++'s hash, which generates deterministic and platform independent data. */
  static long hash(long seed, long index) {
    long h = (index + seed) * 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }

  /** Arrow C++'s GenerateTable: rows of the fields, each field generated from the next seed. */
  static Table generateTable(List<Field> fields, int length, long seed) {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { " + fields.stream().map(Field::declaration).collect(Collectors.joining(" ")) + " }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    List<Group> rows = new ArrayList<>(length);
    for (int index = 0; index < length; index++) {
      Group row = factory.newGroup();
      for (int i = 0; i < fields.size(); i++) {
        fields.get(i).append(row, seed + 1 + i, index);
      }
      rows.add(row);
    }
    return new Table(schema, rows);
  }

  /** Rows, and the schema they follow. */
  record Table(MessageType schema, List<Group> rows) {

    /** Arrow C++'s ConcatAndCombine. */
    static Table concat(Table... parts) {
      return new Table(
          parts[0].schema(),
          Arrays.stream(parts).flatMap(part -> part.rows().stream()).toList());
    }
  }

  /** A case of Arrow C++'s TestCDCSingleRowGroup: the column, and the version of its data pages. */
  record Case(Field field, WriterVersion version) {

    @Override
    public String toString() {
      return field.declaration() + " " + version;
    }
  }

  /** A field, and how Arrow C++'s GenerateArray generates its values. */
  interface Field {

    String declaration();

    /** Appends the value of the field in row {@code index}, unless it is null. */
    void append(Group group, long seed, int index);
  }

  /** A field of a primitive type: one in ten values is null if the field is nullable. */
  record Primitive(String name, Type type, boolean nullable) implements Field {

    @Override
    public String declaration() {
      return (nullable ? "optional " : "required ") + String.format(type.declaration, name) + ";";
    }

    @Override
    public void append(Group group, long seed, int index) {
      long value = hash(seed, index);
      if (!nullable || Long.remainderUnsigned(value, 10) != 0) {
        type.writer.append(group, name, value);
      }
    }
  }

  /**
   * A list field, its rows as Arrow C++ generates them: null, [], [e0], [e1, e2, e3], null, [], [e4],
   * [e5, e6, e7] and so on, with [] for null if the field is not nullable.
   */
  record ListOf(String name, Field element, boolean nullable) implements Field {

    // Arrow C++'s list offsets in each run of four lists, from the run's first element
    private static final int[] OFFSETS = {0, 0, 0, 1, 4};

    @Override
    public String declaration() {
      return (nullable ? "optional" : "required") + " group " + name + " (LIST) { repeated group list { "
          + element.declaration() + " } }";
    }

    @Override
    public void append(Group group, long seed, int index) {
      int kind = index % 4;
      if (kind == 0 && nullable) {
        return;
      }
      Group list = group.addGroup(name);
      int start = index - kind;
      for (int element = start + OFFSETS[kind]; element < start + OFFSETS[kind + 1]; element++) {
        this.element.append(list.addGroup("list"), seed, element);
      }
    }
  }

  /** A struct field of one field, never null. */
  record StructOf(String name, Field field, boolean nullable) implements Field {

    @Override
    public String declaration() {
      return (nullable ? "optional" : "required") + " group " + name + " { " + field.declaration() + " }";
    }

    @Override
    public void append(Group group, long seed, int index) {
      field.append(group.addGroup(name), seed, index);
    }
  }

  /**
   * The primitive types, with the value Arrow C++'s GenerateArray derives from a hash and the width
   * Arrow holds it in: INT16 appends {@code (short) hash} and weighs 2 bytes, as Arrow's int16. Booleans,
   * which Arrow packs into bits, and binaries, whose width varies, weigh 0.
   */
  enum Type {
    BOOLEAN("boolean %s", 0, (group, field, value) -> group.append(field, (value & 1) == 0)),
    UINT8("int32 %s (INTEGER(8,false))", 1, (group, field, value) -> group.append(field, (int) (value & 0xFF))),
    UINT16("int32 %s (INTEGER(16,false))", 2, (group, field, value) -> group.append(field, (int) (value & 0xFFFF))),
    UINT32("int32 %s (INTEGER(32,false))", 4, (group, field, value) -> group.append(field, (int) value)),
    UINT64("int64 %s (INTEGER(64,false))", 8, (group, field, value) -> group.append(field, value)),
    INT8("int32 %s (INTEGER(8,true))", 1, (group, field, value) -> group.append(field, (int) (byte) value)),
    INT16("int32 %s (INTEGER(16,true))", 2, (group, field, value) -> group.append(field, (int) (short) value)),
    INT32("int32 %s", 4, (group, field, value) -> group.append(field, (int) value)),
    INT64("int64 %s", 8, (group, field, value) -> group.append(field, value)),
    FLOAT16(
        "fixed_len_byte_array(2) %s (FLOAT16)",
        2, (group, field, value) -> {
          int bits = (int) Long.remainderUnsigned(value, 1000);
          group.append(field, Binary.fromConstantByteArray(new byte[] {(byte) bits, (byte) (bits >>> 8)}));
        }),
    FLOAT(
        "float %s",
        4, (group, field, value) -> group.append(field, (float) Long.remainderUnsigned(value, 1000) / 1000.0f)),
    DOUBLE(
        "double %s",
        8,
        (group, field, value) -> group.append(field, (double) Long.remainderUnsigned(value, 100000) / 1000.0)),
    // Arrow's decimal128(18, 6), which parquet-java stores and hashes as an INT64
    DECIMAL_18_6(
        "int64 %s (DECIMAL(18,6))",
        8, (group, field, value) -> group.append(field, Long.remainderUnsigned(value, 999_999_999_999L))),
    // Arrow's decimal256(40, 6), its values limited to 9 digits
    DECIMAL_40_6(
        "fixed_len_byte_array(17) %s (DECIMAL(40,6))",
        17,
        (group, field, value) -> group.append(
            field,
            FixedBinaryTestUtils.getFixedBinary(
                17, BigInteger.valueOf(Long.remainderUnsigned(value, 999_999_999L))))),
    STRING(
        "binary %s (STRING)",
        0, (group, field, value) -> group.append(field, "str_" + Long.toUnsignedString(value))),
    BINARY(
        "binary %s",
        0,
        (group, field, value) -> group.append(field, Binary.fromString("bin_" + Long.toUnsignedString(value)))),
    FIXED_LEN_BYTE_ARRAY(
        "fixed_len_byte_array(16) %s",
        16,
        (group, field, value) -> group.append(
            field, Binary.fromString(("bin_" + Long.toUnsignedString(value)).substring(0, 16)))),
    DATE("int32 %s (DATE)", 4, (group, field, value) -> group.append(field, (int) value)),
    TIME_MILLIS(
        "int32 %s (TIME(MILLIS,true))",
        4, (group, field, value) -> group.append(field, Math.abs((int) value % 86_400_000))),
    TIME_NANOS(
        "int64 %s (TIME(NANOS,true))",
        8, (group, field, value) -> group.append(field, Math.abs(value % 86_400_000_000L))),
    TIMESTAMP_NANOS("int64 %s (TIMESTAMP(NANOS,true))", 8, (group, field, value) -> group.append(field, value)),
    // Not one of Arrow C++'s: NaNs with the hash as their payload, which Arrow C++ hashes as it is
    NAN(
        "double %s",
        8, (group, field, value) -> group.append(field, Double.longBitsToDouble(0x7FF8000000000000L | value)));

    private final String declaration;
    private final int byteWidth;
    private final ValueWriter writer;

    Type(String declaration, int byteWidth, ValueWriter writer) {
      this.declaration = declaration;
      this.byteWidth = byteWidth;
      this.writer = writer;
    }
  }

  interface ValueWriter {
    void append(Group group, String field, long value);
  }
}
