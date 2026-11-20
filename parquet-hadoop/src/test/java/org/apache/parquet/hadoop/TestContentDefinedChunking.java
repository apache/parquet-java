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

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.FixedBinaryTestUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Arrow C++'s {@code parquet/chunker_internal_test.cc}: pages weigh between the minimum and maximum
 * chunk size, and an edit changes only the pages around it.
 */
public class TestContentDefinedChunking {

  @TempDir
  private Path tempDir;

  /** Arrow C++'s TestFindDifferences. */
  @Nested
  class FindDifferences {

    @Test
    public void basic() {
      assertThat(findDifferences(List.of(1, 2, 3, 4, 5), List.of(1, 7, 8, 4, 5)))
          .containsExactly(new ChunkDiff(List.of(2, 3), List.of(7, 8)));
    }

    @Test
    public void multipleDifferences() {
      assertThat(findDifferences(List.of(1, 2, 3, 4, 5, 6, 7), List.of(1, 8, 9, 4, 10, 6, 11)))
          .containsExactly(
              new ChunkDiff(List.of(2, 3), List.of(8, 9)),
              new ChunkDiff(List.of(5), List.of(10)),
              new ChunkDiff(List.of(7), List.of(11)));
    }

    @Test
    public void differentLengths() {
      assertThat(findDifferences(List.of(1, 2, 3), List.of(1, 2, 3, 4, 5)))
          .containsExactly(new ChunkDiff(List.of(), List.of(4, 5)));
    }

    @Test
    public void changesAtBothEnds() {
      assertThat(findDifferences(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9), List.of(0, 0, 2, 3, 4, 5, 7, 7, 8)))
          .containsExactly(
              new ChunkDiff(List.of(1), List.of(0, 0)),
              new ChunkDiff(List.of(6), List.of(7)),
              new ChunkDiff(List.of(9), List.of()));
    }

    @Test
    public void emptyArrays() {
      assertThat(findDifferences(List.of(), List.of())).isEmpty();
    }

    @Test
    public void longSequenceWithSingleDifference() {
      assertThat(findDifferences(
              List.of(1994, 2193, 2700, 1913, 2052), List.of(2048, 43, 2080, 2700, 1913, 2052)))
          .containsExactly(new ChunkDiff(List.of(1994, 2193), List.of(2048, 43, 2080)));
    }

    @Test
    public void longSequenceWithMiddleChanges() {
      assertThat(findDifferences(
              List.of(2169, 1976, 2180, 2147, 1934, 1772, 1914, 2075, 2154, 1940, 1934, 1970),
              List.of(2169, 1976, 2180, 2147, 2265, 1804, 1717, 1925, 2122, 1940, 1934, 1970)))
          .containsExactly(new ChunkDiff(
              List.of(1934, 1772, 1914, 2075, 2154), List.of(2265, 1804, 1717, 1925, 2122)));
    }

    @Test
    public void additionalCase() {
      assertThat(findDifferences(
              List.of(445, 312, 393, 401, 410, 138, 558, 457),
              List.of(445, 312, 393, 393, 410, 138, 558, 457)))
          .containsExactly(new ChunkDiff(List.of(401), List.of(393)));
    }
  }

  /** Arrow C++'s TestCDCSingleRowGroup, for each of its column types that Parquet stores apart. */
  @Nested
  @ParameterizedClass
  @MethodSource("cases")
  class SingleRowGroup {

    private static final int PART_SIZE = 128 * 1024;
    private static final int EDIT_SIZE = 128;
    private static final CdcOptions OPTIONS = new CdcOptions(4 * 1024, 16 * 1024, 0);
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
      List<Integer> original = write(Table.concat(part1, part2, part3));
      List<Integer> modified = write(Table.concat(part1, part3));
      // A single row group, so no other page changes
      assertPageLengthDifferences(original, modified, 0, 0, 1, editLength(part2));
    }

    @Test
    public void deleteTwice() throws IOException {
      List<Integer> original = write(Table.concat(part1, part2, part3, part4, part5));
      List<Integer> modified = write(Table.concat(part1, part3, part5));
      // part2 and part4 have as many values
      assertPageLengthDifferences(original, modified, 0, 0, 2, editLength(part2));
    }

    @Test
    public void updateOnce() throws IOException {
      List<Integer> original = write(Table.concat(part1, part2, part3));
      List<Integer> modified = write(Table.concat(part1, part4, part3));
      assertPageLengthDifferences(original, modified, 1);
    }

    @Test
    public void updateTwice() throws IOException {
      List<Integer> original = write(Table.concat(part1, part2, part3, part4, part5));
      List<Integer> modified = write(Table.concat(part1, part6, part3, part7, part5));
      assertPageLengthDifferences(original, modified, 2);
    }

    @Test
    public void insertOnce() throws IOException {
      List<Integer> original = write(Table.concat(part1, part3));
      List<Integer> modified = write(Table.concat(part1, part2, part3));
      assertPageLengthDifferences(original, modified, 0, 1, 0, editLength(part2));
    }

    @Test
    public void insertTwice() throws IOException {
      List<Integer> original = write(Table.concat(part1, part3, part5));
      List<Integer> modified = write(Table.concat(part1, part2, part3, part4, part5));
      assertPageLengthDifferences(original, modified, 0, 2, 0, editLength(part2));
    }

    @Test
    public void prepend() throws IOException {
      List<Integer> original = write(Table.concat(part1, part2, part3));
      List<Integer> modified = write(Table.concat(part4, part1, part2, part3));
      // As many pages or more at the start, and no later page larger
      assertThat(modified).hasSizeGreaterThanOrEqualTo(original.size());
      assertPageLengthDifferences(original, modified, 0, 1, 0, editLength(part4));
    }

    @Test
    public void append() throws IOException {
      List<Integer> original = write(Table.concat(part1, part2, part3));
      List<Integer> modified = write(Table.concat(part1, part2, part3, part4));
      // More pages, or a larger last page; every other page is the same
      assertThat(modified).hasSizeGreaterThanOrEqualTo(original.size());
      assertThat(modified.subList(0, original.size() - 1))
          .containsExactlyElementsOf(original.subList(0, original.size() - 1));
      assertThat(modified.get(original.size() - 1)).isGreaterThanOrEqualTo(original.get(original.size() - 1));
    }

    @Test
    public void emptyTable() throws IOException {
      Table empty = generateTable(List.of(param.field()), 0, 0);
      Path file = writeTable(empty, OPTIONS, ROW_GROUP_LENGTH, param.version());

      // Arrow C++ writes an empty row group, parquet-java none
      assertThat(pageLengths(file, 0)).isEmpty();
    }

    /**
     * Writes the table into a single row group, and checks that the values of its pages weigh between
     * the minimum and maximum chunk size.
     */
    private List<Integer> write(Table table) throws IOException {
      List<List<Integer>> pages = pageLengths(writeTable(table, OPTIONS, ROW_GROUP_LENGTH, param.version()), 0);
      assertThat(pages).hasSize(1);
      assertContentDefinedChunkSizes(table, pages.get(0));
      return pages.get(0);
    }

    /**
     * Arrow C++'s AssertContentDefinedChunkSizes, for primitive columns only: with chunks of 4 to 16
     * KiB, required int32 pages of 1024, 4096 and 100 values (4 KiB, 16 KiB, 400 B) pass, as only the
     * last page may be under the minimum.
     */
    private void assertContentDefinedChunkSizes(Table table, List<Integer> pageLengths) {
      if (!(param.field() instanceof Primitive field)) {
        return;
      }
      int offset = 0;
      for (int page = 0; page < pageLengths.size(); page++) {
        long size = 0;
        for (Group row : table.rows().subList(offset, offset + pageLengths.get(page))) {
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
        offset += pageLengths.get(page);
        if (page < pageLengths.size() - 1) {
          assertThat(size)
              .as("chunk size of page %s", page)
              .isBetween(OPTIONS.minChunkSize(), OPTIONS.maxChunkSize());
        } else {
          assertThat(size).as("chunk size of the last page").isLessThanOrEqualTo(OPTIONS.maxChunkSize());
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
    private static final CdcOptions OPTIONS = new CdcOptions(4 * 1024, 16 * 1024, 0);

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
      Path original = write(Table.concat(part1, edit1, part2, part3));
      Path modified = write(Table.concat(part1, edit1, edit2, part2, part3));

      for (int column = 0; column < fields.size(); column++) {
        List<List<Integer>> originalPages = pageLengths(original, column);
        List<List<Integer>> modifiedPages = pageLengths(modified, column);
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
      Path original = write(Table.concat(part1, edit1, part2, part3, edit2));
      Path modified = write(Table.concat(part1, part2, part3, edit2));

      for (int column = 0; column < fields.size(); column++) {
        List<List<Integer>> originalPages = pageLengths(original, column);
        List<List<Integer>> modifiedPages = pageLengths(modified, column);
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
      Path original = write(Table.concat(part1, edit1, part2, part3, edit2));
      Path modified = write(Table.concat(part1, edit3, part2, part3, edit2));

      for (int column = 0; column < fields.size(); column++) {
        List<List<Integer>> originalPages = pageLengths(original, column);
        List<List<Integer>> modifiedPages = pageLengths(modified, column);
        assertThat(originalPages).hasSize(7);
        assertThat(modifiedPages).hasSize(7);
        assertThat(modifiedPages.subList(0, 2)).containsExactlyElementsOf(originalPages.subList(0, 2));
        assertPageLengthDifferences(originalPages.get(2), modifiedPages.get(2), 1);
        assertThat(modifiedPages.subList(2, 7)).containsExactlyElementsOf(originalPages.subList(2, 7));
      }
    }

    @Test
    public void append() throws IOException {
      Path original = write(Table.concat(part1, edit1, part2, part3));
      Path modified = write(Table.concat(part1, edit1, part2, part3, edit2));

      for (int column = 0; column < fields.size(); column++) {
        List<List<Integer>> originalPages = pageLengths(original, column);
        List<List<Integer>> modifiedPages = pageLengths(modified, column);
        assertThat(originalPages).hasSize(7);
        assertThat(modifiedPages).hasSize(7);
        assertThat(modifiedPages.subList(0, 6)).containsExactlyElementsOf(originalPages.subList(0, 6));
        // Only the last row group grows, and of it only the last page
        List<Integer> originalLast = originalPages.get(6);
        List<Integer> modifiedLast = modifiedPages.get(6);
        assertThat(modifiedLast).hasSameSizeAs(originalLast);
        assertThat(modifiedLast.subList(0, modifiedLast.size() - 1))
            .containsExactlyElementsOf(originalLast.subList(0, originalLast.size() - 1));
        assertThat(modifiedLast.get(modifiedLast.size() - 1))
            .isGreaterThan(originalLast.get(originalLast.size() - 1));
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
      Path single = writeTable(table, OPTIONS, rows, PARQUET_1_0);
      Path multi = writeTable(table, OPTIONS, rowGroupLength, PARQUET_1_0);

      assertThat(pageLengths(single, 0)).hasSize(1);
      assertThat(pageLengths(multi, 0)).hasSize(6);
      for (int column = 0; column < fields.size(); column++) {
        List<Integer> singleEnds = pageEnds(single, column);
        List<Integer> expected = new ArrayList<>(singleEnds);
        for (int i = 1; i < 6; i++) {
          expected.add(i * rowGroupLength);
        }
        Collections.sort(expected);

        assertThat(pageEnds(multi, column)).as("column %s", column).containsExactlyElementsOf(expected);
      }
    }

    /**
     * The page lengths a build of Arrow C++'s main branch, which has the unreleased apache/arrow#51685,
     * writes for this table: {@code WriteTable} with row groups of 64Ki rows, {@code CdcOptions{64 KiB,
     * 256 KiB, 0}}, V1 pages, no dictionary or compression, and no page size or rows per page limit.
     */
    @Test
    public void pageLengthsMatchArrowCpp() throws IOException {
      Path file = writeTable(
          Table.concat(part1, edit1, part2, part3),
          new CdcOptions(64 * 1024, 256 * 1024, 0),
          ROW_GROUP_LENGTH,
          PARQUET_1_0);

      assertThat(pageLengths(file, 0))
          .as("int32")
          .containsExactly(
              List.of(28446, 22751, 14339),
              List.of(8214, 23968, 20647, 12707),
              List.of(14647, 33235, 17654),
              List.of(2454, 29854, 20647, 12581),
              List.of(14645, 33235, 17656),
              List.of(2452, 29854, 20647, 12583),
              List.of(128));
      assertThat(pageLengths(file, 1))
          .as("float64")
          .containsExactly(
              List.of(13867, 13300, 9396, 10801, 15077, 3095),
              List.of(14371, 15918, 20443, 14285, 519),
              List.of(10507, 13212, 12970, 10801, 15077, 2969),
              List.of(14497, 15918, 20443, 14285, 393),
              List.of(13991, 13300, 9396, 10801, 15077, 2971),
              List.of(14495, 15918, 20443, 14285, 395),
              List.of(128));
      assertThat(pageLengths(file, 2))
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

    private Path write(Table table) throws IOException {
      return writeTable(table, OPTIONS, ROW_GROUP_LENGTH, PARQUET_1_0);
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
    Path file =
        writeTable(table, new CdcOptions(1024, 2048, -1), table.rows().size(), PARQUET_1_0);

    assertThat(pageLengths(file, 0))
        .as("strings")
        .containsExactly(List.of(
            116, 60, 115, 87, 114, 63, 114, 63, 108, 120, 84, 116, 60, 115, 57, 120, 66, 108, 72, 86, 106,
            116, 88, 116, 70, 116, 79, 117, 84, 111, 81, 111, 33));
    assertThat(pageLengths(file, 1))
        .as("nan")
        .containsExactly(List.of(224, 233, 256, 217, 242, 256, 210, 221, 189));
  }

  /**
   * The record writer's page size checks, which also flush held back nulls, only add page ends: with
   * chunks ending after 100 and 250 values, pages may end after 40, 100, 180 and 250, never after 40,
   * 120 and 250.
   */
  @Test
  public void pageSizeCheckScheduleMovesNoChunkBoundary() throws Exception {
    Table table =
        generateTable(List.of(new ListOf("f0", new Primitive("element", Type.STRING, true), true)), 40_000, 0);
    CdcOptions options = new CdcOptions(16 * 1024, 64 * 1024, 0);
    Configuration conf = outputFormatConf(table.schema());
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, options.minChunkSize());
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MAX_SIZE, options.maxChunkSize());
    conf.setInt(ParquetOutputFormat.PAGE_SIZE, 8 * 1024);

    List<Integer> chunks = pageEnds(writeTable(table, options, table.rows().size(), PARQUET_1_0), 0);
    assertThat(pageEnds(writeThroughOutputFormat(conf, table, "scheduled"), 0))
        .containsAll(chunks)
        .hasSizeGreaterThan(2 * chunks.size());
  }

  @Test
  public void invalidOptionsFailOnlyWhileEnabled() {
    Table empty = generateTable(List.of(new Primitive("f0", Type.INT32, false)), 0, 0);
    Configuration conf = outputFormatConf(empty.schema());
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, 8 * 1024 * 1024);

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, false);
    assertThatCode(() -> writeThroughOutputFormat(conf, empty, "off")).doesNotThrowAnyException();

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    assertThatThrownBy(() -> writeThroughOutputFormat(conf, empty, "on"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking size range: maximum chunk size (1048576) must be greater "
            + "than minimum chunk size (8388608)");
    assertThat(tempDir.resolve("on.parquet")).doesNotExist();
  }

  @Test
  public void configurationKeysReachTheWriter() throws Exception {
    Table table = generateTable(List.of(new Primitive("f0", Type.STRING, true)), 40_000, 0);
    Configuration conf = outputFormatConf(table.schema());
    CdcOptions options = new CdcOptions(16 * 1024, 64 * 1024, 1);
    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, true);
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MIN_SIZE, options.minChunkSize());
    conf.setLong(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_MAX_SIZE, options.maxChunkSize());
    conf.setInt(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_NORM_LEVEL, options.normLevel());

    List<List<Integer>> pages = pageLengths(writeThroughOutputFormat(conf, table, "configured"), 0);
    assertThat(pages)
        .containsExactlyElementsOf(
            pageLengths(writeTable(table, options, table.rows().size(), PARQUET_1_0), 0));
    assertThat(pages.get(0)).hasSizeGreaterThan(1);

    conf.setBoolean(ParquetOutputFormat.CONTENT_DEFINED_CHUNKING_ENABLED, false);
    assertThat(pageLengths(writeThroughOutputFormat(conf, table, "unconfigured"), 0))
        .containsExactly(List.of(table.rows().size()));
  }

  private Path path(String label) {
    return tempDir.resolve(label + ".parquet");
  }

  /**
   * Arrow C++'s WriteTableToBuffer, checking that the rows read back are those written. Without page
   * limits, pages end only at chunk and row group ends; dictionary encoding is off, as it moves none.
   */
  private Path writeTable(Table table, CdcOptions options, int rowGroupLength, WriterVersion version)
      throws IOException {
    Path file = path(UUID.randomUUID().toString());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
        .withType(table.schema())
        .withContentDefinedChunking(true)
        .withContentDefinedChunkingMinSize(options.minChunkSize())
        .withContentDefinedChunkingMaxSize(options.maxChunkSize())
        .withContentDefinedChunkingNormLevel(options.normLevel())
        .withRowGroupRowCountLimit(rowGroupLength)
        .withDictionaryEncoding(false)
        .withWriterVersion(version)
        .withPageSize(Integer.MAX_VALUE)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .build()) {
      for (Group row : table.rows()) {
        writer.write(row);
      }
    }
    try (ParquetReader<Group> reader = ParquetReader.builder(
            new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toUri()))
        .build()) {
      for (Group row : table.rows()) {
        assertThat(reader.read()).as("row read back").asString().isEqualTo(row.toString());
      }
      assertThat(reader.read())
          .as("row read back beyond the written ones")
          .isNull();
    }
    return file;
  }

  /** The configuration of a {@link ParquetOutputFormat} without the page size and row count limits. */
  private static Configuration outputFormatConf(MessageType schema) {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    conf.setInt(ParquetOutputFormat.PAGE_ROW_COUNT_LIMIT, Integer.MAX_VALUE);
    conf.setInt(ParquetOutputFormat.PAGE_SIZE, Integer.MAX_VALUE);
    conf.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, false);
    return conf;
  }

  private Path writeThroughOutputFormat(Configuration conf, Table table, String label) throws Exception {
    Path file = path(label);
    RecordWriter<Void, Group> writer = new ExampleOutputFormat()
        .getRecordWriter(conf, new org.apache.hadoop.fs.Path(file.toUri()), CompressionCodecName.UNCOMPRESSED);
    try {
      for (Group row : table.rows()) {
        writer.write(null, row);
      }
    } finally {
      writer.close(null);
    }
    return file;
  }

  /** Arrow C++'s CdcOptions. */
  record CdcOptions(long minChunkSize, long maxChunkSize, int normLevel) {}

  /**
   * Arrow C++'s GetColumnParquetInfo: the value counts of the data pages of a column, in each row
   * group, none of them empty.
   */
  private static List<List<Integer>> pageLengths(Path file, int column) throws IOException {
    List<List<Integer>> rowGroups = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
      ColumnDescriptor path =
          reader.getFileMetaData().getSchema().getColumns().get(column);
      for (PageReadStore rowGroup = reader.readNextRowGroup();
          rowGroup != null;
          rowGroup = reader.readNextRowGroup()) {
        List<Integer> pages = new ArrayList<>();
        PageReader pageReader = rowGroup.getPageReader(path);
        for (DataPage page = pageReader.readPage(); page != null; page = pageReader.readPage()) {
          assertThat(page.getValueCount()).as("values of a data page").isPositive();
          pages.add(page.getValueCount());
        }
        rowGroups.add(pages);
      }
    }
    return rowGroups;
  }

  /** Arrow C++'s GetPageEnds: where the pages of a column end, counted in values across its row groups. */
  private static List<Integer> pageEnds(Path file, int column) throws IOException {
    List<Integer> ends = new ArrayList<>();
    int end = 0;
    for (List<Integer> rowGroup : pageLengths(file, column)) {
      for (int length : rowGroup) {
        end += length;
        ends.add(end);
      }
    }
    return ends;
  }

  /** A run of page lengths that differ, like a hunk of a diff: [2, 3] to [7, 8]. */
  record ChunkDiff(List<Integer> first, List<Integer> second) {

    /** How many values the run gains: 10 from [2, 3] to [7, 8]. */
    int growth() {
      return second.stream().mapToInt(Integer::intValue).sum()
          - first.stream().mapToInt(Integer::intValue).sum();
    }
  }

  /**
   * Arrow C++'s FindDifferences, aligning the page lengths by their longest common subsequence: [1, 2,
   * 3, 4, 5] and [1, 7, 8, 4, 5] differ in the run [2, 3] to [7, 8]. A run only inserting next to one
   * only deleting is merged: [] to [9] and [6] to [] make [6] to [9].
   */
  static List<ChunkDiff> findDifferences(List<Integer> first, List<Integer> second) {
    int n = first.size();
    int m = second.size();
    int[][] common = new int[n + 1][m + 1];
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < m; j++) {
        common[i + 1][j + 1] = first.get(i).equals(second.get(j))
            ? common[i][j] + 1
            : Math.max(common[i + 1][j], common[i][j + 1]);
      }
    }

    List<int[]> matches = new ArrayList<>();
    for (int i = n, j = m; i > 0 && j > 0; ) {
      if (first.get(i - 1).equals(second.get(j - 1))) {
        matches.add(new int[] {--i, --j});
      } else if (common[i - 1][j] >= common[i][j - 1]) {
        i--;
      } else {
        j--;
      }
    }
    Collections.reverse(matches);

    List<ChunkDiff> diffs = new ArrayList<>();
    int lastI = 0;
    int lastJ = 0;
    for (int[] match : matches) {
      if (match[0] > lastI || match[1] > lastJ) {
        diffs.add(new ChunkDiff(first.subList(lastI, match[0]), second.subList(lastJ, match[1])));
      }
      lastI = match[0] + 1;
      lastJ = match[1] + 1;
    }
    if (lastI < n || lastJ < m) {
      diffs.add(new ChunkDiff(first.subList(lastI, n), second.subList(lastJ, m)));
    }

    List<ChunkDiff> merged = new ArrayList<>();
    for (ChunkDiff diff : diffs) {
      ChunkDiff previous = merged.isEmpty() ? null : merged.get(merged.size() - 1);
      if (previous != null && previous.first().isEmpty() && diff.second().isEmpty()) {
        merged.set(merged.size() - 1, new ChunkDiff(diff.first(), previous.second()));
      } else if (previous != null
          && previous.second().isEmpty()
          && diff.first().isEmpty()) {
        merged.set(merged.size() - 1, new ChunkDiff(previous.first(), diff.second()));
      } else {
        merged.add(diff);
      }
    }
    return merged;
  }

  /**
   * Arrow C++'s AssertPageLengthDifferences: deleting 128 values turns [100, 200, 300] into [100, 72,
   * 300], so no run keeps its size, none grows and one shrinks, by the edit length.
   */
  private static void assertPageLengthDifferences(
      List<Integer> original,
      List<Integer> modified,
      int equalDiffs,
      int largerDiffs,
      int smallerDiffs,
      int editLength) {
    List<Integer> growths = findDifferences(original, modified).stream()
        .map(ChunkDiff::growth)
        .toList();
    String description = "growth of each run of pages from " + original + " to " + modified;

    assertThat(growths).as(description).filteredOn(growth -> growth == 0).hasSize(equalDiffs);
    assertThat(growths).as(description).filteredOn(growth -> growth > 0).hasSize(largerDiffs);
    assertThat(growths).as(description).filteredOn(growth -> growth < 0).hasSize(smallerDiffs);
    assertThat(growths)
        .as(description)
        .allSatisfy(growth -> assertThat(Math.abs(growth)).isIn(0, editLength));
  }

  /**
   * Arrow C++'s AssertPageLengthDifferences for updates, which shift no value: [100, 200, 300] to [100,
   * 150, 50, 300] is one run, holding as many values.
   */
  private static void assertPageLengthDifferences(List<Integer> original, List<Integer> modified, int maxEqualDiffs) {
    List<Integer> growths = findDifferences(original, modified).stream()
        .map(ChunkDiff::growth)
        .toList();

    assertThat(growths)
        .as("growth of each run of pages from %s to %s", original, modified)
        .hasSizeLessThanOrEqualTo(maxEqualDiffs)
        .allMatch(growth -> growth == 0);
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
