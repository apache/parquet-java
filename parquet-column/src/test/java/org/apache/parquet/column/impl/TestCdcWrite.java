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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Drives {@link ColumnWriteStore} the way the record writer does and checks the pages that come
 * out, so this covers {@link ChunkingColumnWriter} rather than the chunker on its
 * own. Assertions are enabled under Surefire, so a page cut landing off a record boundary trips
 * {@code ColumnWriterBase}'s own assert here rather than producing a quietly invalid file.
 *
 * <p>Every case raises the page row count limit and the page size, because at their defaults they
 * cut pages long before a content defined boundary is reached and the test would measure them
 * instead.
 */
public class TestCdcWrite {

  private static final CdcOptions OPTIONS = options(4 * 1024, 16 * 1024, 0);

  private static final MessageType REQUIRED = MessageTypeParser.parseMessageType("message m { required int64 v; }");
  private static final MessageType OPTIONAL = MessageTypeParser.parseMessageType("message m { optional int64 v; }");
  private static final MessageType BINARY = MessageTypeParser.parseMessageType("message m { optional binary v; }");
  private static final MessageType REPEATED = MessageTypeParser.parseMessageType("message m { repeated int64 v; }");
  private static final MessageType REQUIRED_BINARY =
      MessageTypeParser.parseMessageType("message m { required binary v; }");

  /**
   * A value wider than the whole size envelope closes a chunk by itself, so the very first value
   * written asks for a page break with nothing yet in the page. The write path has to swallow that
   * one: {@link ColumnWriterBase#writePage()} rejects an empty page outright. Arrow C++ pushes the
   * same zero-length chunk and filters it downstream instead, which comes to the same thing.
   */
  @Test
  public void aBoundaryOnTheFirstValueOfAPageDoesNotWriteAnEmptyOne() {
    List<DataPage> pages = writePages(REQUIRED_BINARY, unboundedProps(options(0, 32, 0), true), 4, (writer, i) -> {
      byte[] wide = new byte[64];
      Arrays.fill(wide, (byte) i);
      writer.write(Binary.fromConstantByteArray(wide), 0, 0);
    });

    // Each 64-byte value overshoots the 32-byte maximum on its own, so every value but the first
    // closes the page before it and no page is ever empty.
    assertThat(valueCounts(pages)).containsExactly(1, 1, 1, 1);
  }

  /**
   * Every primitive type reaches the chunker.
   *
   * <p>{@link ChunkingColumnWriter} hooks each {@code write} overload separately, so a type whose hook
   * is missing keeps writing position-based pages while every other type chunks -- silently, and
   * only for files with such a column. Driving the chunker directly cannot see this; the hook has
   * to be exercised through a real column writer, which is what this does.
   */
  @ParameterizedTest
  @MethodSource("primitiveColumns")
  public void everyPrimitiveTypeGoesThroughTheChunker(String label, MessageType schema) {
    // With the position based limits out of the way, a column that is not chunked is one page.
    assertThat(primitivePageValueCounts(schema, false)).hasSize(1);
    assertThat(primitivePageValueCounts(schema, true)).hasSizeGreaterThan(1);
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

  /** Writes 60 000 values of {@code schema}'s primitive type and returns the pages they land in. */
  private static List<Integer> primitivePageValueCounts(MessageType schema, boolean chunking) {
    PrimitiveTypeName type = schema.getColumns().get(0).getPrimitiveType().getPrimitiveTypeName();
    long[] values = newLongs(60_000, 13);
    return valueCounts(writePages(schema, unboundedProps(OPTIONS, chunking), values.length, (writer, i) -> {
      long value = values[i];
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
          // BINARY and FIXED_LEN_BYTE_ARRAY both take the eight value bytes.
          writer.write(Binary.fromConstantByteArray(BytesUtils.longToBytes(value)), 0, 0);
          break;
      }
    }));
  }

  // --------------------------------------------------------------- basics

  @Test
  public void cutsMoreThanOnePageAndAccountsForEveryValue() {
    long[] values = newLongs(30_000, 1);
    List<Integer> pages = pageValueCounts(REQUIRED, values);
    assertThat(pages).hasSizeGreaterThan(1);
    assertThat(pages.stream().mapToInt(Integer::intValue).sum()).isEqualTo(values.length);
  }

  // ------------------------------------------------- the dedup property

  @ParameterizedTest
  @MethodSource("schemas")
  public void anEditLeavesTheSurroundingPagesAlone(String label, MessageType schema) {
    long[] original = newLongs(60_000, 11);
    long[] edited = insert(original, 25_000, newLongs(300, 42));

    List<Integer> before = pageValueCounts(schema, original);
    List<Integer> after = pageValueCounts(schema, edited);

    int prefix = sharedPrefix(before, after);
    int suffix = sharedSuffix(before, after, prefix);

    assertThat(prefix).as("pages shared before the edit").isPositive();
    assertThat(suffix).as("pages shared after the edit").isPositive();
    assertThat(prefix + suffix).as("pages left untouched by the edit").isGreaterThan(before.size() * 3 / 4);
  }

  static Stream<Arguments> schemas() {
    return Stream.of(
        Arguments.of("required int64", REQUIRED),
        Arguments.of("optional int64", OPTIONAL),
        Arguments.of("optional binary", BINARY),
        Arguments.of("repeated int64", REPEATED));
  }

  @Test
  public void onlyChunkingRealignsAfterAnInsertion() throws IOException {
    // Both writers share the pages *before* an insertion -- those bytes never moved. The
    // difference, and the whole point of the feature, is what happens after it: a position-based
    // writer shares nothing beyond that prefix, while chunking resynchronises and shares the tail
    // as well. Page bytes, not value counts: a fixed-size writer emits same-sized pages after an
    // insertion, so counts alone would hide that every one of them shifted.
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

  /**
   * Writes with a page size small enough that the position-based limits produce many pages -- which
   * is what makes a with/without comparison mean anything -- and returns every page's bytes.
   */
  private static List<Binary> boundedPageBytes(long[] values, boolean chunking) throws IOException {
    ParquetProperties props = ParquetProperties.builder()
        .withPageSize(16 * 1024)
        .withMinRowCountForPageSizeCheck(1)
        .withDictionaryEncoding(false)
        .withContentDefinedChunking(OPTIONS)
        .withContentDefinedChunking(chunking)
        .build();

    List<Binary> pages = new ArrayList<>();
    for (DataPage page : writePages(REQUIRED, props, values.length, (writer, i) -> writer.write(values[i], 0, 0))) {
      pages.add(
          Binary.fromConstantByteArray(((DataPageV1) page).getBytes().toByteArray()));
    }
    return pages;
  }

  // ------------------------------------------------ the required null path

  /**
   * A null written to a required column must contribute nothing at all to the rolling hash.
   *
   * <p>{@code MessageColumnIO} reaches {@code writeNull(0, 0)} for a required field the record
   * simply omitted, and there {@code definitionLevel == maxDef == 0}: there are no levels to hash
   * and, despite that equality, no value either. Feeding one would look exactly like a real value
   * to the chunker.
   *
   * <p>The nulls are spread through the column rather than bunched at the front, because a gear
   * hash only remembers its last few bytes -- prepending anything perturbs almost nothing
   * downstream, which is the whole point of the algorithm and would make a leading run a useless
   * probe. Page boundaries are then compared in terms of how many real values precede them, which
   * is the part the nulls must not disturb.
   */
  @Test
  public void aNullOnARequiredColumnContributesNothingToTheHash() {
    long[] values = newLongs(30_000, 17);

    List<Integer> withoutNulls = requiredPageBreaksByValueIndex(values, 0);
    // Without this the comparison holds trivially when chunking produces a single page on both
    // sides, which is exactly what a broken chunker does.
    assertThat(withoutNulls).hasSizeGreaterThan(1);
    assertThat(requiredPageBreaksByValueIndex(values, 100)).containsExactlyElementsOf(withoutNulls);
  }

  /**
   * Writes a required column, injecting a {@code writeNull(0, 0)} before every
   * {@code nullEvery}-th value ({@code 0} to inject none), and returns each page boundary as the
   * number of real values written before it.
   *
   * <p>Such a chunk is not a valid one -- a required column has nowhere to record a null, which is
   * a pre-existing property of this writer rather than anything chunking introduces -- but its
   * page boundaries are what this reads.
   */
  private static List<Integer> requiredPageBreaksByValueIndex(long[] values, int nullEvery) {
    // slots[k] is the value written to slot k, or null for an injected null, and
    // valueIndexBySlot[k] is how many real values had been written before it.
    List<Long> slots = new ArrayList<>();
    List<Integer> valueIndexBySlot = new ArrayList<>();
    for (int i = 0; i < values.length; ++i) {
      if (nullEvery > 0 && i % nullEvery == 0) {
        slots.add(null);
        valueIndexBySlot.add(i);
      }
      slots.add(values[i]);
      valueIndexBySlot.add(i);
    }

    List<DataPage> pages = writePages(REQUIRED, unboundedProps(OPTIONS, true), slots.size(), (writer, k) -> {
      Long value = slots.get(k);
      if (value == null) {
        writer.writeNull(0, 0);
      } else {
        writer.write(value.longValue(), 0, 0);
      }
    });

    List<Integer> breaks = new ArrayList<>();
    int slot = 0;
    for (int count : valueCounts(pages)) {
      slot += count;
      // Where this page ends, counted in real values rather than written slots.
      breaks.add(slot < valueIndexBySlot.size() ? valueIndexBySlot.get(slot) : values.length);
    }
    return breaks;
  }

  // ----------------------------------------------------------- helpers

  /** Writes one column of {@code values} and returns the value count of each page produced. */
  private static List<Integer> pageValueCounts(MessageType schema, long[] values) {
    ColumnDescriptor path = schema.getColumns().get(0);
    boolean binary = path.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY;
    int maxDef = path.getMaxDefinitionLevel();
    return valueCounts(writePages(schema, unboundedProps(OPTIONS, true), values.length, (writer, i) -> {
      long value = values[i];
      if (maxDef > 0 && value % 8 == 0) {
        // A null every so often, so the optional and repeated schemas exercise the level paths.
        writer.writeNull(0, maxDef - 1);
      } else if (binary) {
        writer.write(Binary.fromString(Long.toString(value)), 0, maxDef);
      } else {
        writer.write(value, 0, maxDef);
      }
    }));
  }
}
