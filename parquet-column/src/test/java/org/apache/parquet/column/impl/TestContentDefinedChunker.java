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

import static org.apache.parquet.column.impl.ChunkingTestUtil.OPTIONAL_LIST_OF_INT64_PAGES;
import static org.apache.parquet.column.impl.ChunkingTestUtil.chunkSizes;
import static org.apache.parquet.column.impl.ChunkingTestUtil.lcg;
import static org.apache.parquet.column.impl.ChunkingTestUtil.newLongs;
import static org.apache.parquet.column.impl.ChunkingTestUtil.options;
import static org.apache.parquet.column.impl.ChunkingTestUtil.unboundedProps;
import static org.apache.parquet.column.impl.ChunkingTestUtil.valueCounts;
import static org.apache.parquet.column.impl.ChunkingTestUtil.writePages;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The expected page value counts of the {@code *MatchesArrowCpp} tests are those of the pages
 * pyarrow writes for the same {@link ChunkingTestUtil#lcg} values and chunking options.
 */
public class TestContentDefinedChunker {

  private static final int ROWS = 200_000;
  private static final CdcOptions OPTIONS = options(64 * 1024, 256 * 1024, 0);
  private static final List<Integer> REQUIRED_INT64 =
      Arrays.asList(17054, 17725, 12281, 14801, 17490, 15449, 19582, 14469, 22343, 16007, 10731, 20511, 1557);

  private final ContentDefinedChunker chunker = chunkerFor(options(0, 64, 0));

  @Test
  public void producesBoundariesWithinTheSizeEnvelope() {
    long[] values = newLongs(20_000, 1);
    ContentDefinedChunker chunker = chunkerFor(options(512, 4096, 0));
    List<Integer> sizes = chunkSizes(values.length, i -> {
      chunker.roll(values[i], 0, 0);
      return chunker.needNewChunk();
    });

    assertThat(sizes.subList(0, sizes.size() - 1))
        .allSatisfy(size -> assertThat(size * 8).isBetween(512, 4096))
        .filteredOn(size -> size * 8 < 4096)
        .as("chunks the hash ends")
        .hasSizeGreaterThan(sizes.size() / 2);
  }

  @Test
  public void oneByteBinaryHashesLikeBoolean() {
    ContentDefinedChunker viaBoolean = chunkerFor(options(0, 1024, 0));
    ContentDefinedChunker viaBinary = chunkerFor(options(0, 1024, 0));
    List<Boolean> fromBoolean = new ArrayList<>();
    List<Boolean> fromBinary = new ArrayList<>();
    for (long x : lcg(4000)) {
      boolean bit = (x & 0x100000000L) != 0;
      viaBoolean.roll(bit, 0, 0);
      fromBoolean.add(viaBoolean.needNewChunk());
      viaBinary.roll(Binary.fromConstantByteArray(new byte[] {(byte) (bit ? 1 : 0)}), 0, 0);
      fromBinary.add(viaBinary.needNewChunk());
    }

    assertThat(fromBinary).containsExactlyElementsOf(fromBoolean);
    assertThat(chunkSizes(fromBinary.size(), fromBinary::get))
        .filteredOn(size -> size < 1000)
        .as("chunks the hash ends")
        .hasSizeGreaterThan(2);
  }

  /** {@code MessageColumnIO} writes {@code writeNull(0, 0)} for a required field a record omits. */
  @Test
  public void nullOnRequiredColumnHashesNothing() {
    ContentDefinedChunker withNulls = chunkerFor(options(512, 4096, 0));
    ContentDefinedChunker withoutNulls = chunkerFor(options(512, 4096, 0));
    List<Boolean> actual = new ArrayList<>();
    List<Boolean> expected = new ArrayList<>();
    long[] values = newLongs(30_000, 17);
    for (int i = 0; i < values.length; ++i) {
      if (i % 100 == 0) {
        withNulls.rollNull(0, 0);
        actual.add(withNulls.needNewChunk());
        expected.add(false);
      }
      withNulls.roll(values[i], 0, 0);
      actual.add(withNulls.needNewChunk());
      withoutNulls.roll(values[i], 0, 0);
      expected.add(withoutNulls.needNewChunk());
    }

    assertThat(actual).containsExactlyElementsOf(expected);
    assertThat(chunkSizes(actual.size(), actual::get))
        .filteredOn(size -> size < 500)
        .as("chunks the hash ends")
        .hasSizeGreaterThan(10);
  }

  @Test
  public void chunksIntegers() {
    assertThat(chunkSizes(120, i -> {
          chunker.roll(i * 0x9E3779B1, 0, 0);
          return chunker.needNewChunk();
        }))
        .containsExactly(9, 9, 13, 16, 2, 9, 12, 11, 15, 11, 10, 3);
  }

  /** Quiet NaNs, which {@code Float.intBitsToFloat} keeps on every platform. */
  @Test
  public void floatsHashRawBitsIncludingNanPayloads() {
    assertThat(chunkSizes(120, i -> {
          chunker.roll(Float.intBitsToFloat(0x7FC00001 + i), 0, 0);
          return chunker.needNewChunk();
        }))
        .containsExactly(12, 11, 9, 16, 11, 12, 10, 12, 10, 14, 3);
  }

  @Test
  public void doublesHashRawBitsIncludingNanPayloads() {
    assertThat(chunkSizes(120, i -> {
          chunker.roll(Double.longBitsToDouble(0x7FF8000000000001L + i), 0, 0);
          return chunker.needNewChunk();
        }))
        .containsExactly(7, 8, 2, 8, 8, 1, 8, 8, 1, 8, 8, 8, 8, 1, 8, 1, 8, 8, 4, 7);
  }

  @Test
  public void requiredInt64MatchesArrowCpp() {
    assertThat(pageValueCounts("required int64 v;", ROWS, unboundedProps(OPTIONS)))
        .containsExactlyElementsOf(REQUIRED_INT64);
  }

  @Test
  public void optionalInt64MatchesArrowCpp() {
    assertThat(pageValueCounts("optional int64 v;", ROWS, unboundedProps(OPTIONS)))
        .containsExactly(
            13812, 13460, 11759, 11204, 11876, 20212, 10075, 19599, 17988, 10262, 11450, 15229, 11339,
            11377, 10358);
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void optionalBinaryMatchesArrowCpp(WriterVersion version) {
    assertThat(pageValueCounts("optional binary v (STRING);", ROWS, props(version)))
        .containsExactly(
            11588, 10905, 10978, 13364, 16800, 10173, 12896, 11080, 16760, 12559, 12333, 9652, 10732, 11297,
            12799, 12445, 3639);
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void optionalListOfInt64MatchesArrowCpp(WriterVersion version) {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message t { optional group v (LIST) { repeated group list { optional int64 element; } } }");
    long[] values = lcg(ROWS);
    List<Integer> counts = valueCounts(writePages(schema, props(version), ROWS, (writer, i) -> {
      long x = values[i];
      int n = (int) Long.remainderUnsigned(x, 4);
      if (Long.remainderUnsigned(x, 11) == 0) {
        writer.writeNull(0, 0);
      } else if (n == 0) {
        writer.writeNull(0, 1);
      }
      for (int k = 0; k < n && Long.remainderUnsigned(x, 11) != 0; k++) {
        writer.write(x >>> (16 * k), k == 0 ? 0 : 1, 3);
      }
    }));

    assertThat(counts).containsExactlyElementsOf(OPTIONAL_LIST_OF_INT64_PAGES);
  }

  /** A small maximum, which ends most chunks, pins that its cut leaves the run counter alone. */
  @Test
  public void maximumSizeCutsMatchArrowCpp() {
    assertThat(pageValueCounts("required int64 v;", 20_000, unboundedProps(options(0, 4096, -2))))
        .containsExactly(
            511, 512, 345, 512, 220, 512, 361, 512, 74, 512, 512, 199, 512, 382, 512, 512, 509, 512, 512,
            226, 512, 512, 2, 465, 512, 512, 349, 512, 460, 512, 98, 512, 512, 512, 132, 512, 512, 385, 508,
            512, 199, 512, 512, 455, 512, 415, 393);
  }

  /** The eight little-endian bytes of each value, wherever they lie, hash like the int64 values. */
  @Test
  public void binaryValuesHashOnlyTheirOwnBytes() {
    long[] values = lcg(ROWS);
    int pad = 3;
    int padded = pad + 8 + 5;
    ByteBuffer direct = ByteBuffer.allocateDirect(ROWS * padded);
    ByteBuffer heap = ((ByteBuffer) ByteBuffer.allocate(pad + ROWS * padded).position(pad)).slice();
    for (long value : values) {
      direct.put(padded(value, pad, padded));
      heap.put(padded(value, pad, padded));
    }

    assertThat(binaryPageValueCounts(i -> Binary.fromConstantByteArray(BytesUtils.longToBytes(values[i]))))
        .containsExactlyElementsOf(REQUIRED_INT64);
    assertThat(binaryPageValueCounts(i -> Binary.fromConstantByteArray(padded(values[i], pad, padded), pad, 8)))
        .containsExactlyElementsOf(REQUIRED_INT64);
    assertThat(binaryPageValueCounts(i -> Binary.fromConstantByteBuffer(direct, i * padded + pad, 8)))
        .containsExactlyElementsOf(REQUIRED_INT64);
    assertThat(binaryPageValueCounts(i -> Binary.fromConstantByteBuffer(heap, i * padded + pad, 8)))
        .containsExactlyElementsOf(REQUIRED_INT64);
  }

  /** Mostly ported from Arrow C++'s TestCDC.RollingHashMaskCalculation and ChunkSizeParameterValidation. */
  @Test
  public void maskMatchesTheReferenceForEachNormalizationLevel() {
    assertThat(ContentDefinedChunker.calculateMask(256 * 1024L, 1024 * 1024L, 0))
        .isEqualTo(0xFFFE000000000000L);
    assertThat(ContentDefinedChunker.calculateMask(256 * 1024L, 1024 * 1024L, 1))
        .isEqualTo(0xFFFC000000000000L);
    assertThat(ContentDefinedChunker.calculateMask(256 * 1024L, 1024 * 1024L, 2))
        .isEqualTo(0xFFF8000000000000L);
    assertThat(ContentDefinedChunker.calculateMask(256 * 1024L, 1024 * 1024L, 3))
        .isEqualTo(0xFFF0000000000000L);
    assertThat(ContentDefinedChunker.calculateMask(256 * 1024L, 1024 * 1024L, -1))
        .isEqualTo(0xFFFF000000000000L);
  }

  @Test
  public void maskMatchesTheReferenceAtTheEdgesOfItsRange() {
    assertThat(ContentDefinedChunker.calculateMask(0, 32, 0)).isEqualTo(0x8000000000000000L);
    assertThat(ContentDefinedChunker.calculateMask(0, 64, 0)).isEqualTo(0xC000000000000000L);
    assertThat(ContentDefinedChunker.calculateMask(0, 16, -1)).isEqualTo(0x8000000000000000L);
    // A zero target clamps to no bits rather than minus one.
    assertThat(ContentDefinedChunker.calculateMask(0, 8, -1)).isEqualTo(0x8000000000000000L);
    assertThat(ContentDefinedChunker.calculateMask(128, 384, -59)).isEqualTo(0xFFFFFFFFFFFFFFFEL);
  }

  @Test
  public void rejectsNegativeMinimum() {
    assertThatThrownBy(() -> ContentDefinedChunker.calculateMask(-1, 1024, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking minimum chunk size (negative): -1");
  }

  @Test
  public void rejectsRangeThatIsNotAscending() {
    assertThatThrownBy(() -> ContentDefinedChunker.calculateMask(1024, 512, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking size range: maximum chunk size (512) must be greater "
            + "than minimum chunk size (1024)");
    assertThatThrownBy(() -> ContentDefinedChunker.calculateMask(32, 32, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking size range: maximum chunk size (32) must be greater "
            + "than minimum chunk size (32)");
  }

  /** The mask needs at least one bit, so the size range must be at least 32, 64 or 128 for level 0, 1 or 2. */
  @ParameterizedTest
  @CsvSource({"0, 16, 0", "32, 48, 0", "1, 33, 1", "0, 123, 2", "128, 384, -60"})
  public void rejectsSizeRangeTooNarrowForTheNormalizationLevel(long min, long max, int normLevel) {
    assertThatThrownBy(() -> ContentDefinedChunker.calculateMask(min, max, normLevel))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("The content defined chunking mask must be between 1 and 63 bits");
  }

  @ParameterizedTest
  @CsvSource({"0, 32, 0", "32, 64, 0", "1, 65, 1", "0, 128, 2", "1073741824, 2147483648, 0"})
  public void acceptsSizeRangeWideEnoughForTheNormalizationLevel(long min, long max, int normLevel) {
    assertThatCode(() -> ContentDefinedChunker.calculateMask(min, max, normLevel))
        .doesNotThrowAnyException();
  }

  /** A deliberate deviation: Arrow C++ and arrow-rs overflow here. */
  @Test
  public void handlesEnvelopeThatWouldOverflowTheAverage() {
    assertThat(ContentDefinedChunker.calculateMask(256 * 1024L, Long.MAX_VALUE, 0))
        .isEqualTo(0xFFFFFFFFFFFFFFC0L);
  }

  private static ContentDefinedChunker chunkerFor(CdcOptions options) {
    return new ContentDefinedChunker(
        options,
        new ColumnDescriptor(new String[] {"v"}, Types.required(INT64).named("v"), 0, 0));
  }

  private static ParquetProperties props(WriterVersion version) {
    return ParquetProperties.copy(unboundedProps(OPTIONS))
        .withWriterVersion(version)
        .build();
  }

  /** Every eighth value of an optional column is null, and a binary column holds strings. */
  private static List<Integer> pageValueCounts(String field, int rows, ParquetProperties props) {
    MessageType schema = MessageTypeParser.parseMessageType("message t { " + field + " }");
    ColumnDescriptor path = schema.getColumns().get(0);
    int maxDef = path.getMaxDefinitionLevel();
    boolean binary = path.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY;
    long[] values = lcg(rows);
    return valueCounts(writePages(schema, props, rows, (writer, i) -> {
      long x = values[i];
      if (maxDef > 0 && Long.remainderUnsigned(x, 8) == 0) {
        writer.writeNull(0, maxDef - 1);
      } else if (binary) {
        writer.write(Binary.fromString("row-" + Long.remainderUnsigned(x, 1_000_000L)), 0, maxDef);
      } else {
        writer.write(x, 0, maxDef);
      }
    }));
  }

  private static List<Integer> binaryPageValueCounts(IntFunction<Binary> value) {
    MessageType schema = MessageTypeParser.parseMessageType("message t { required binary v; }");
    return valueCounts(
        writePages(schema, unboundedProps(OPTIONS), ROWS, (writer, i) -> writer.write(value.apply(i), 0, 0)));
  }

  /** The value's little-endian bytes at {@code pad} in {@code length} bytes of filler. */
  private static byte[] padded(long value, int pad, int length) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) 0xA5);
    System.arraycopy(BytesUtils.longToBytes(value), 0, bytes, pad, 8);
    return bytes;
  }
}
