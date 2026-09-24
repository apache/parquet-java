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

import static org.apache.parquet.column.impl.ChunkingTestSupport.lcg;
import static org.apache.parquet.column.impl.ChunkingTestSupport.options;
import static org.apache.parquet.column.impl.ChunkingTestSupport.unboundedProps;
import static org.apache.parquet.column.impl.ChunkingTestSupport.valueCounts;
import static org.apache.parquet.column.impl.ChunkingTestSupport.writePages;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

/**
 * Pins the chunker's page boundaries to the ones Arrow C++ produces for the same data.
 *
 * <p>The rest of the suite checks properties any content-derived hash would satisfy -- an edit
 * stays local, chunks respect the size envelope -- so it passes just as happily against a chunker
 * that is self-consistent and wrong. These vectors are the part that does not.
 *
 * <p>The expected values were produced by Arrow C++ itself, through pyarrow's
 * {@code use_content_defined_chunking}, and read back off the resulting files. Touching
 * {@link CdcChunker} or {@link GearHashTable} must not change a single number here.
 */
public class TestCdcChunkerGoldenBoundaries {

  private static final int ROWS = 200_000;

  private static final CdcOptions OPTIONS = options(64 * 1024, 256 * 1024, 0);
  private static final CdcOptions TIGHT_OPTIONS = options(0, 4096, -2);

  // A binary value's eight bytes sit at PAD within PADDED, with filler either side.
  private static final int PAD = 3;
  private static final int PADDED = PAD + 8 + 5;

  /** No levels are hashed at all: {@code maxDef == maxRep == 0}. */
  @Test
  public void requiredInt64MatchesArrowCpp() {
    assertThat(pageValueCounts("message t { required int64 v; }"))
        .containsExactly(
            17054, 17725, 12281, 14801, 17490, 15449, 19582, 14469, 22343, 16007, 10731, 20511, 1557);
  }

  /** Definition levels are hashed, and the value only where one is present. */
  @Test
  public void optionalInt64MatchesArrowCpp() {
    assertThat(pageValueCounts("message t { optional int64 v; }"))
        .containsExactly(
            13812, 13460, 11759, 11204, 11876, 20212, 10075, 19599, 17988, 10262, 11450, 15229, 11339,
            11377, 10358);
  }

  /** The same level path, reaching the chunker's element-bytes dispatch instead of fixed width. */
  @Test
  public void optionalBinaryMatchesArrowCpp() {
    assertThat(pageValueCounts("message t { optional binary v (STRING); }"))
        .containsExactly(
            11588, 10905, 10978, 13364, 16800, 10173, 12896, 11080, 16760, 12559, 12333, 9652, 10732, 11297,
            12799, 12445, 3639);
  }

  /**
   * The binary path, fed arbitrary bytes rather than text. A {@code required binary} column holding
   * each value's eight little-endian bytes presents the chunker the byte stream the
   * {@code required int64} case does, so it must reproduce Arrow C++'s vector for that case. Unlike
   * the text case it feeds bytes above 0x7F, which a sign-extension slip in the table index would
   * hash differently.
   *
   * <p>A {@link Binary} is often a window onto a larger buffer, and only the window belongs to the
   * value, so each case but the first surrounds it with filler that moves every boundary if hashed.
   */
  @Test
  public void binaryValuesHashOnlyTheirOwnBytes() {
    List<Integer> expected = pageValueCounts("message t { required int64 v; }");
    long[] values = lcg(ROWS);
    ByteBuffer direct = ByteBuffer.allocateDirect(ROWS * PADDED);
    for (long value : values) {
      direct.put(padded(value));
    }

    assertThat(binaryPageValueCounts(i -> Binary.fromConstantByteArray(BytesUtils.longToBytes(values[i]))))
        .as("a whole byte array")
        .containsExactlyElementsOf(expected);
    assertThat(binaryPageValueCounts(i -> Binary.fromConstantByteArray(padded(values[i]), PAD, 8)))
        .as("a slice of a byte array")
        .containsExactlyElementsOf(expected);
    assertThat(binaryPageValueCounts(
            i -> Binary.fromConstantByteBuffer(ByteBuffer.wrap(padded(values[i]), PAD, 8))))
        .as("a window onto a heap buffer")
        .containsExactlyElementsOf(expected);
    assertThat(binaryPageValueCounts(i -> Binary.fromConstantByteBuffer(direct, i * PADDED + PAD, 8)))
        .as("a window onto a direct buffer")
        .containsExactlyElementsOf(expected);
  }

  /**
   * An envelope whose mask is selective enough that the rolling hash usually fails to complete a
   * run before {@code maxChunkSize}, so the hard cut decides most boundaries -- the 512-value
   * chunks below are exactly 4096 bytes of int64.
   *
   * <p>The other cases never reach that branch: with a wide envelope the eight-table normalisation
   * makes hitting the cap a thin tail. It is also the branch the references handle
   * unintuitively -- the hard cut zeroes the size counter but deliberately leaves the run counter
   * alone -- so without this case, resetting the run counter there changes nothing any golden
   * vector can see.
   */
  @Test
  public void aMaximumSizeDominatedEnvelopeMatchesArrowCpp() {
    assertThat(pageValueCounts("message t { required int64 v; }", 20_000, TIGHT_OPTIONS))
        .containsExactly(
            511, 512, 345, 512, 220, 512, 361, 512, 74, 512, 512, 199, 512, 382, 512, 512, 509, 512, 512,
            226, 512, 512, 2, 465, 512, 512, 349, 512, 460, 512, 98, 512, 512, 512, 132, 512, 512, 385, 508,
            512, 199, 512, 512, 455, 512, 415, 393);
  }

  /**
   * The nested path: both levels hashed, and a cut allowed only where a record starts.
   *
   * <p>This is the only case that hashes a repetition level at all, so it is the only one that can
   * tell the two levels apart -- swap the order they are fed to the chunker in and the flat cases
   * above do not move.
   */
  @Test
  public void optionalListOfInt64MatchesArrowCpp() {
    assertThat(nestedPageValueCounts())
        .containsExactly(
            13336, 9928, 13972, 11483, 10503, 11533, 15500, 9599, 14956, 11109, 10846, 14956, 13858, 15861,
            13510, 16720, 11116, 10562, 9536, 11052, 12880, 12392, 10121, 14988, 13928, 10761, 11464);
  }

  /**
   * Writes {@code optional group v (LIST) { repeated group list { optional int64 element } }}, the
   * three-level encoding pyarrow produces for {@code list<int64>}: a null list is one slot at
   * definition level 0, an empty list one slot at level 1, and each element of a present list a
   * slot at level 3, the first of them starting the record.
   */
  private static List<Integer> nestedPageValueCounts() {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message t { optional group v (LIST) { repeated group list { optional int64 element; } } }");
    long[] values = lcg(ROWS);
    return valueCounts(writePages(schema, unboundedProps(OPTIONS, true), ROWS, (writer, i) -> {
      long x = values[i];
      if (Long.remainderUnsigned(x, 11) == 0) {
        writer.writeNull(0, 0); // the list itself is null
      } else {
        int n = (int) Long.remainderUnsigned(x, 4);
        if (n == 0) {
          writer.writeNull(0, 1); // an empty list
        } else {
          for (int k = 0; k < n; k++) {
            writer.write(x >>> (16 * k), k == 0 ? 0 : 1, 3);
          }
        }
      }
    }));
  }

  /**
   * Writes one column of the shared generated data through a real
   * {@link org.apache.parquet.column.ColumnWriteStore} and returns the value count of every page.
   *
   * <p>The position-based limits are lifted so that every boundary below is the chunker's. Arrow's
   * own equivalents are lifted the same way on the reference side.
   */
  private static List<Integer> pageValueCounts(String schemaText) {
    return pageValueCounts(schemaText, ROWS, OPTIONS);
  }

  private static List<Integer> pageValueCounts(String schemaText, int rows, CdcOptions options) {
    MessageType schema = MessageTypeParser.parseMessageType(schemaText);
    ColumnDescriptor path = schema.getColumns().get(0);
    int maxDef = path.getMaxDefinitionLevel();
    boolean binary = path.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY;
    long[] values = lcg(rows);
    return valueCounts(writePages(schema, unboundedProps(options, true), rows, (writer, i) -> {
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

  /** The pages of a {@code required binary} column holding {@code value.apply(i)} in row {@code i}. */
  private static List<Integer> binaryPageValueCounts(IntFunction<Binary> value) {
    MessageType schema = MessageTypeParser.parseMessageType("message t { required binary v; }");
    return valueCounts(writePages(
        schema, unboundedProps(OPTIONS, true), ROWS, (writer, i) -> writer.write(value.apply(i), 0, 0)));
  }

  private static byte[] padded(long value) {
    byte[] bytes = new byte[PADDED];
    Arrays.fill(bytes, (byte) 0xA5);
    System.arraycopy(BytesUtils.longToBytes(value), 0, bytes, PAD, 8);
    return bytes;
  }
}
