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
import static org.apache.parquet.column.impl.ChunkingTestSupport.lcg;
import static org.apache.parquet.column.impl.ChunkingTestSupport.newLongs;
import static org.apache.parquet.column.impl.ChunkingTestSupport.options;
import static org.apache.parquet.column.impl.ChunkingTestSupport.sharedPrefix;
import static org.apache.parquet.column.impl.ChunkingTestSupport.sharedSuffix;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

/**
 * Cut behaviour of the chunker itself; {@code TestRollingHashMask} pins the mask. The expected
 * chunk sizes are Arrow C++'s, not this implementation's: pyarrow writes the same values with the
 * same 0/64-byte envelope to pages of these sizes.
 */
public class TestCdcChunker {

  /** A tiny envelope, so that few values make many chunks. JUnit gives each test a fresh one. */
  private final CdcChunker chunker = chunkerFor(options(0, 64, 0));

  @Test
  public void producesBoundariesWithinTheSizeEnvelope() {
    long min = 512;
    long max = 4096;
    List<Integer> sizes = chunkSizesOverLongs(newLongs(20_000, 1), min, max);

    // Every chunk but the last is a completed one, of eight-byte values.
    assertThat(sizes).hasSizeGreaterThan(1);
    assertThat(sizes.subList(0, sizes.size() - 1))
        .allSatisfy(size -> assertThat((long) size * 8).isBetween(min, max));
  }

  @Test
  public void anEditPerturbsOnlyTheChunksAroundIt() {
    long[] original = newLongs(50_000, 7);
    List<Integer> before = chunkSizesOverLongs(original, 512, 4096);
    List<Integer> after = chunkSizesOverLongs(insert(original, 20_000, newLongs(500, 99)), 512, 4096);

    int prefix = sharedPrefix(before, after);
    assertThat(prefix).as("chunks shared before the edit").isPositive();
    assertThat(sharedSuffix(before, after, prefix))
        .as("nearly every chunk after the edit realigns")
        .isGreaterThan((before.size() - prefix) * 9 / 10);
  }

  @Test
  public void aOneByteBinaryHashesTheSameAsAOneByteFixedWidthValue() {
    CdcChunker viaBoolean = chunkerFor(options(0, 1024, 0));
    CdcChunker viaBinary = chunkerFor(options(0, 1024, 0));
    Binary one = Binary.fromConstantByteArray(new byte[] {1});
    Binary zero = Binary.fromConstantByteArray(new byte[] {0});
    List<Boolean> fromBoolean = new ArrayList<>();
    List<Boolean> fromBinary = new ArrayList<>();
    // Varying bytes: a constant byte stream drives the gear hash to a fixed point that never matches.
    for (long x : lcg(4000)) {
      boolean bit = (x & 0x100000000L) != 0;
      fromBoolean.add(viaBoolean.offer(bit, 0, 0));
      fromBinary.add(viaBinary.offer(bit ? one : zero, 0, 0));
    }
    assertThat(fromBinary).containsExactlyElementsOf(fromBoolean);
    assertThat(fromBinary).contains(true);
  }

  /** The minimum is a multiple of eight, so both skip the hash up to the same value. */
  @Test
  public void anEightByteBinaryHashesTheSameAsALong() {
    long[] values = newLongs(20_000, 19);
    CdcChunker viaBinary = chunkerFor(options(512, 4096, 0));
    assertThat(chunkSizes(
            values.length,
            i -> viaBinary.offer(
                Binary.fromConstantByteArray(ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(values[i])
                    .array()),
                0,
                0)))
        .containsExactlyElementsOf(chunkSizesOverLongs(values, 512, 4096));
  }

  /**
   * {@code MessageColumnIO} writes {@code writeNull(0, 0)} for a required field a record omits,
   * where {@code definitionLevel == maxDef}; there is still no value to hash. The nulls are spread
   * out because a gear hash forgets old bytes, so leading ones would perturb almost nothing.
   */
  @Test
  public void aNullOnARequiredColumnContributesNothingToTheHash() {
    CdcChunker withNulls = chunkerFor(options(512, 4096, 0));
    CdcChunker withoutNulls = chunkerFor(options(512, 4096, 0));
    List<Boolean> actual = new ArrayList<>();
    List<Boolean> expected = new ArrayList<>();
    long[] values = newLongs(30_000, 17);
    for (int i = 0; i < values.length; ++i) {
      if (i % 100 == 0) {
        actual.add(withNulls.offerNull(0, 0));
        expected.add(false);
      }
      actual.add(withNulls.offer(values[i], 0, 0));
      expected.add(withoutNulls.offer(values[i], 0, 0));
    }
    assertThat(actual).containsExactlyElementsOf(expected);
    assertThat(actual).contains(true);
  }

  /** The 4-byte dispatch, which the golden vectors do not reach. */
  @Test
  public void chunksIntegers() {
    assertThat(chunkSizes(120, i -> chunker.offer(i * 0x9E3779B1, 0, 0)))
        .containsExactly(9, 9, 13, 16, 2, 9, 12, 11, 15, 11, 10, 3);
  }

  /**
   * NaNs differing only in payload must hash differently, which {@code Float.floatToIntBits} would
   * prevent. The payloads are quiet NaNs, because {@code Float.intBitsToFloat} may quieten a
   * signalling one depending on the platform.
   */
  @Test
  public void floatsHashTheirRawBitsIncludingNanPayloads() {
    assertThat(chunkSizes(120, i -> chunker.offer(Float.intBitsToFloat(0x7FC00001 + i), 0, 0)))
        .containsExactly(12, 11, 9, 16, 11, 12, 10, 12, 10, 14, 3);
  }

  /** As above, for doubles and {@code Double.doubleToLongBits}. */
  @Test
  public void doublesHashTheirRawBitsIncludingNanPayloads() {
    assertThat(chunkSizes(120, i -> chunker.offer(Double.longBitsToDouble(0x7FF8000000000001L + i), 0, 0)))
        .containsExactly(7, 8, 2, 8, 8, 1, 8, 8, 1, 8, 8, 8, 8, 1, 8, 1, 8, 8, 4, 7);
  }

  private static List<Integer> chunkSizesOverLongs(long[] values, long min, long max) {
    CdcChunker chunker = chunkerFor(options(min, max, 0));
    return chunkSizes(values.length, i -> chunker.offer(values[i], 0, 0));
  }

  /** A chunker for a flat required column. */
  private static CdcChunker chunkerFor(CdcOptions options) {
    return new CdcChunker(
        options,
        new ColumnDescriptor(new String[] {"v"}, Types.required(INT64).named("v"), 0, 0));
  }
}
