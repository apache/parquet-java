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
import static org.apache.parquet.column.impl.ChunkingTestSupport.lcg;
import static org.apache.parquet.column.impl.ChunkingTestSupport.lcgFrom;
import static org.apache.parquet.column.impl.ChunkingTestSupport.newLongs;
import static org.apache.parquet.column.impl.ChunkingTestSupport.options;
import static org.apache.parquet.column.impl.ChunkingTestSupport.sharedPrefix;
import static org.apache.parquet.column.impl.ChunkingTestSupport.sharedSuffix;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

/**
 * Cut behaviour of the chunker itself. The mask it is built from is pinned separately, against
 * Arrow C++'s own vectors, by {@code TestRollingHashMask}.
 *
 * <p>The expected chunk sizes are not recorded from this implementation. They come from a
 * transcription of Arrow C++'s {@code chunker_internal.cc}, so they fail if this port drifts from
 * the reference rather than merely if its own output changes.
 */
public class TestCdcChunker {

  /**
   * The envelope every dispatch case below drives: a chunk of 8 eight-byte values is a
   * maximum-size cut, and anything shorter is the rolling hash completing a run. JUnit builds a
   * test instance per method, so each gets a fresh one.
   */
  private final CdcChunker chunker = chunkerFor(options(0, 64, 0));

  @Test
  public void producesBoundariesWithinTheSizeEnvelope() {
    long min = 512;
    long max = 4096;
    List<Integer> sizes = chunkSizesOverLongs(newLongs(20_000, 1), min, max);

    // Every chunk but the last is a completed one, so it must respect both ends of the envelope.
    // Eight bytes per value, and the boundary lands before the triggering value.
    assertThat(sizes).hasSizeGreaterThan(1);
    assertThat(sizes.subList(0, sizes.size() - 1))
        .allSatisfy(size -> assertThat((long) size * 8).isBetween(min, max));
  }

  @Test
  public void localizesAnEditToTheChunksAroundIt() {
    Perturbation edit = perturbationFromInserting(50_000, 20_000, 500);

    // The chunks before the edit are untouched and the ones after it realign, so the two chunk
    // sequences share a prefix and a suffix. A position-based writer would share no suffix at all.
    assertThat(edit.prefix).as("chunks shared before the edit").isPositive();
    assertThat(edit.suffix).as("chunks shared after the edit").isPositive();
    assertThat(edit.suffix)
        .as("nearly every chunk after the edit realigns")
        .isGreaterThan((edit.originalChunks - edit.prefix) * 9 / 10);
  }

  @Test
  public void theEditsBlastRadiusDoesNotGrowWithTheFile() {
    // The distinguishing property: for a position-based writer every chunk after the edit shifts,
    // so doubling the file doubles the damage. Here it stays put.
    Perturbation small = perturbationFromInserting(50_000, 20_000, 500);
    Perturbation large = perturbationFromInserting(100_000, 20_000, 500);

    assertThat(large.originalChunks).isGreaterThan(small.originalChunks * 3 / 2);
    assertThat(large.perturbed()).isEqualTo(small.perturbed());
  }

  @Test
  public void aOneByteBinaryHashesTheSameAsAOneByteFixedWidthValue() {
    // The bytes must vary: a constant one-byte stream drives the gear hash to a fixed point that
    // never matches, so every byte value would produce the same maximum-size-only cuts.
    CdcChunker viaBoolean = chunkerFor(options(0, 1024, 0));
    CdcChunker viaBinary = chunkerFor(options(0, 1024, 0));
    Binary one = Binary.fromConstantByteArray(new byte[] {1});
    Binary zero = Binary.fromConstantByteArray(new byte[] {0});
    List<Boolean> fromBoolean = new ArrayList<>();
    List<Boolean> fromBinary = new ArrayList<>();
    long[] values = lcg(4000);
    for (long x : values) {
      boolean bit = (x & 0x100000000L) != 0;
      fromBoolean.add(viaBoolean.offer(bit, 0, 0));
      fromBinary.add(viaBinary.offer(bit ? one : zero, 0, 0));
    }
    assertThat(fromBinary).containsExactlyElementsOf(fromBoolean);
    assertThat(fromBinary).contains(true);
  }

  // ------------------------------------------- the run state machine

  /**
   * Pins the interleaving of the two cut conditions.
   *
   * <p>With a 64-byte maximum and eight-byte values, a chunk of 8 is a maximum-size cut and
   * anything shorter is the rolling hash completing a run of eight matches. The mixture below is
   * what makes this sensitive to the run counter's bookkeeping: a maximum-size cut must <i>not</i>
   * reset it, so a run that is part way through carries across the cut, and the shorter chunks land
   * where they do. Reset it and the short chunks move.
   */
  @Test
  public void interleavesHashAndMaximumSizeCuts() {
    long[] values = lcg(120);
    assertThat(chunkSizes(values.length, i -> chunker.offer(values[i], 0, 0)))
        .containsExactly(7, 2, 8, 3, 8, 8, 8, 2, 8, 1, 8, 8, 2, 8, 1, 8, 2, 8, 1, 8, 8, 1, 2);
  }

  // ------------------------------------------------ value dispatch

  /** The 4-byte dispatch, which the golden vectors do not reach. */
  @Test
  public void chunksIntegers() {
    assertThat(chunkSizes(120, i -> chunker.offer(i * 0x9E3779B1, 0, 0)))
        .containsExactly(9, 9, 13, 16, 2, 9, 12, 11, 15, 11, 10, 3);
  }

  /** The 8-byte floating point dispatch, likewise unreached elsewhere. */
  @Test
  public void chunksDoubles() {
    long[] values = lcgFrom(99, 120);
    assertThat(chunkSizes(values.length, i -> chunker.offer(Double.longBitsToDouble(values[i]), 0, 0)))
        .containsExactly(7, 3, 8, 8, 1, 8, 8, 8, 8, 2, 8, 2, 8, 8, 8, 8, 1, 8, 1, 7);
  }

  /**
   * Floats are hashed as their raw bits, so two NaNs differing only in payload are two different
   * values to the chunker. {@code Float.floatToIntBits} would collapse all 120 to one canonical NaN
   * and leave a constant byte stream.
   *
   * <p>The payloads are quiet NaNs: {@code Float.intBitsToFloat} is allowed to quieten a signalling
   * pattern, and which patterns signal is platform dependent, so a signalling range would make this
   * vector architecture dependent.
   */
  @Test
  public void floatsHashTheirRawBitsIncludingNanPayloads() {
    assertThat(chunkSizes(120, i -> chunker.offer(Float.intBitsToFloat(0x7FC00001 + i), 0, 0)))
        .containsExactly(12, 11, 9, 16, 11, 12, 10, 12, 10, 14, 3);
  }

  /** As above for doubles, where the collapsing method is {@code Double.doubleToLongBits}. */
  @Test
  public void doublesHashTheirRawBitsIncludingNanPayloads() {
    assertThat(chunkSizes(120, i -> chunker.offer(Double.longBitsToDouble(0x7FF8000000000001L + i), 0, 0)))
        .containsExactly(7, 8, 2, 8, 8, 1, 8, 8, 1, 8, 8, 8, 8, 1, 8, 1, 8, 8, 4, 7);
  }

  // ----------------------------------------------------------- helpers

  /**
   * Value counts per page, counted the way {@code ChunkingColumnWriter} does: a boundary closes the
   * page before the triplet that triggered it, and whatever is left over is the last page.
   */
  private static List<Integer> chunkSizes(int count, IntPredicate offer) {
    List<Integer> sizes = new ArrayList<>();
    int current = 0;
    for (int i = 0; i < count; ++i) {
      if (offer.test(i) && current > 0) {
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

  /**
   * Runs a flat, required column of longs through the chunker the way {@code ChunkingColumnWriter}
   * will: roll the value, then ask for a boundary. Returns the number of values in each chunk.
   */
  private static List<Integer> chunkSizesOverLongs(long[] values, long min, long max) {
    CdcChunker chunker = chunkerFor(options(min, max, 0));
    return chunkSizes(values.length, i -> chunker.offer(values[i], 0, 0));
  }

  /** A chunker for a flat required column, which is what every case here drives. */
  private static CdcChunker chunkerFor(CdcOptions options) {
    return new CdcChunker(
        options,
        new ColumnDescriptor(new String[] {"v"}, Types.required(INT64).named("v"), 0, 0));
  }

  /** The shared prefix and suffix of the chunk sequences before and after an edit. */
  private static final class Perturbation {
    final int originalChunks;
    final int prefix;
    final int suffix;

    Perturbation(int originalChunks, int prefix, int suffix) {
      this.originalChunks = originalChunks;
      this.prefix = prefix;
      this.suffix = suffix;
    }

    int perturbed() {
      return originalChunks - prefix - suffix;
    }
  }

  private static Perturbation perturbationFromInserting(int count, int at, int insertedCount) {
    long[] original = newLongs(count, 7);
    long[] edited = insert(original, at, newLongs(insertedCount, 99));

    List<Integer> before = chunkSizesOverLongs(original, 512, 4096);
    List<Integer> after = chunkSizesOverLongs(edited, 512, 4096);

    int prefix = sharedPrefix(before, after);
    return new Perturbation(before.size(), prefix, sharedSuffix(before, after, prefix));
  }
}
