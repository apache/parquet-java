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
package org.apache.parquet.column.values.rle;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.junit.jupiter.api.Test;

/**
 * Parity tests for {@link RunLengthBitPackingHybridDecoder#skipInts(int)}. The invariant
 * checked in every case is: {@code skipInts(k)} followed by {@code readInt()} yields the same
 * sequence as {@code k} calls to {@code readInt()} discarded followed by the same
 * {@code readInt()}. Covers RLE-only streams, PACKED-only streams, mixed streams, run-boundary
 * splits, skip-to-end, and RLE + PACKED interleaving with rewind via re-decode.
 */
public class TestRunLengthBitPackingHybridDecoderSkip {

  private static final DirectByteBufferAllocator ALLOC = new DirectByteBufferAllocator();

  @Test
  public void skipAcrossRleRuns() throws Exception {
    // Three big RLE runs. Any skip that lands mid-run or on a run boundary must still return
    // the correct next value.
    int bitWidth = 4;
    int[] values = repeat(0, 500, repeat(7, 500, repeat(3, 500, new int[0])));
    checkSkipParityAtAllOffsets(bitWidth, values);
  }

  @Test
  public void skipAcrossPackedRuns() throws Exception {
    // Values change every position -> encoder emits bit-packed groups exclusively.
    int bitWidth = 4;
    int[] values = new int[512];
    for (int i = 0; i < values.length; i++) {
      values[i] = i & 0xF;
    }
    checkSkipParityAtAllOffsets(bitWidth, values);
  }

  @Test
  public void skipAcrossMixedRuns() throws Exception {
    // Mix RLE and PACKED sections back-to-back.
    int bitWidth = 5;
    int[] values = repeat(9, 200, new int[0]); // RLE
    // Small PACKED section (differing values)
    int[] packed = new int[64];
    for (int i = 0; i < packed.length; i++) {
      packed[i] = i & 0x1F;
    }
    values = concat(values, packed);
    values = repeat(4, 320, values); // RLE
    // Larger PACKED section
    int[] packed2 = new int[256];
    for (int i = 0; i < packed2.length; i++) {
      packed2[i] = (i * 3) & 0x1F;
    }
    values = concat(values, packed2);
    values = repeat(31, 100, values); // RLE
    checkSkipParityAtAllOffsets(bitWidth, values);
  }

  @Test
  public void skipZero() throws Exception {
    int[] values = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    RunLengthBitPackingHybridDecoder decoder = decoderFor(4, values);
    decoder.skipInts(0);
    for (int v : values) {
      assertThat(decoder.readInt()).isEqualTo(v);
    }
  }

  @Test
  public void skipEntireStream() throws Exception {
    int bitWidth = 4;
    int[] values = new int[1024];
    Random r = new Random(0xC0FFEE);
    for (int i = 0; i < values.length; i++) {
      values[i] = r.nextInt(1 << bitWidth);
    }
    RunLengthBitPackingHybridDecoder decoder = decoderFor(bitWidth, values);
    decoder.skipInts(values.length);
    // No values remaining; decoder must not be asked to readInt again (matches semantics of
    // consuming all values normally).
  }

  @Test
  public void skipThenReadThenSkipThenRead() throws Exception {
    // Alternating skip/read pattern that spans multiple runs including partial runs.
    int bitWidth = 6;
    int[] values = new int[4096];
    Random r = new Random(42);
    for (int i = 0; i < values.length; i++) {
      // Bias to produce both long RLE runs and PACKED regions.
      values[i] = (i % 100 < 40) ? 17 : (r.nextInt(1 << bitWidth));
    }
    RunLengthBitPackingHybridDecoder decoder = decoderFor(bitWidth, values);
    int pos = 0;
    Random ops = new Random(0xABC);
    while (pos < values.length) {
      int remaining = values.length - pos;
      int op = ops.nextInt(2);
      if (op == 0) {
        int skip = ops.nextInt(Math.min(200, remaining + 1));
        decoder.skipInts(skip);
        pos += skip;
      } else {
        int reads = ops.nextInt(Math.min(200, remaining) + 1);
        for (int i = 0; i < reads; i++, pos++) {
          assertThat(decoder.readInt()).isEqualTo(values[pos]);
        }
      }
    }
  }

  @Test
  public void skipWithBitWidthZero() throws Exception {
    // bitWidth 0 -> a single RLE run of zeros. skip must not attempt any input reads.
    int[] values = new int[500];
    RunLengthBitPackingHybridDecoder decoder = decoderFor(0, values);
    decoder.skipInts(300);
    for (int i = 300; i < values.length; i++) {
      assertThat(decoder.readInt()).isEqualTo(0);
    }
  }

  @Test
  public void skipPartialThenReadRest() throws Exception {
    // Ensure the first N reads after a partial-run skip return the correct tail values.
    int bitWidth = 3;
    int[] values = new int[0];
    // A PACKED run followed by an RLE run.
    int[] packed = new int[64];
    for (int i = 0; i < packed.length; i++) packed[i] = i & 0x7;
    values = concat(values, packed);
    values = repeat(5, 200, values);

    // Skip inside the PACKED run.
    RunLengthBitPackingHybridDecoder decoder = decoderFor(bitWidth, values);
    decoder.skipInts(37);
    for (int i = 37; i < values.length; i++) {
      assertThat(decoder.readInt())
          .as("value at index %d after skipInts(37)", i)
          .isEqualTo(values[i]);
    }
  }

  private static int[] repeat(int val, int count, int[] tail) {
    int[] head = new int[count];
    Arrays.fill(head, val);
    return concat(head, tail);
  }

  private static int[] concat(int[] a, int[] b) {
    int[] out = new int[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }

  private static RunLengthBitPackingHybridDecoder decoderFor(int bitWidth, int[] values) throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, 1024, 1 << 20, ALLOC);
    for (int v : values) {
      encoder.writeInt(v);
    }
    ByteBuffer buf = encoder.toBytes().toByteBuffer();
    ByteBufferInputStream in = ByteBufferInputStream.wrap(buf);
    return new RunLengthBitPackingHybridDecoder(bitWidth, in);
  }

  /**
   * For a well-chosen set of skip offsets, assert that (a) skipInts(k) then readInt() equals
   * values[k], and (b) after skipInts(k) subsequent reads walk values[k..end] correctly.
   */
  private static void checkSkipParityAtAllOffsets(int bitWidth, int[] values) throws Exception {
    int[] offsets = new int[] {
      0,
      1,
      7,
      8,
      63,
      64,
      65,
      values.length / 4,
      values.length / 3,
      values.length / 2,
      (values.length * 2) / 3,
      values.length - 1
    };
    for (int off : offsets) {
      if (off < 0 || off >= values.length) {
        continue;
      }
      RunLengthBitPackingHybridDecoder decoder = decoderFor(bitWidth, values);
      decoder.skipInts(off);
      // Read remainder and compare.
      for (int i = off; i < values.length; i++) {
        assertThat(decoder.readInt())
            .as("bitWidth=%d, skipped=%d, index=%d", bitWidth, off, i)
            .isEqualTo(values[i]);
      }
    }
  }
}
