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
package org.apache.parquet.column.values.alp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.junit.jupiter.api.Test;

/**
 * Pins the exact bytes the writer produces. Round-trip tests pass for any valid encoding, so they
 * miss changes that still decode, such as different exceptions, placeholders or preset order.
 */
public class AlpPageBytesTest {

  @Test
  public void testDoublePage() throws Exception {
    assertThat(doublePage(1.5, 2.25, 3.0))
        .containsExactly(bytes(
            "00 00 03 03000000", // header: mode, integer encoding, log vector size 3, 3 values
            "04000000", // offset array
            "12 10 0000", // AlpInfo: exponent 18, factor 16, no exceptions
            "9600000000000000 08", // ForInfo: frame of reference 150, bit width 8
            "00 4b 96")); // deltas 0, 75, 150
  }

  @Test
  public void testDoublePageWithException() throws Exception {
    assertThat(doublePage(0.5, Double.NaN, 1.25))
        .containsExactly(bytes(
            "00 00 03 03000000",
            "04000000",
            "12 10 0100", // one exception
            "3200000000000000 07", // frame of reference 50, bit width 7
            "00 c0 12", // deltas 0, 0 (the NaN's slot holds the first encoded value), 75
            "0100", // exception position 1
            "000000000000f87f")); // NaN
  }

  @Test
  public void testFloatPage() throws Exception {
    assertThat(floatPage(1.5f, 2.25f, 3.0f))
        .containsExactly(bytes(
            "00 00 03 03000000",
            "04000000",
            "08 06 0000", // exponent 8: at 9, 2.25 and 3.0 would overflow an int
            "96000000 08", // 4-byte frame of reference
            "00 4b 96"));
  }

  @Test
  public void testPageWithTwoVectors() throws Exception {
    assertThat(doublePage(1, 2, 3, 4, 5, 6, 7, 8, 9))
        .containsExactly(bytes(
            "00 00 03 09000000",
            "08000000 18000000", // offsets of the two vectors
            "10 10 0000",
            "0100000000000000 03",
            "88 c6 fa", // deltas 0..7 at 3 bits each
            "00 00 0000", // the second vector holds one value
            "0900000000000000 00")); // bit width 0, so no packed values follow
  }

  @Test
  public void testEmptyPage() throws Exception {
    assertThat(doublePage()).containsExactly(bytes("00 00 03 00000000"));
  }

  /** Ties keep HashMap order, which here differs from the C++ order (larger exponent first). */
  @Test
  public void testPresetTieOrder() {
    List<int[]> samples = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      samples.add(new int[] {18, 18});
      samples.add(new int[] {16, 16});
    }
    int[][] presets = AlpValuesWriter.buildPresets(samples, AlpValuesWriter.MAX_PRESET_COMBINATIONS);
    assertThat(presets).hasDimensions(2, 2);
    assertThat(presets[0]).containsExactly(16, 16);
    assertThat(presets[1]).containsExactly(18, 18);
  }

  private static byte[] doublePage(double... values) throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer =
        new AlpValuesWriter.DoubleAlpValuesWriter(256, 256, new DirectByteBufferAllocator(), 8);
    try {
      for (double v : values) {
        writer.writeDouble(v);
      }
      return writer.getBytes().toByteArray();
    } finally {
      writer.reset();
      writer.close();
    }
  }

  private static byte[] floatPage(float... values) throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer =
        new AlpValuesWriter.FloatAlpValuesWriter(256, 256, new DirectByteBufferAllocator(), 8);
    try {
      for (float v : values) {
        writer.writeFloat(v);
      }
      return writer.getBytes().toByteArray();
    } finally {
      writer.reset();
      writer.close();
    }
  }

  private static byte[] bytes(String... hex) {
    String digits = String.join("", hex).replace(" ", "");
    byte[] out = new byte[digits.length() / 2];
    for (int i = 0; i < out.length; i++) {
      out[i] = (byte) Integer.parseInt(digits.substring(2 * i, 2 * i + 2), 16);
    }
    return out;
  }
}
