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
package org.apache.parquet.internal.column.chunking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Ported one for one from Arrow C++'s {@code TestCDC.RollingHashMaskCalculation} and
 * {@code TestCDC.ChunkSizeParameterValidation} ({@code cpp/src/parquet/chunker_internal_test.cc}),
 * which is what makes these an independent oracle rather than a restatement of this
 * implementation.
 */
public class TestRollingHashMask {

  private static final long MIN_SIZE = 256 * 1024L;
  private static final long MAX_SIZE = 1024 * 1024L;

  @Test
  public void maskMatchesTheReferenceForEachNormalizationLevel() {
    assertThat(RollingHashMask.calculate(MIN_SIZE, MAX_SIZE, 0)).isEqualTo(0xFFFE000000000000L);
    assertThat(RollingHashMask.calculate(MIN_SIZE, MAX_SIZE, 1)).isEqualTo(0xFFFC000000000000L);
    assertThat(RollingHashMask.calculate(MIN_SIZE, MAX_SIZE, 2)).isEqualTo(0xFFF8000000000000L);
    assertThat(RollingHashMask.calculate(MIN_SIZE, MAX_SIZE, 3)).isEqualTo(0xFFF0000000000000L);
    assertThat(RollingHashMask.calculate(MIN_SIZE, MAX_SIZE, -1)).isEqualTo(0xFFFF000000000000L);
  }

  @Test
  public void maskMatchesTheReferenceAtTheEdgesOfItsRange() {
    assertThat(RollingHashMask.calculate(0, 32, 0)).isEqualTo(0x8000000000000000L);
    assertThat(RollingHashMask.calculate(0, 64, 0)).isEqualTo(0xC000000000000000L);
    assertThat(RollingHashMask.calculate(0, 16, -1)).isEqualTo(0x8000000000000000L);
    assertThat(RollingHashMask.calculate(128, 384, -59)).isEqualTo(0xFFFFFFFFFFFFFFFEL);
  }

  @Test
  public void rejectsARangeThatIsNotAscending() {
    assertThatThrownBy(() -> RollingHashMask.calculate(1024, 512, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be greater than");
    assertThatThrownBy(() -> RollingHashMask.calculate(32, 32, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be greater than");
  }

  @Test
  public void rejectsASizeRangeTooNarrowForTheNormalizationLevel() {
    // With eight tables the mask needs at least one bit, so the min/max gap must be at least 32 at
    // normLevel 0, 64 at 1 and 128 at 2.
    assertThatThrownBy(() -> RollingHashMask.calculate(0, 16, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 63 bits");
    assertThatCode(() -> RollingHashMask.calculate(0, 32, 0)).doesNotThrowAnyException();
    assertThatThrownBy(() -> RollingHashMask.calculate(32, 48, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 63 bits");
    assertThatCode(() -> RollingHashMask.calculate(32, 64, 0)).doesNotThrowAnyException();

    assertThatThrownBy(() -> RollingHashMask.calculate(1, 33, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 63 bits");
    assertThatCode(() -> RollingHashMask.calculate(1, 65, 1)).doesNotThrowAnyException();

    assertThatThrownBy(() -> RollingHashMask.calculate(0, 123, 2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 63 bits");
    assertThatCode(() -> RollingHashMask.calculate(0, 128, 2)).doesNotThrowAnyException();

    assertThatThrownBy(() -> RollingHashMask.calculate(128, 384, -60))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 63 bits");
  }

  @Test
  public void acceptsTheDefaultAndLargeEnvelopes() {
    assertThatCode(() -> RollingHashMask.calculate(1024 * 1024L * 1024L, 2L * 1024 * 1024 * 1024, 0))
        .doesNotThrowAnyException();
  }

  /**
   * Arrow C++ and arrow-rs overflow when they sum the two sizes; halving first avoids it. No usable
   * page size is anywhere near here, but the deviation is deliberate.
   */
  @Test
  public void handlesAnEnvelopeThatWouldOverflowTheAverage() {
    assertThat(RollingHashMask.calculate(256 * 1024L, Long.MAX_VALUE, 0)).isEqualTo(0xFFFFFFFFFFFFFFC0L);
  }
}
