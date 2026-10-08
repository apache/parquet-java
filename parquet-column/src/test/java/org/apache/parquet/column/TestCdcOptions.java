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
package org.apache.parquet.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestCdcOptions {

  /** Boundaries only match Arrow C++ and arrow-rs while the defaults do. */
  @Test
  public void defaultsMatchTheOtherImplementations() {
    assertThat(CdcOptions.DEFAULT.getMinChunkSize()).isEqualTo(256 * 1024L);
    assertThat(CdcOptions.DEFAULT.getMaxChunkSize()).isEqualTo(1024 * 1024L);
    assertThat(CdcOptions.DEFAULT.getNormLevel()).isZero();
  }

  @Test
  public void toStringNamesEverySetting() {
    assertThat(options(64 * 1024, 256 * 1024, -1))
        .asString()
        .contains("65536")
        .contains("262144")
        .contains("-1");
  }

  @Test
  public void rejectsSizesAtTheSetterThatSaysWhichOneIsWrong() {
    assertThatThrownBy(() -> CdcOptions.builder().withMinChunkSize(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking minimum chunk size (negative): -1");
    assertThatThrownBy(() -> CdcOptions.builder().withMaxChunkSize(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content defined chunking maximum chunk size (not positive): 0");
  }

  @Test
  public void anUnusableSizeEnvelopeIsRejectedWhenTheOptionsAreBuilt() {
    assertThatThrownBy(() -> CdcOptions.builder()
            .withMinChunkSize(0)
            .withMaxChunkSize(16)
            .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 63 bits");
  }

  private static CdcOptions options(long min, long max, int normLevel) {
    return CdcOptions.builder()
        .withMinChunkSize(min)
        .withMaxChunkSize(max)
        .withNormLevel(normLevel)
        .build();
  }
}
