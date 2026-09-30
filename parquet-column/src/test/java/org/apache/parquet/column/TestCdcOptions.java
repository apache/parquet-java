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

import static org.apache.parquet.column.impl.ChunkingTestSupport.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestCdcOptions {

  /** Arrow C++ and arrow-rs both default to 256 KiB / 1 MiB / 0; boundaries only match theirs while these do. */
  @Test
  public void defaultsMatchTheOtherImplementations() {
    assertThat(CdcOptions.DEFAULT.getMinChunkSize()).isEqualTo(256 * 1024L);
    assertThat(CdcOptions.DEFAULT.getMaxChunkSize()).isEqualTo(1024 * 1024L);
    assertThat(CdcOptions.DEFAULT.getNormLevel()).isZero();
  }

  @Test
  public void equalsDistinguishesEverySetting() {
    CdcOptions base = options(64 * 1024, 256 * 1024, 0);

    assertThat(base).isEqualTo(options(64 * 1024, 256 * 1024, 0));
    assertThat(base).hasSameHashCodeAs(options(64 * 1024, 256 * 1024, 0));

    assertThat(base).isNotEqualTo(options(32 * 1024, 256 * 1024, 0));
    assertThat(base).isNotEqualTo(options(64 * 1024, 512 * 1024, 0));
    assertThat(base).isNotEqualTo(options(64 * 1024, 256 * 1024, 1));
    assertThat(base).isNotEqualTo(null);
    assertThat(base).isNotEqualTo("not options");
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
    // Rejected here rather than once a column writer exists, so a bad configuration cannot get as
    // far as a half-written file.
    assertThatThrownBy(() -> CdcOptions.builder()
            .withMinChunkSize(0)
            .withMaxChunkSize(16)
            .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 63 bits");

    assertThatThrownBy(() -> CdcOptions.builder()
            .withMinChunkSize(1024)
            .withMaxChunkSize(512)
            .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be greater than");
  }
}
