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

import static org.apache.parquet.column.impl.ContentDefinedChunker.calculateMask;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Arrow C++'s TestCDC in {@code parquet/chunker_internal_test.cc}. */
public class TestContentDefinedChunker {

  private static final String MASK_OUT_OF_RANGE = "The content defined chunking mask must be between 1 and 63 bits";

  @ParameterizedTest
  @CsvSource({
    "262144, 1048576, 0",
    "0, 32, 0",
    "32, 64, 0",
    "1048576, 2097152, 0",
    "1073741824, 2147483648, 0",
    "1, 65, 1",
    "0, 128, 2"
  })
  public void chunkSizeParametersAreAccepted(long min, long max, int normLevel) {
    assertThatCode(() -> properties(min, max, normLevel)).doesNotThrowAnyException();
  }

  /** With norm level 0, 1 or 2 the maximum must exceed the minimum by at least 32, 64 or 128 bytes. */
  @ParameterizedTest
  @CsvSource({
    "0, -1, 0",
    "1024, 512, 0",
    "-1, 0, 0",
    "0, 0, 0",
    "0, 16, 0",
    "-16, -16, 0",
    "16, 0, 0",
    "32, 32, 0",
    "32, 48, 0",
    "1, -1, 1",
    "-1, 1, 1",
    "1, 1, 1",
    "1, 32, 1",
    "1, 33, 1",
    "0, 123, 2"
  })
  public void chunkSizeParametersAreRejected(long min, long max, int normLevel) {
    assertThatThrownBy(() -> properties(min, max, normLevel))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("content defined chunking");
  }

  @Test
  public void rollingHashMaskCalculation() {
    long min = 256 * 1024;
    long max = 1024 * 1024;
    assertThat(calculateMask(min, max, 0)).isEqualTo(0xFFFE000000000000L);
    assertThat(calculateMask(min, max, 1)).isEqualTo(0xFFFC000000000000L);
    assertThat(calculateMask(min, max, 2)).isEqualTo(0xFFF8000000000000L);
    assertThat(calculateMask(min, max, 3)).isEqualTo(0xFFF0000000000000L);
    assertThat(calculateMask(min, max, -1)).isEqualTo(0xFFFF000000000000L);

    // The mask bits must be between 1 and 63 once adjusted for the 8 gear hash tables
    assertThatThrownBy(() -> calculateMask(0, 16, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(MASK_OUT_OF_RANGE);
    assertThat(calculateMask(0, 32, 0)).isEqualTo(0x8000000000000000L);
    assertThatThrownBy(() -> calculateMask(0, 32, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(MASK_OUT_OF_RANGE);
    assertThat(calculateMask(0, 64, 0)).isEqualTo(0xC000000000000000L);
    assertThat(calculateMask(0, 16, -1)).isEqualTo(0x8000000000000000L);

    // Unrealistic, but checks the validation
    assertThatThrownBy(() -> calculateMask(128, 384, -60))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(MASK_OUT_OF_RANGE);
    assertThat(calculateMask(128, 384, -59)).isEqualTo(0xFFFFFFFFFFFFFFFEL);

    // Unlike Arrow C++, the average chunk size does not overflow
    assertThat(calculateMask(256 * 1024, Long.MAX_VALUE, 0)).isEqualTo(0xFFFFFFFFFFFFFFC0L);
  }

  /**
   * Arrow C++'s WriteSingleColumnParquetFile: a column write store made without the chunkers of the
   * file, which every row group of the file must continue, is rejected.
   */
  @Test
  public void storeWithoutTheChunkersOfTheFileIsRejected() {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required int32 number; }");
    ParquetProperties props =
        ParquetProperties.builder().withContentDefinedChunking(true).build();
    String message = "Content defined chunking needs the chunkers of the file";

    assertThatThrownBy(() -> props.newColumnWriteStore(schema, new MemPageStore(1), column -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> props.newColumnWriteStore(schema, new MemPageStore(1)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> new ColumnWriteStoreV1(new MemPageStore(1), props))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> new ColumnWriteStoreV2(schema, new MemPageStore(1), column -> null, props, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(message);
    assertThatThrownBy(() -> props.newColumnWriteStore(schema, new MemPageStore(1), column -> null, List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Expected a chunker per column");
  }

  private static ParquetProperties properties(long min, long max, int normLevel) {
    return ParquetProperties.builder()
        .withContentDefinedChunking(true)
        .withContentDefinedChunkingMinSize(min)
        .withContentDefinedChunkingMaxSize(max)
        .withContentDefinedChunkingNormLevel(normLevel)
        .build();
  }
}
