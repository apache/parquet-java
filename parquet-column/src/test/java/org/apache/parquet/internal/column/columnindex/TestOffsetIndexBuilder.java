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
package org.apache.parquet.internal.column.columnindex;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OffsetIndexBuilder}.
 */
public class TestOffsetIndexBuilder {
  @Test
  public void testBuilderWithSizeAndRowCount() {
    OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
    assertThat(builder.build()).isNull();
    assertThat(builder.build(1234)).isNull();

    builder.add(1000, 10);
    builder.add(2000, 19);
    builder.add(3000, 27);
    builder.add(1200, 9);
    assertCorrectValues(builder.build(), 0, 1000, 0, 1000, 2000, 10, 3000, 3000, 29, 6000, 1200, 56);
    assertCorrectValues(builder.build(10000), 10000, 1000, 0, 11000, 2000, 10, 13000, 3000, 29, 16000, 1200, 56);
  }

  @Test
  public void testNoOpBuilderWithSizeAndRowCount() {
    OffsetIndexBuilder builder = OffsetIndexBuilder.getNoOpBuilder();
    builder.add(1, 2);
    builder.add(3, 4);
    builder.add(5, 6);
    builder.add(7, 8);
    assertThat(builder.build()).isNull();
    assertThat(builder.build(1000)).isNull();
  }

  @Test
  public void testBuilderWithOffsetSizeIndex() {
    OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
    assertThat(builder.build()).isNull();
    assertThat(builder.build(1234)).isNull();

    builder.add(1000, 10000, 0);
    builder.add(22000, 12000, 100);
    builder.add(48000, 22000, 211);
    builder.add(90000, 30000, 361);
    assertCorrectValues(builder.build(), 1000, 10000, 0, 22000, 12000, 100, 48000, 22000, 211, 90000, 30000, 361);
    assertCorrectValues(
        builder.build(100000), 101000, 10000, 0, 122000, 12000, 100, 148000, 22000, 211, 190000, 30000, 361);
  }

  @Test
  public void testNoOpBuilderWithOffsetSizeIndex() {
    OffsetIndexBuilder builder = OffsetIndexBuilder.getNoOpBuilder();
    builder.add(1, 2, 3);
    builder.add(4, 5, 6);
    builder.add(7, 8, 9);
    builder.add(10, 11, 12);
    assertThat(builder.build()).isNull();
    assertThat(builder.build(1000)).isNull();
  }

  private void assertCorrectValues(OffsetIndex offsetIndex, long... offset_size_rowIndex_triplets) {
    assertThat(offset_size_rowIndex_triplets.length % 3).isEqualTo(0);
    int pageCount = offset_size_rowIndex_triplets.length / 3;
    assertThat(offsetIndex.getPageCount()).as("Invalid pageCount").isEqualTo(pageCount);
    for (int i = 0; i < pageCount; ++i) {
      assertThat(offsetIndex.getOffset(i)).isEqualTo(offset_size_rowIndex_triplets[3 * i]);
      assertThat(offsetIndex.getCompressedPageSize(i)).isEqualTo(offset_size_rowIndex_triplets[3 * i + 1]);
      assertThat(offsetIndex.getFirstRowIndex(i)).isEqualTo(offset_size_rowIndex_triplets[3 * i + 2]);
      long expectedLastPageIndex = (i < pageCount - 1) ? (offset_size_rowIndex_triplets[3 * i + 5] - 1) : 999;
      assertThat(offsetIndex.getLastRowIndex(i, 1000)).isEqualTo(expectedLastPageIndex);
    }
  }
}
