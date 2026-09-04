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
package org.apache.parquet.column.mem;

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMemPageStore {

  private static final Logger LOG = LoggerFactory.getLogger(TestMemPageStore.class);

  private String[] path = {"foo", "bar"};

  @Test
  public void test() throws IOException {
    MemPageStore memPageStore = new MemPageStore(10);
    ColumnDescriptor col = new ColumnDescriptor(path, PrimitiveTypeName.INT64, 2, 2);
    LongStatistics stats = new LongStatistics();
    PageWriter pageWriter = memPageStore.getPageWriter(col);

    pageWriter.writePage(BytesInput.from(new byte[735]), 209, stats, BIT_PACKED, BIT_PACKED, PLAIN);
    pageWriter.writePage(BytesInput.from(new byte[743]), 209, stats, BIT_PACKED, BIT_PACKED, PLAIN);
    pageWriter.writePage(BytesInput.from(new byte[743]), 209, stats, BIT_PACKED, BIT_PACKED, PLAIN);
    pageWriter.writePage(BytesInput.from(new byte[735]), 209, stats, BIT_PACKED, BIT_PACKED, PLAIN);

    PageReader pageReader = memPageStore.getPageReader(col);
    long totalValueCount = pageReader.getTotalValueCount();
    LOG.info("Total value count: " + totalValueCount);

    assertThat(totalValueCount)
        .as("Expected total value count to be 836 (4 pages * 209 values)")
        .isEqualTo(836);

    int total = 0;
    int pageCount = 0;
    do {
      DataPage readPage = pageReader.readPage();

      // Assert page was successfully read
      assertThat(readPage).as("Page should not be null").isNotNull();
      // Assert page has expected value count
      assertThat(readPage.getValueCount())
          .as("Each page should have 209 values")
          .isEqualTo(209);
      // Assert encodings when the implementation is DataPageV1
      assertThat(readPage).as("Page should be an instance of DataPageV1").isInstanceOf(DataPageV1.class);
      if (readPage instanceof DataPageV1) {
        DataPageV1 v1 = (DataPageV1) readPage;
        assertThat(v1.getRlEncoding())
            .as("Page repetition level encoding should be BIT_PACKED")
            .isEqualTo(BIT_PACKED);
        assertThat(v1.getDlEncoding())
            .as("Page definition level encoding should be BIT_PACKED")
            .isEqualTo(BIT_PACKED);
        assertThat(v1.getValueEncoding())
            .as("Page value encoding should be PLAIN")
            .isEqualTo(PLAIN);
      }

      total += readPage.getValueCount();
      pageCount++;
    } while (total < totalValueCount);

    // Assert we read exactly the expected number of pages and values
    assertThat(pageCount).as("Should have read 4 pages").isEqualTo(4);
    assertThat(total).as("Total values read should match totalValueCount").isEqualTo(totalValueCount);
  }
}
