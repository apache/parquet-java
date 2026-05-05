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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
import org.junit.Test;
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

    assertEquals("Expected total value count to be 836 (4 pages * 209 values)", 836, totalValueCount);

    int total = 0;
    int pageCount = 0;
    do {
      DataPage readPage = pageReader.readPage();

      // Assert page was successfully read
      assertNotNull("Page should not be null", readPage);
      // Assert page has expected value count
      assertEquals("Each page should have 209 values", 209, readPage.getValueCount());
      // Assert encodings when the implementation is DataPageV1
      assertTrue("Page should be an instance of DataPageV1", readPage instanceof DataPageV1);
      if (readPage instanceof DataPageV1) {
        DataPageV1 v1 = (DataPageV1) readPage;
        assertEquals("Page repetition level encoding should be BIT_PACKED", BIT_PACKED, v1.getRlEncoding());
        assertEquals("Page definition level encoding should be BIT_PACKED", BIT_PACKED, v1.getDlEncoding());
        assertEquals("Page value encoding should be PLAIN", PLAIN, v1.getValueEncoding());
      }

      total += readPage.getValueCount();
      pageCount++;
    } while (total < totalValueCount);

    // Assert we read exactly the expected number of pages and values
    assertEquals("Should have read 4 pages", 4, pageCount);
    assertEquals("Total values read should match totalValueCount", totalValueCount, total);
  }
}
