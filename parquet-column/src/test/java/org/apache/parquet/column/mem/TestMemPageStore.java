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

import static org.apache.parquet.column.Encoding.*;

import java.io.IOException;

import org.junit.Test;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class TestMemPageStore {

  private String[] path = { "foo", "bar" };

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
    System.out.println(totalValueCount);
    int total = 0;
    do {
      DataPage readPage = pageReader.readPage();
      total += readPage.getValueCount();
      System.out.println(readPage);
      // TODO: assert
    } while (total < totalValueCount);
  }
}
