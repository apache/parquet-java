/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.mem;

import static parquet.column.Encoding.PLAIN;

import java.io.IOException;

import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class TestMemPageStore {

  private String[] path = { "foo", "bar"};

  @Test
  public void test() throws IOException {
    MemPageStore memPageStore = new MemPageStore();
    ColumnDescriptor col = new ColumnDescriptor(path , PrimitiveTypeName.INT64, 2, 2);
    PageWriter pageWriter = memPageStore.getPageWriter(col);
    pageWriter.writePage(BytesInput.from(new byte[735]), 209, PLAIN);
    pageWriter.writePage(BytesInput.from(new byte[743]), 209, PLAIN);
    pageWriter.writePage(BytesInput.from(new byte[743]), 209, PLAIN);
    pageWriter.writePage(BytesInput.from(new byte[735]), 209, PLAIN);
    PageReader pageReader = memPageStore.getPageReader(col);
    long totalValueCount = pageReader.getTotalValueCount();
    System.out.println(totalValueCount);
    int total = 0;
    do {
      Page readPage = pageReader.readPage();
      total += readPage.getValueCount();
      System.out.println(readPage);
      // TODO: assert
    } while (total < totalValueCount);
  }
}
