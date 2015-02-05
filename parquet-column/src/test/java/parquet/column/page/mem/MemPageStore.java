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
package parquet.column.page.mem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.UnknownColumnException;
import parquet.column.page.DataPage;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.column.page.PageWriteStore;
import parquet.column.page.PageWriter;


public class MemPageStore implements PageReadStore, PageWriteStore {
  private static final Log LOG = Log.getLog(MemPageStore.class);

  private Map<ColumnDescriptor, MemPageWriter> pageWriters = new HashMap<ColumnDescriptor, MemPageWriter>();

  private long rowCount;

  public MemPageStore(long rowCount) {
    super();
    this.rowCount = rowCount;
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    MemPageWriter pageWriter = pageWriters.get(path);
    if (pageWriter == null) {
      pageWriter = new MemPageWriter();
      pageWriters.put(path, pageWriter);
    }
    return pageWriter;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor descriptor) {
    MemPageWriter pageWriter = pageWriters.get(descriptor);
    if (pageWriter == null) {
      throw new UnknownColumnException(descriptor);
    }
    List<DataPage> pages = new ArrayList<DataPage>(pageWriter.getPages());
    if (Log.DEBUG) LOG.debug("initialize page reader with "+ pageWriter.getTotalValueCount() + " values and " + pages.size() + " pages");
    return new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  public void addRowCount(long count) {
    rowCount += count;
  }
}
