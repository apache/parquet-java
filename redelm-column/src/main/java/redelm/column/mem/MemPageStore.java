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
package redelm.column.mem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redelm.column.ColumnDescriptor;
import redelm.column.UnknownColumnException;

public class MemPageStore extends PageStore {

  private Map<ColumnDescriptor, MemPageWriter> pageWriters = new HashMap<ColumnDescriptor, MemPageWriter>();

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    MemPageWriter pageWriter;
    if (pageWriters.containsKey(pageWriters)) {
      pageWriter = pageWriters.get(path);
    } else {
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
    List<Page> pages = new ArrayList<Page>(pageWriter.getPages());
    return new MemPageReader(pageWriter.getTotalValueCount(), pages.iterator());
  }

}
