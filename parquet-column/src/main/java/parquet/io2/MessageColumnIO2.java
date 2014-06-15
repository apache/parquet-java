/**
 * Copyright 2012 Twitter, Inc.
 * Copyright 2014 GoDaddy, Inc.
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
package parquet.io2;

import parquet.column.ColumnDescriptor;
import parquet.column.impl.ColumnReadStoreImpl;
import parquet.column.page.PageReadStore;
import parquet.filter.UnboundRecordFilter;
import parquet.io.*;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.List;

public class MessageColumnIO2 extends GroupColumnIO2<MessageType> {
  public MessageColumnIO2(
      final MessageType type,
      final String name,
      final LeafInfo leafInfo,
      final List<ColumnIO2<?>> children) {
    super(type, name, leafInfo, children);
  }

  public <T> RecordReader<T> getRecordReader(PageReadStore columns, RecordMaterializer<T> recordMaterializer) {
    final List<PrimitiveColumnIO2> leaves = getLeafColumnIO();
    if (leaves.size() > 0) {
      return new parquet.io2.RecordReaderImplementation<T>(
          this,
          recordMaterializer,
          false,
          new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType())
      );
    } else {
      return new EmptyRecordReader<T>(recordMaterializer);
    }
  }

  public <T> RecordReader<T> getRecordReader(PageReadStore columns, RecordMaterializer<T> recordMaterializer,
                                             UnboundRecordFilter unboundFilter) {

    return (unboundFilter == null)
        ? getRecordReader(columns, recordMaterializer)
        : new FilteredRecordReader<T>(
        this,
        recordMaterializer,
        false,
        new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType()),
        unboundFilter,
        columns.getRowCount()
    );
  }

  public List<ColumnDescriptor> getPhysicalColumnDescriptors() {
    final List<PrimitiveColumnIO2> leafs = getLeafColumnIO();
    final ArrayList<ColumnDescriptor> res = new ArrayList<ColumnDescriptor>(leafs.size());
    for (PrimitiveColumnIO2 leaf : leafs) {
      res.add(leaf.getLeafInfo().getPhysicalPath().getColumnDescriptor());
    }
    return res;
  }
}
