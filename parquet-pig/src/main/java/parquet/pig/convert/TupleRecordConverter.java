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
package parquet.pig.convert;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import parquet.io.convert.GroupConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.GroupType;

public class TupleRecordConverter extends RecordConverter<Tuple> {

  private TupleConverter root;

  public TupleRecordConverter(GroupType parquetSchema, Schema pigSchema) {
    this.root = new TupleConverter(parquetSchema, pigSchema);
  }

  @Override
  public Tuple getCurrentRecord() {
    return root.getCurrentTuple();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

}
