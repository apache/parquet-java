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
package parquet.example.data.simple.convert;

import parquet.example.data.Group;
import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.GroupType;
import parquet.schema.Type;

class SimpleGroupConverter extends RecordConverter<Group> {
  private final SimpleGroupConverter parent;
  private final int index;
  private Group current;
  private GroupConverter[] groupConverters;
  private PrimitiveConverter[] primitiveConverters;

  SimpleGroupConverter(SimpleGroupConverter parent, int index, GroupType schema) {
    this.parent = parent;
    this.index = index;

    groupConverters = new GroupConverter[schema.getFieldCount()];
    primitiveConverters = new PrimitiveConverter[schema.getFieldCount()];

    for (int i = 0; i < groupConverters.length; i++) {
      final Type type = schema.getType(i);
      if (type.isPrimitive()) {
        primitiveConverters[i] = new SimplePrimitiveConverter(this, i);
      } else {
        groupConverters[i] = new SimpleGroupConverter(this, i, type.asGroupType());
      }

    }
  }

  @Override
  public void start() {
    current = parent.getCurrentRecord().addGroup(index);
  }

  @Override
  public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
    return primitiveConverters[fieldIndex];
  }

  @Override
  public GroupConverter getGroupConverter(int fieldIndex) {
    return groupConverters[fieldIndex];
  }

  @Override
  public void end() {
  }

  @Override
  public Group getCurrentRecord() {
    return current;
  }
}