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
package parquet.example.data;

import parquet.io.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.Type;

public class GroupWriter {

  private final RecordConsumer recordConsumer;
  private final GroupType schema;

  public GroupWriter(RecordConsumer recordConsumer, GroupType schema) {
    this.recordConsumer = recordConsumer;
    this.schema = schema;
  }

  public void write(Group group) {
    recordConsumer.startMessage();
    writeGroup(group, schema);
    recordConsumer.endMessage();
  }

  private void writeGroup(Group group, GroupType type) {
    int fieldCount = type.getFieldCount();
    for (int field = 0; field < fieldCount; ++field) {
      int valueCount = group.getFieldRepetitionCount(field);
      if (valueCount > 0) {
        Type fieldType = type.getType(field);
        String fieldName = fieldType.getName();
        recordConsumer.startField(fieldName, field);
        for (int index = 0; index < valueCount; ++index) {
          if (fieldType.isPrimitive()) {
            group.writeValue(field, index, recordConsumer);
          } else {
            recordConsumer.startGroup();
            writeGroup(group.getGroup(field, index), fieldType.asGroupType());
            recordConsumer.endGroup();
          }
        }
        recordConsumer.endField(fieldName, field);
      }
    }
  }
}