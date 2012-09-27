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
package redelm.pig;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import redelm.io.RecordConsumer;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

public class TupleWriter {

  private final RecordConsumer recordConsumer;
  private final MessageType rootSchema;

  public TupleWriter(RecordConsumer recordConsumer, MessageType schema) {
    this.recordConsumer = recordConsumer;
    this.rootSchema = schema;
  }

  public void write(Tuple t) throws ExecException {
    recordConsumer.startMessage();
    writeTuple(rootSchema, t);
    recordConsumer.endMessage();
  }

  private void writeTuple(GroupType schema, Tuple t) throws ExecException {
    List<Type> fields = schema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      if (!t.isNull(i)) {
        Type fieldType = fields.get(i);
        if (fieldType.getRepetition() == Repetition.REPEATED) {
          DataBag bag = (DataBag)t.get(i);
          if (bag.size() > 0) {
            recordConsumer.startField(fieldType.getName(), i);
            for (Tuple tuple : bag) {
              if (fieldType.isPrimitive()) {
                writeValue(fieldType, tuple, 0);
              } else {
                recordConsumer.startGroup();
                writeTuple(fieldType.asGroupType(), tuple);
                recordConsumer.endGroup();
              }
            }
            recordConsumer.endField(fieldType.getName(), i);
          }
        } else {
          recordConsumer.startField(fieldType.getName(), i);
          writeValue(fieldType, t, i);
          recordConsumer.endField(fieldType.getName(), i);
        }
      }
    }
  }

  private void writeValue(Type type, Tuple t, int i) throws ExecException {
    if (type.isPrimitive()) {
      switch (type.asPrimitiveType().getPrimitive()) {
      // TODO: use PrimitiveTuple accessors
        case BINARY:
          recordConsumer.addBinary(((DataByteArray)t.get(i)).get());
          break;
        case BOOL:
          recordConsumer.addBoolean((Boolean)t.get(i));
          break;
        case INT64:
          recordConsumer.addInt(((Number)t.get(i)).intValue());
          break;
        case STRING:
          recordConsumer.addString((String)t.get(i));
          break;
        default:
          throw new UnsupportedOperationException(type.asPrimitiveType().getPrimitive().name());
      }
    } else {
      recordConsumer.startGroup();
      writeTuple(type.asGroupType(), (Tuple)t.get(i));
      recordConsumer.endGroup();
    }
  }

}
