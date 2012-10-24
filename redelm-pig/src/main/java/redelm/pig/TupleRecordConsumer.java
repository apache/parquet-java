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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import redelm.Log;
import redelm.io.RecordConsumer;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TupleRecordConsumer extends RecordConsumer {
  private static final boolean DEBUG = Log.DEBUG;
  private static final Log LOG = Log.getLog(TupleRecordConsumer.class);
  private static final TupleFactory tf = TupleFactory.getInstance();
  private static final BagFactory bf = BagFactory.getInstance();

  private Deque<Type> types = new ArrayDeque<Type>();
  private Deque<FieldSchema> pigTypes = new ArrayDeque<FieldSchema>();
  private Deque<Tuple> groups = new ArrayDeque<Tuple>();
  private Deque<Integer> fields = new ArrayDeque<Integer>();
  private final Collection<Tuple> destination;

  public TupleRecordConsumer(MessageType schema, Schema pigSchema, Collection<Tuple> destination) {
    try {
      this.destination = destination;
      this.types.push(schema);
      this.pigTypes.push(new FieldSchema("tuple", pigSchema, DataType.TUPLE));
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void startMessage() {
    groups.push(tf.newTuple(types.peek().asGroupType().getFieldCount()));
  }

  @Override
  public void endMessage() {
    destination.add(groups.pop());
  }

  @Override
  public void startField(String field, int index) {
    fields.push(index);
  }

  @Override
  public void endField(String field, int index) {
    fields.pop();
  }

  @Override
  public void startGroup() {
    try {
      Type fieldType = types.peek().asGroupType().getType(fields.peek());

      FieldSchema fieldSchema = getPigChildSchema();
      Tuple newTuple = tf.newTuple(fieldType.asGroupType().getFieldCount());
      types.push(fieldType);
      pigTypes.push(fieldSchema);
      groups.push(newTuple);
    } catch (Exception e) {
      throw new RuntimeException("error "+e.toString()+"\ntype: "+types.peek()+"\npig type: "+pigTypes.peek(), e);
    }
  }

  private FieldSchema getPigChildSchema() throws FrontendException {
    FieldSchema fieldSchema;
    FieldSchema currentPigType = pigTypes.peek();
    int fieldIndex = fields.peek();
    if (currentPigType.type == DataType.BAG) {
      if (DEBUG) LOG.debug("skipping Bag of : " + currentPigType.schema);
      fieldSchema = currentPigType.schema.getField(0).schema.getField(fieldIndex);
    } else if (currentPigType.type == DataType.MAP) {
      if (DEBUG) LOG.debug("skipping Map of : " + currentPigType.schema);
      if (fieldIndex == 0) {
        fieldSchema = new FieldSchema("key", DataType.CHARARRAY);
      } else {
        fieldSchema = currentPigType.schema.getField(0);
      }
    } else {
      fieldSchema = currentPigType.schema.getField(fieldIndex);
    }
    return fieldSchema;
  }

  @Override
  public void endGroup() {
    types.pop();
    pigTypes.pop();
    setCurrentField(groups.pop());
  }

  private void setCurrentField(Object value) {
    try {
      Tuple parent = groups.peek();
      GroupType type = types.peek().asGroupType();
      int fieldIndex = fields.peek();
      FieldSchema pigChildSchema = getPigChildSchema();
      if (type.getType(fieldIndex).getRepetition() == Repetition.REPEATED) {
        switch (pigChildSchema.type) {
        case DataType.BAG:
          DataBag bag = (DataBag)parent.get(fieldIndex);
          if (bag == null) {
            bag = bf.newDefaultBag();
            parent.set(fieldIndex, bag);
          }
          if (value instanceof Tuple) {
            bag.add((Tuple)value);
          } else {
            bag.add(tf.newTuple(value));
          }
          break;
        case DataType.MAP:
          Map<String, Object> map = (Map<String, Object>)parent.get(fieldIndex);
          if (map == null) {
            map = new HashMap<String, Object>();
            parent.set(fieldIndex, map);
          }
          Tuple t = (Tuple)value;
          map.put((String)t.get(0), t.get(1));
          break;
        default:
          throw new RuntimeException("unsupported repeated field "+pigChildSchema+" for "+type.getType(fieldIndex));
        }
      } else {
        parent.set(fieldIndex, value);
      }
    } catch (Exception e) {
      throw new RuntimeException("error\ntype: "+types.peek()+"\npig type: "+pigTypes.peek() + "\nfield: "+fields.peek(), e);
    }
  }

  @Override
  public void addInt(int value) {
    setCurrentField(value);
  }

  @Override
  public void addLong(long value) {
    setCurrentField(value);
  }

  @Override
  public void addString(String value) {
    setCurrentField(value);
  }

  @Override
  public void addBoolean(boolean value) {
    setCurrentField(value);
  }

  @Override
  public void addBinary(byte[] value) {
    setCurrentField(new DataByteArray(value));
  }

  @Override
  public void addFloat(float value) {
    setCurrentField(value);
  }

  @Override
  public void addDouble(double value) {
    setCurrentField(value);
  }

}
