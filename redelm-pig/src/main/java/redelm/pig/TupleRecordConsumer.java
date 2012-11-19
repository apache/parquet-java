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

import static redelm.schema.Type.Repetition.REPEATED;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import redelm.Log;
import redelm.io.RecordConsumer;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 *
 * Converts a tuple into the format understood by the striping algorithm
 *
 * TODO: pre-process the schema to avoid traversing it for each record
 *
 * @author Julien Le Dem
 *
 */
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
      if (schema == null) {
        throw new NullPointerException("schema");
      }
      if (pigSchema == null) {
        throw new NullPointerException("pigSchema");
      }
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
      types.push(fieldType);
      pigTypes.push(fieldSchema);
      switch (fieldSchema.type) {
        case DataType.BAG:
          groups.peek().set(fields.peek(), bf.newDefaultBag());
          break;
        case DataType.MAP:
          groups.peek().set(fields.peek(), new HashMap<String, Object>());
          break;
        default:
          Tuple newTuple = tf.newTuple(fieldType.asGroupType().getFieldCount());
          groups.push(newTuple);
      }
    } catch (Exception e) {
      throw new RuntimeException("error "+e.toString()+"\ntype: "+types.peek()+"\npig type: "+pigTypes.peek(), e);
    }
  }

  private FieldSchema getPigChildSchema() {
    FieldSchema fieldSchema;
    Iterator<FieldSchema> it = pigTypes.iterator();
    FieldSchema currentPigType = it.next();
    FieldSchema previousPigType = it.hasNext() ? it.next() : null;
    int fieldIndex = fields.peek();
    try {
      if (previousPigType!=null && previousPigType.type == DataType.MAP) {
        if (DEBUG) LOG.debug("handling Map of : " + currentPigType);
        if (fieldIndex == 0) {
          fieldSchema = new FieldSchema("key", DataType.CHARARRAY);
        } else if (fieldIndex == 1) {
          fieldSchema = currentPigType;
        } else {
          throw new RuntimeException("can't access field" + fieldIndex + " in map entry " + previousPigType);
        }
      } else {
        fieldSchema = currentPigType.schema.getField(fieldIndex);
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage()+" currentPigType: "+currentPigType.type+" " +currentPigType + " at "+fieldIndex,e);
    }
    return fieldSchema;
  }

  @Override
  public void endGroup() {
    types.pop();
    FieldSchema fieldSchema = pigTypes.pop();
    switch (fieldSchema.type) {
    case DataType.BAG:
    case DataType.MAP:
      if (DEBUG) LOG.debug("not poping the value");
      break;
    default:
      setCurrentField(groups.pop());
    }
  }

  private void setCurrentField(Object value) {
    try {
      Tuple parent = groups.peek();
      GroupType type = types.peek().asGroupType();
      Iterator<Integer> it = fields.iterator();
      int fieldIndex = it.next();
      int previousFieldIndex = it.hasNext() ? it.next() : -1;
      if (type.getType(fieldIndex).getRepetition() == REPEATED) {
        Object repeated = parent.get(previousFieldIndex);
        if (repeated instanceof DataBag) {
          DataBag bag = (DataBag) repeated;
          if (value instanceof Tuple) {
            bag.add((Tuple)value);
          } else {
            bag.add(tf.newTuple(value));
          }
        } else if (repeated instanceof Map) {
          @SuppressWarnings("unchecked") // I know
          Map<String, Object> map = (Map<String, Object>)repeated;
          Tuple t = (Tuple)value;
          map.put((String)t.get(0), t.get(1));
        } else {
          throw new RuntimeException("Unsupported repeated field " + repeated.getClass().getName() + " " + repeated);
        }
      } else {
        parent.set(fieldIndex, value);
      }
    } catch (Exception e) {
      throw new RuntimeException("error\ntype: "+types.peek()+"\npig type: "+pigTypes.peek() + "\nfield: "+fields.peek(), e);
    }
  }

  @Override
  public void addInteger(int value) {
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
