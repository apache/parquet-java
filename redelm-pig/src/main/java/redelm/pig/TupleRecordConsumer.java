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

import redelm.io.RecordConsumer;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class TupleRecordConsumer extends RecordConsumer {

  private static final TupleFactory tf = TupleFactory.getInstance();
  private static final BagFactory bf = BagFactory.getInstance();

  private Deque<Type> types = new ArrayDeque<Type>();
  private Deque<Tuple> groups = new ArrayDeque<Tuple>();
  private Deque<String> fields = new ArrayDeque<String>();
  private final Collection<Tuple> destination;

  public TupleRecordConsumer(MessageType schema, Collection<Tuple> destination) {
    this.destination = destination;
    this.types.push(schema);
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
    fields.push(field);
  }

  @Override
  public void endField(String field, int index) {
    fields.pop();
  }

  @Override
  public void startGroup() {
    Type fieldType = types.peek().asGroupType().getType(fields.peek());
    Tuple newTuple = tf.newTuple(fieldType.asGroupType().getFieldCount());
    setCurrentField(newTuple);
    types.push(fieldType);
    groups.push(newTuple);
  }

  @Override
  public void endGroup() {
    groups.pop();
    types.pop();
  }

  private void setCurrentField(Object value) {
    try {
      Tuple parent = groups.peek();
      GroupType type = types.peek().asGroupType();
      int fieldIndex = type.getFieldIndex(fields.peek());
      if (type.getType(fieldIndex).getRepetition() == Repetition.REPEATED) {
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
      } else {
        parent.set(fieldIndex, value);
      }
    } catch (ExecException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addInt(int value) {
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
