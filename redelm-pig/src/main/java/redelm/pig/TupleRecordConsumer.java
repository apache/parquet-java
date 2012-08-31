package redelm.pig;

import java.util.ArrayDeque;
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

  TupleFactory tf = TupleFactory.getInstance();
  BagFactory bf = BagFactory.getInstance();

  Deque<Type> types = new ArrayDeque<Type>();
  Deque<Tuple> groups = new ArrayDeque<Tuple>();
  Deque<String> fields = new ArrayDeque<String>();
  int indent = 0;

  public TupleRecordConsumer(MessageType schema, Tuple root) {
    types.push(schema);
    groups.push(root);
  }

  @Override
  public void startField(String field) {
    types.push(types.peek().asGroupType().getType(field));
    fields.push(field);
  }

  @Override
  public void startGroup() {
    Tuple newTuple = tf.newTuple();
    setCurrentField(newTuple);
    groups.push(newTuple);
  }

  private void setCurrentField(Object value) {
    try {
      Tuple parent = groups.peek();
      GroupType type = types.peek().asGroupType();
      int fieldIndex = type.getFieldIndex(fields.peek());
      if (type.getRepetition() == Repetition.REPEATED) {
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
  public void endGroup() {
    groups.pop();
  }

  @Override
  public void endField(String field) {
    fields.pop();
    types.pop();
  }

}
