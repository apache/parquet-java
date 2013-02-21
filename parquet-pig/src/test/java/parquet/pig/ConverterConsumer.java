package parquet.pig;

import java.util.ArrayDeque;
import java.util.Deque;

import parquet.io.RecordConsumer;
import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.schema.MessageType;
import parquet.schema.Type;

public class ConverterConsumer extends RecordConsumer {

  private final GroupConverter root;
  private final MessageType schema;

  private Deque<GroupConverter> path = new ArrayDeque<GroupConverter>();
  private Deque<Type> typePath = new ArrayDeque<Type>();
  private GroupConverter current;
  private PrimitiveConverter currentPrimitive;
  private Type currentType;

  public ConverterConsumer(GroupConverter recordConsumer, MessageType schema) {
    this.root = recordConsumer;
    this.schema = schema;
  }

  @Override
  public void startMessage() {
    root.start();
    this.currentType = schema;
    this.current = root;
  }

  @Override
  public void endMessage() {
    root.end();
  }

  @Override
  public void startField(String field, int index) {
    path.push(current);
    typePath.push(currentType);
    currentType = currentType.asGroupType().getType(index);
    if (currentType.isPrimitive()) {
      currentPrimitive = current.getPrimitiveConverter(index);
    } else {
      current = current.getGroupConverter(index);
    }
  }

  @Override
  public void endField(String field, int index) {
    currentType = typePath.pop();
    current = path.pop();
  }

  @Override
  public void startGroup() {
    current.start();
  }

  @Override
  public void endGroup() {
    current.end();
  }

  @Override
  public void addInteger(int value) {
    currentPrimitive.addInt(value);
  }

  @Override
  public void addLong(long value) {
    currentPrimitive.addLong(value);
  }

  @Override
  public void addBoolean(boolean value) {
    currentPrimitive.addBoolean(value);
  }

  @Override
  public void addBinary(byte[] value) {
    currentPrimitive.addBinary(value);
  }

  @Override
  public void addFloat(float value) {
    currentPrimitive.addFloat(value);
  }

  @Override
  public void addDouble(double value) {
    currentPrimitive.addDouble(value);
  }

}
