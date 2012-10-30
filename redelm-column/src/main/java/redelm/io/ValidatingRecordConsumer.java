package redelm.io;

import java.util.ArrayDeque;
import java.util.Deque;

import redelm.Log;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType.Primitive;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

public class ValidatingRecordConsumer extends RecordConsumer {
  private static final Log LOG = Log.getLog(ValidatingRecordConsumer.class);
  private static final boolean DEBUG = Log.DEBUG;

  private final RecordConsumer delegate;

  private Deque<Type> types = new ArrayDeque<Type>();
  private Deque<Integer> fields = new ArrayDeque<Integer>();
  private Deque<Integer> previousField = new ArrayDeque<Integer>();
  private Deque<Integer> fieldValueCount = new ArrayDeque<Integer>();

  public ValidatingRecordConsumer(RecordConsumer delegate, MessageType schema) {
    this.delegate = delegate;
    this.types.push(schema);
  }

  public void startMessage() {
    previousField.push(-1);
    delegate.startMessage();
  }

  public void endMessage() {
    delegate.endMessage();
    validateMissingFields(types.peek().asGroupType().getFieldCount());
    previousField.pop();
  }

  public void startField(String field, int index) {
    if (index <= previousField.peek()) {
      throw new InvalidRecordException("fields must be added in order " + field + " index " + index + " is before previous field " + previousField.peek());
    }
    validateMissingFields(index);
    fields.push(index);
    fieldValueCount.push(0);
    delegate.startField(field, index);
  }

  private void validateMissingFields(int index) {
    for (int i = previousField.peek() + 1; i < index; i++) {
      Type type = types.peek().asGroupType().getType(i);
      if (type.getRepetition() == Repetition.REQUIRED) {
        throw new InvalidRecordException("required field is missing " + type);
      }
    }
  }

  public void endField(String field, int index) {
    delegate.endField(field, index);
    fieldValueCount.pop();
    previousField.push(fields.pop());
  }

  public void startGroup() {
    previousField.push(-1);
    types.push(types.peek().asGroupType().getType(fields.peek()));
    delegate.startGroup();
  }

  public void endGroup() {
    delegate.endGroup();
    validateMissingFields(types.peek().asGroupType().getFieldCount());
    types.pop();
    previousField.pop();
  }

  private void validate(Primitive p) {
    Type currentType = types.peek().asGroupType().getType(fields.peek());
    int c = fieldValueCount.pop() + 1;
    fieldValueCount.push(c);
    if (DEBUG) LOG.debug("validate " + p + " for " + currentType.getName());
    switch (currentType.getRepetition()) {
      case OPTIONAL:
      case REQUIRED:
        if (c > 1) {
          throw new InvalidRecordException("repeated value when the type is not repeated in " + currentType);
        }
        break;
      case REPEATED:
        break;
      default:
        throw new InvalidRecordException("unknown repetition " + currentType.getRepetition() + " in " + currentType);
    }
    if (!currentType.isPrimitive() || currentType.asPrimitiveType().getPrimitive() != p) {
      throw new InvalidRecordException("expected type " + currentType + " but got "+ p);
    }
  }

  public void addInt(int value) {
    validate(Primitive.INT32);
    delegate.addInt(value);
  }

  public void addLong(long value) {
    validate(Primitive.INT64);
    delegate.addLong(value);
  }

  public void addString(String value) {
    validate(Primitive.STRING);
    delegate.addString(value);
  }

  public void addBoolean(boolean value) {
    validate(Primitive.BOOL);
    delegate.addBoolean(value);
  }

  public void addBinary(byte[] value) {
    validate(Primitive.BINARY);
    delegate.addBinary(value);
  }

  public void addFloat(float value) {
    validate(Primitive.FLOAT);
    delegate.addFloat(value);
  }

  public void addDouble(double value) {
    validate(Primitive.DOUBLE);
    delegate.addDouble(value);
  }

}
