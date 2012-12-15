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
package redelm.io;

import java.util.ArrayDeque;
import java.util.Deque;

import redelm.Log;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType.Primitive;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

/**
 * Wraps a record consumer
 * Validates the record written aainst the schema and pass down the event to the wrapped consumer
 *
 * @author Julien Le Dem
 *
 * @param T the materialized record
 */
public class ValidatingRecordConsumer<T> extends RecordConsumer<T> {
  private static final Log LOG = Log.getLog(ValidatingRecordConsumer.class);
  private static final boolean DEBUG = Log.DEBUG;

  private final RecordConsumer<T> delegate;

  private Deque<Type> types = new ArrayDeque<Type>();
  private Deque<Integer> fields = new ArrayDeque<Integer>();
  private Deque<Integer> previousField = new ArrayDeque<Integer>();
  private Deque<Integer> fieldValueCount = new ArrayDeque<Integer>();

  /**
   *
   * @param delegate the consumer to pass down the event to
   * @param schema the schema to validate against
   */
  public ValidatingRecordConsumer(RecordConsumer<T> delegate, MessageType schema) {
    this.delegate = delegate;
    this.types.push(schema);
  }

  /**
   * {@inheritDoc}
   */
  public void startMessage() {
    previousField.push(-1);
    delegate.startMessage();
  }

  /**
   * {@inheritDoc}
   */
  public void endMessage() {
    delegate.endMessage();
    validateMissingFields(types.peek().asGroupType().getFieldCount());
    previousField.pop();
  }

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
  public void endField(String field, int index) {
    delegate.endField(field, index);
    fieldValueCount.pop();
    previousField.push(fields.pop());
  }

  /**
   * {@inheritDoc}
   */
  public void startGroup() {
    previousField.push(-1);
    types.push(types.peek().asGroupType().getType(fields.peek()));
    delegate.startGroup();
  }

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
  public void addInteger(int value) {
    validate(Primitive.INT32);
    delegate.addInteger(value);
  }

  /**
   * {@inheritDoc}
   */
  public void addLong(long value) {
    validate(Primitive.INT64);
    delegate.addLong(value);
  }

  /**
   * {@inheritDoc}
   */
  public void addString(String value) {
    validate(Primitive.STRING);
    delegate.addString(value);
  }

  /**
   * {@inheritDoc}
   */
  public void addBoolean(boolean value) {
    validate(Primitive.BOOLEAN);
    delegate.addBoolean(value);
  }

  /**
   * {@inheritDoc}
   */
  public void addBinary(byte[] value) {
    validate(Primitive.BINARY);
    delegate.addBinary(value);
  }

  /**
   * {@inheritDoc}
   */
  public void addFloat(float value) {
    validate(Primitive.FLOAT);
    delegate.addFloat(value);
  }

  /**
   * {@inheritDoc}
   */
  public void addDouble(double value) {
    validate(Primitive.DOUBLE);
    delegate.addDouble(value);
  }

  @Override
  public T getCurrentRecord() {
    return delegate.getCurrentRecord();
  }

}
