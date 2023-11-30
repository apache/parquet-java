/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.io;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a record consumer
 * Validates the record written against the schema and pass down the event to the wrapped consumer
 */
public class ValidatingRecordConsumer extends RecordConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(ValidatingRecordConsumer.class);

  private final RecordConsumer delegate;

  private Deque<Type> types = new ArrayDeque<>();
  private Deque<Integer> fields = new ArrayDeque<>();
  private Deque<Integer> previousField = new ArrayDeque<>();
  private Deque<Integer> fieldValueCount = new ArrayDeque<>();

  /**
   * @param delegate the consumer to pass down the event to
   * @param schema   the schema to validate against
   */
  public ValidatingRecordConsumer(RecordConsumer delegate, MessageType schema) {
    this.delegate = delegate;
    this.types.push(schema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void startMessage() {
    previousField.push(-1);
    delegate.startMessage();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endMessage() {
    delegate.endMessage();
    validateMissingFields(types.peek().asGroupType().getFieldCount());
    previousField.pop();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void startField(String field, int index) {
    if (index <= previousField.peek()) {
      throw new InvalidRecordException("fields must be added in order " + field + " index " + index
          + " is before previous field " + previousField.peek());
    }
    validateMissingFields(index);
    fields.push(index);
    fieldValueCount.push(0);
    delegate.startField(field, index);
  }

  private void validateMissingFields(int index) {
    for (int i = previousField.peek() + 1; i < index; i++) {
      Type type = types.peek().asGroupType().getType(i);
      if (type.isRepetition(Repetition.REQUIRED)) {
        throw new InvalidRecordException("required field is missing " + type);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endField(String field, int index) {
    delegate.endField(field, index);
    fieldValueCount.pop();
    previousField.push(fields.pop());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void startGroup() {
    previousField.push(-1);
    types.push(types.peek().asGroupType().getType(fields.peek()));
    delegate.startGroup();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endGroup() {
    delegate.endGroup();
    validateMissingFields(types.peek().asGroupType().getFieldCount());
    types.pop();
    previousField.pop();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    delegate.flush();
  }

  private void validate(PrimitiveTypeName p) {
    Type currentType = types.peek().asGroupType().getType(fields.peek());
    int c = fieldValueCount.pop() + 1;
    fieldValueCount.push(c);
    LOG.debug("validate {} for {}", p, currentType.getName());
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
        throw new InvalidRecordException(
            "unknown repetition " + currentType.getRepetition() + " in " + currentType);
    }
    if (!currentType.isPrimitive() || currentType.asPrimitiveType().getPrimitiveTypeName() != p) {
      throw new InvalidRecordException("expected type " + p + " but got " + currentType);
    }
  }

  private void validate(PrimitiveTypeName... ptypes) {
    Type currentType = types.peek().asGroupType().getType(fields.peek());
    int c = fieldValueCount.pop() + 1;
    fieldValueCount.push(c);
    if (LOG.isDebugEnabled()) LOG.debug("validate " + Arrays.toString(ptypes) + " for " + currentType.getName());
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
        throw new InvalidRecordException(
            "unknown repetition " + currentType.getRepetition() + " in " + currentType);
    }
    if (!currentType.isPrimitive()) {
      throw new InvalidRecordException("expected type in " + Arrays.toString(ptypes) + " but got " + currentType);
    }
    for (PrimitiveTypeName p : ptypes) {
      if (currentType.asPrimitiveType().getPrimitiveTypeName() == p) {
        return; // type is valid
      }
    }
    throw new InvalidRecordException("expected type in " + Arrays.toString(ptypes) + " but got " + currentType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addInteger(int value) {
    validate(INT32);
    delegate.addInteger(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addLong(long value) {
    validate(INT64);
    delegate.addLong(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addBoolean(boolean value) {
    validate(BOOLEAN);
    delegate.addBoolean(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addBinary(Binary value) {
    validate(BINARY, INT96, FIXED_LEN_BYTE_ARRAY);
    delegate.addBinary(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addFloat(float value) {
    validate(FLOAT);
    delegate.addFloat(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addDouble(double value) {
    validate(DOUBLE);
    delegate.addDouble(value);
  }
}
