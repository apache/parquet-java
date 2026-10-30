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
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.VectorLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.TypeUtil;
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
  private VectorLogicalTypeAnnotation vectorType;
  private String vectorName;
  private long vectorCount;
  private int vectorDepth;

  /**
   * @param delegate the consumer to pass down the event to
   * @param schema   the schema to validate against
   */
  public ValidatingRecordConsumer(RecordConsumer delegate, MessageType schema) {
    TypeUtil.checkValidVectorSchema(schema);
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
    Type currentType = types.peek().asGroupType().getType(fields.peek());
    if (fieldValueCount.peek() == 0
        && currentType.isRepetition(Repetition.REQUIRED)
        && (currentType.getLogicalTypeAnnotation() instanceof VectorLogicalTypeAnnotation
            || vectorType != null && types.size() == vectorDepth + 1)) {
      throw new InvalidRecordException("required field is missing " + currentType);
    }
    delegate.endField(field, index);
    fieldValueCount.pop();
    previousField.pop();
    previousField.push(fields.pop());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void startGroup() {
    Type child = types.peek().asGroupType().getType(fields.peek());
    if (child.getLogicalTypeAnnotation() instanceof VectorLogicalTypeAnnotation) {
      int count = fieldValueCount.pop() + 1;
      fieldValueCount.push(count);
      if (count > 1) {
        throw new InvalidRecordException("repeated value when the type is not repeated in " + child);
      }
      vectorType = (VectorLogicalTypeAnnotation) child.getLogicalTypeAnnotation();
      vectorName = child.getName();
      vectorCount = 0;
      vectorDepth = types.size() + 1;
    } else if (vectorType != null && types.size() == vectorDepth) {
      vectorCount++;
      if (vectorCount > vectorType.getNumElements()) {
        throw new InvalidRecordException("Invalid VECTOR length for " + vectorName + ": expected "
            + vectorType.getNumElements() + ", got " + vectorCount);
      }
    }
    previousField.push(-1);
    types.push(child);
    delegate.startGroup();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endGroup() {
    boolean endingVector = vectorType != null && types.size() == vectorDepth;
    if (endingVector && vectorCount != vectorType.getNumElements()) {
      throw new InvalidRecordException("Invalid VECTOR length for " + vectorName + ": expected "
          + vectorType.getNumElements() + ", got " + vectorCount);
    }
    delegate.endGroup();
    validateMissingFields(types.peek().asGroupType().getFieldCount());
    types.pop();
    previousField.pop();
    if (endingVector) {
      vectorType = null;
      vectorName = null;
      vectorCount = 0;
      vectorDepth = 0;
    }
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
    if (vectorType != null
        && types.peek().asGroupType().getType(fields.peek()).getLogicalTypeAnnotation()
            instanceof Float16LogicalTypeAnnotation) {
      if (value.length() != 2) {
        throw new InvalidRecordException("Invalid FLOAT16 VECTOR element length: " + value.length());
      }
      if ((value.get2BytesLittleEndian() & 0x7c00) == 0x7c00) {
        throw new InvalidRecordException("Non-finite VECTOR element in " + vectorName);
      }
    }
    delegate.addBinary(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addFloat(float value) {
    validate(FLOAT);
    if (vectorType != null && !Float.isFinite(value)) {
      throw new InvalidRecordException("Non-finite VECTOR element in " + vectorName);
    }
    delegate.addFloat(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addDouble(double value) {
    validate(DOUBLE);
    if (vectorType != null && !Double.isFinite(value)) {
      throw new InvalidRecordException("Non-finite VECTOR element in " + vectorName);
    }
    delegate.addDouble(value);
  }
}
