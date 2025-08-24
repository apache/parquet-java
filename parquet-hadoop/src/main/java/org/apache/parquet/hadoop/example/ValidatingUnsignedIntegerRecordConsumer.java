/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.example;

import java.util.Stack;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * A RecordConsumer that validates unsigned integer values to ensure they comply
 * with the Parquet specification.
 *
 * This consumer wraps another RecordConsumer and validates that values written
 * to unsigned integer fields (UINT_8, UINT_16, UINT_32, UINT_64) are within
 * the valid unsigned range before delegating to the wrapped consumer.
 */
public class ValidatingUnsignedIntegerRecordConsumer extends RecordConsumer {

  private final RecordConsumer delegate;
  private final MessageType schema;
  private final Stack<Type> types = new Stack<>();
  private final Stack<Integer> fields = new Stack<>();

  public ValidatingUnsignedIntegerRecordConsumer(RecordConsumer delegate, MessageType schema) {
    this.delegate = delegate;
    this.schema = schema;
  }

  @Override
  public void startMessage() {
    types.push(schema);
    delegate.startMessage();
  }

  @Override
  public void endMessage() {
    types.pop();
    delegate.endMessage();
  }

  @Override
  public void startField(String field, int index) {
    fields.push(index);

    if (!types.isEmpty()) {
      Type parentType = types.peek();
      if (parentType.asGroupType() != null
          && index < parentType.asGroupType().getFieldCount()) {
        Type fieldType = parentType.asGroupType().getType(index);
        types.push(fieldType);
      }
    }

    delegate.startField(field, index);
  }

  @Override
  public void endField(String field, int index) {
    if (!types.isEmpty() && !fields.isEmpty()) {
      types.pop();
      fields.pop();
    }
    delegate.endField(field, index);
  }

  @Override
  public void startGroup() {
    delegate.startGroup();
  }

  @Override
  public void endGroup() {
    delegate.endGroup();
  }

  @Override
  public void addInteger(int value) {
    validateUnsignedInteger(value);
    delegate.addInteger(value);
  }

  @Override
  public void addLong(long value) {
    validateUnsignedLong(value);
    delegate.addLong(value);
  }

  @Override
  public void addBoolean(boolean value) {
    delegate.addBoolean(value);
  }

  @Override
  public void addBinary(org.apache.parquet.io.api.Binary value) {
    delegate.addBinary(value);
  }

  @Override
  public void addFloat(float value) {
    delegate.addFloat(value);
  }

  @Override
  public void addDouble(double value) {
    delegate.addDouble(value);
  }

  private void validateUnsignedInteger(int value) {
    Type currentType = getCurrentFieldType();
    if (currentType != null && currentType.isPrimitive()) {
      LogicalTypeAnnotation logicalType = currentType.asPrimitiveType().getLogicalTypeAnnotation();
      if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
        LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
            (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
        if (!intType.isSigned()) {
          switch (intType.getBitWidth()) {
            case 8:
              if (value < 0 || value > 255) {
                throw new InvalidRecordException("Value " + value
                    + " is out of range for UINT_8 (0-255) in field " + currentType.getName());
              }
              break;
            case 16:
              if (value < 0 || value > 65535) {
                throw new InvalidRecordException("Value " + value
                    + " is out of range for UINT_16 (0-65535) in field " + currentType.getName());
              }
              break;
            case 32:
            case 64:
              if (value < 0) {
                throw new InvalidRecordException("Negative value " + value
                    + " is not allowed for unsigned integer type " + currentType.getName());
              }
              break;
          }
        }
      }
    }
  }

  private void validateUnsignedLong(long value) {
    Type currentType = getCurrentFieldType();
    if (currentType != null && currentType.isPrimitive()) {
      LogicalTypeAnnotation logicalType = currentType.asPrimitiveType().getLogicalTypeAnnotation();
      if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
        LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
            (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
        if (!intType.isSigned()) {
          if (value < 0) {
            throw new InvalidRecordException("Negative value " + value
                + " is not allowed for unsigned integer type " + currentType.getName());
          }
        }
      }
    }
  }

  private Type getCurrentFieldType() {
    if (fields.isEmpty() || types.isEmpty()) {
      return null;
    }

    Type parentType = types.size() > 1 ? types.get(types.size() - 2) : schema;
    if (parentType.isPrimitive()) {
      return parentType;
    }

    if (parentType.asGroupType() != null) {
      int fieldIndex = fields.peek();
      if (fieldIndex >= 0 && fieldIndex < parentType.asGroupType().getFieldCount()) {
        return parentType.asGroupType().getType(fieldIndex);
      }
    }
    return null;
  }
}
