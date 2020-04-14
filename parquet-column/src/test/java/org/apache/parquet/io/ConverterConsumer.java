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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class ConverterConsumer extends RecordConsumer {

  private final GroupConverter root;
  private final MessageType schema;

  private Deque<GroupConverter> path = new ArrayDeque<>();
  private Deque<Type> typePath = new ArrayDeque<>();
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
      currentPrimitive = current.getConverter(index).asPrimitiveConverter();
    } else {
      current = current.getConverter(index).asGroupConverter();
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
  public void addBinary(Binary value) {
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    // do nothing
  }

}
