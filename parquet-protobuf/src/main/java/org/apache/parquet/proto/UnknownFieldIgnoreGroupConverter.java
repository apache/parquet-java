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
package org.apache.parquet.proto;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/**
 * Ignore unknown fields of group type..
 */
public class UnknownFieldIgnoreGroupConverter extends GroupConverter {

  private final Converter[] fields;

  public UnknownFieldIgnoreGroupConverter(GroupType messageField) {
    fields = new Converter[messageField.getFieldCount()];
    int fieldIndex = 0;
    for (Type fieldType : messageField.getFields()) {
      fields[fieldIndex++] = fieldType.isPrimitive() ? UnknownFieldIgnorePrimitiveConverter.INSTANCE
        : new UnknownFieldIgnoreGroupConverter(fieldType.asGroupType());
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return fields[fieldIndex];
  }

  @Override
  public void start() {

  }

  @Override
  public void end() {

  }
}
