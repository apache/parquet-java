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
package parquet.example.data.simple.convert;

import parquet.example.data.Group;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.schema.GroupType;
import parquet.schema.Type;

class SimpleGroupConverter extends GroupConverter {
  private final SimpleGroupConverter parent;
  private final int index;
  protected Group current;
  private Converter[] converters;

  SimpleGroupConverter(SimpleGroupConverter parent, int index, GroupType schema) {
    this.parent = parent;
    this.index = index;

    converters = new Converter[schema.getFieldCount()];

    for (int i = 0; i < converters.length; i++) {
      final Type type = schema.getType(i);
      if (type.isPrimitive()) {
        converters[i] = new SimplePrimitiveConverter(this, i);
      } else {
        converters[i] = new SimpleGroupConverter(this, i, type.asGroupType());
      }

    }
  }

  @Override
  public void start() {
    current = parent.getCurrentRecord().addGroup(index);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void end() {
  }

  public Group getCurrentRecord() {
    return current;
  }
}