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
package redelm.pig.converter;

import java.io.UnsupportedEncodingException;

import redelm.schema.GroupType;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class MapKeyValueConverter extends Converter {
  private TupleConverter value;
  private int currentField;
  private String currentKey;

  MapKeyValueConverter(GroupType redelmSchema, Schema pigSchema, MapConverter parent) throws FrontendException {
    super(parent);
    assert redelmSchema.getFieldCount() == 2;
    assert redelmSchema.getType(0).getName().equals("key");
    assert redelmSchema.getType(1).getName().equals("value");
    value = new TupleConverter(redelmSchema.getType(1).asGroupType(), pigSchema, this);
  }

  @Override
  public void start() {
    this.currentKey = null;
  }

  @Override
  public void startField(String field, int index) {
    currentField = index;
  }

  @Override
  public void endField(String field, int index) {
    assert currentField == index;
    currentField = -1;
  }

  @Override
  public Converter startGroup() {
    assert currentField == 1;
    return value;
  }

  @Override
  public void endGroup() {
  }

  @Override
  public Tuple get() {
    return value.get();
  }

  @Override
  public void set(Object value) {
    assert currentField == 0;
    try {
      currentKey = new String((byte[])value, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public String getKey() {
    return currentKey;
  }

}
