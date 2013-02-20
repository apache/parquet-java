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
package parquet.pig.converter;

import java.util.HashMap;
import java.util.Map;


import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import parquet.pig.TupleConversionException;
import parquet.schema.GroupType;

public class MapConverter extends Converter {

  private Map<String, Tuple> currentMap;
  private MapKeyValueConverter keyValue;

  MapConverter(GroupType redelmSchema, FieldSchema pigSchema, Converter parent) throws FrontendException {
    super(parent);
    keyValue = new MapKeyValueConverter(redelmSchema.getType(0).asGroupType(), pigSchema.schema.getField(0).schema, this);
  }

  @Override
  public void start() {
    currentMap = new HashMap<String, Tuple>();
  }

  @Override
  public void startField(String field, int index) {
    assert index == 0;
  }

  @Override
  public void endField(String field, int index) {
    assert index == 0;
  }

  @Override
  public Converter startGroup() {
    return keyValue;
  }

  @Override
  public void endGroup() {
    currentMap.put(keyValue.getKey(), keyValue.get());
  }

  @Override
  public Map<String, Tuple> get() {
    return currentMap;
  }

  @Override
  public void set(Object value) {
    throw new TupleConversionException("maps contain only key/value groups, not primitive value: "+ value);
  }

  @Override
  public void toString(String indent, StringBuffer sb) {
    sb.append(indent).append(getClass().getSimpleName()).append("{\n");
    keyValue.toString(" " + indent, sb);
    sb.append("\n").append(indent).append("}");
  }

}
