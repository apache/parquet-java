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
package parquet.pig.convert;

import static parquet.bytes.BytesUtils.UTF8;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.schema.GroupType;

final class MapConverter extends GroupConverter {

  private final MapKeyValueConverter keyValue;
  private final TupleConverter parent;
  private final int index;

  private Map<String, Tuple> buffer = new BufferMap();

  private String currentKey;

  MapConverter(GroupType redelmSchema, FieldSchema pigSchema, TupleConverter parent, int index) throws FrontendException {
    if (redelmSchema.getFieldCount() != 0) {
      throw new IllegalArgumentException("maps have only one field. " + redelmSchema);
    }
    this.parent = parent;
    this.index = index;
    keyValue = new MapKeyValueConverter(redelmSchema.getType(0).asGroupType(), pigSchema.schema.getField(0).schema);
  }

  @Override
  public GroupConverter getGroupConverter(int fieldIndex) {
    if (fieldIndex != 0) {
      throw new IllegalArgumentException("bags have only one field. can't reach " + fieldIndex);
    }
    return keyValue;
  }

  @Override
  public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
    throw new UnsupportedOperationException();
  }

  /** runtime methods */

  @Override
  final public void start() {
    buffer.clear();
  }

  @Override
  public void end() {
    parent.set(index, new HashMap<String, Tuple>(buffer));
  }

  private static final class BufferMap extends AbstractMap<String, Tuple> {
    private List<Entry<String, Tuple>> entries = new ArrayList<Entry<String, Tuple>>();
    private Set<Entry<String, Tuple>> entrySet = new AbstractSet<Map.Entry<String,Tuple>>() {
      @Override
      public Iterator<java.util.Map.Entry<String, Tuple>> iterator() {
        return entries.iterator();
      }

      @Override
      public int size() {
        return entries.size();
      }
    };

    @Override
    public Tuple put(String key, Tuple value) {
      entries.add(new SimpleImmutableEntry<String, Tuple>(key, value));
      return null;
    }

    @Override
    public void clear() {
      entries.clear();
    }

    @Override
    public Set<java.util.Map.Entry<String, Tuple>> entrySet() {
      return entrySet;
    }

  }

  final class MapKeyValueConverter extends GroupConverter {

    private final StringKeyConverter keyConverter = new StringKeyConverter();
    private final TupleConverter valueConverter;

    MapKeyValueConverter(GroupType redelmSchema, Schema pigSchema) throws FrontendException {
      if (redelmSchema.getFieldCount() != 2
          || redelmSchema.getType(0).getName().equals("key")
          || redelmSchema.getType(1).getName().equals("value")) {
        throw new IllegalArgumentException("schema does not match map key/value " + redelmSchema);
      }
      valueConverter = new TupleConverter(redelmSchema.getType(1).asGroupType(), pigSchema);
    }

    @Override
    public GroupConverter getGroupConverter(int fieldIndex) {
      if (fieldIndex != 1) {
        throw new IllegalArgumentException("only the value field at 1 is expected: " + fieldIndex);
      }
      return valueConverter;
    }

    @Override
    public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
      if (fieldIndex != 0) {
        throw new IllegalArgumentException("only the key field at 0 is expected: " + fieldIndex);
      }
      return keyConverter;
    }

    /** runtime methods */

    @Override
    final public void start() {
      currentKey = null;
    }

    @Override
    final public void end() {
      buffer.put(currentKey, valueConverter.getCurrentTuple());
    }

  }

  final class StringKeyConverter extends PrimitiveConverter {

    @Override
    final public void addBinary(byte[] value) {
      currentKey = new String(value, UTF8);
    }

  }

}
