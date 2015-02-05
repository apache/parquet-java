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
package parquet.pig.convert;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.pig.PigSchemaConverter;
import parquet.pig.SchemaConversionException;
import parquet.schema.GroupType;

/**
 * Converts groups into Pig Maps
 *
 * @author Julien Le Dem
 *
 */
final class MapConverter extends GroupConverter {

  private final MapKeyValueConverter keyValue;
  private final ParentValueContainer parent;

  private Map<String, Object> buffer = new BufferMap();

  private Object currentKey;
  private Object currentValue;

  MapConverter(GroupType parquetSchema, FieldSchema pigSchema, ParentValueContainer parent, boolean numbersDefaultToZero, boolean columnIndexAccess) throws FrontendException {
    if (parquetSchema.getFieldCount() != 1) {
      throw new IllegalArgumentException("maps have only one field. " + parquetSchema);
    }
    this.parent = parent;
    keyValue = new MapKeyValueConverter(parquetSchema.getType(0).asGroupType(), pigSchema.schema.getField(0), numbersDefaultToZero, columnIndexAccess);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    if (fieldIndex != 0) {
      throw new IllegalArgumentException("maps have only one field. can't reach " + fieldIndex);
    }
    return keyValue;
  }

  /** runtime methods */

  @Override
  final public void start() {
    buffer.clear();
  }

  @Override
  public void end() {
    parent.add(new LinkedHashMap<String, Object>(buffer));
  }

  /**
   * to contain the values of the Map until we read them all
   * @author Julien Le Dem
   *
   */
  private static final class BufferMap extends AbstractMap<String, Object> {
    private List<Entry<String, Object>> entries = new ArrayList<Entry<String, Object>>();
    private Set<Entry<String, Object>> entrySet = new AbstractSet<Map.Entry<String,Object>>() {
      @Override
      public Iterator<java.util.Map.Entry<String, Object>> iterator() {
        return entries.iterator();
      }

      @Override
      public int size() {
        return entries.size();
      }
    };

    @Override
    public Tuple put(String key, Object value) {
      entries.add(new SimpleImmutableEntry<String, Object>(key, value));
      return null;
    }

    @Override
    public void clear() {
      entries.clear();
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
      return entrySet;
    }

  }

  /**
   * convert Key/Value groups into map entries
   *
   * @author Julien Le Dem
   *
   */
  final class MapKeyValueConverter extends GroupConverter {

    private final Converter keyConverter;
    private final Converter valueConverter;

    MapKeyValueConverter(GroupType parquetSchema, Schema.FieldSchema pigSchema, boolean numbersDefaultToZero, boolean columnIndexAccess) {
      if (parquetSchema.getFieldCount() != 2
          || !parquetSchema.getType(0).getName().equals("key")
          || !parquetSchema.getType(1).getName().equals("value")) {
        throw new IllegalArgumentException("schema does not match map key/value " + parquetSchema);
      }
      try {
        keyConverter = TupleConverter.newConverter(new PigSchemaConverter().convertField(parquetSchema.getType(0)).getField(0), 
            parquetSchema.getType(0), new ParentValueContainer() {
        void add(Object value) {
          currentKey = value;
        }
      }, numbersDefaultToZero, columnIndexAccess);
      } catch (FrontendException fe) {
        throw new SchemaConversionException("can't convert keytype "+ parquetSchema.getType(0), fe);
      }
      valueConverter = TupleConverter.newConverter(pigSchema, parquetSchema.getType(1), new ParentValueContainer() {
        void add(Object value) {
          currentValue = value;
        }
      }, numbersDefaultToZero, columnIndexAccess);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex == 0) {
        return keyConverter;
      } else if (fieldIndex == 1) {
        return valueConverter;
      }
      throw new IllegalArgumentException("only the key (0) and value (1) fields expected: " + fieldIndex);
    }

    /** runtime methods */

    @Override
    final public void start() {
      currentKey = null;
      currentValue = null;
    }

    @Override
    public void end() {
      buffer.put(currentKey.toString(), currentValue);
      currentKey = null;
      currentValue = null;
    }

  }

  /**
   * convert the key into a string
   *
   * @author Julien Le Dem
   *
   */
  final class StringKeyConverter extends PrimitiveConverter {

    @Override
    final public void addBinary(Binary value) {
      currentKey = value.toStringUsingUTF8();
    }

  }

}
