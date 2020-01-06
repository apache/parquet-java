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

package org.apache.parquet.cli.csv;

import org.apache.parquet.cli.util.RecordException;
import org.apache.parquet.cli.util.Schemas;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import java.util.List;

class RecordBuilder<E> {
  private final Schema schema;
  private final Class<E> recordClass;
  private final Schema.Field[] fields;
  private final int[] indexes; // Record position to CSV field position

  public RecordBuilder(Schema schema, Class<E> recordClass, List<String> header) {
    this.schema = schema;
    this.recordClass = recordClass;

    // initialize the index and field arrays
    fields = schema.getFields().toArray(new Schema.Field[0]);
    indexes = new int[fields.length];

    if (header != null) {
      for (int i = 0; i < fields.length; i += 1) {
        fields[i] = schema.getFields().get(i);
        indexes[i] = Integer.MAX_VALUE; // never present in the row
      }

      // there's a header in next
      for (int i = 0; i < header.size(); i += 1) {
        Schema.Field field = schema.getField(header.get(i));
        if (field != null) {
          indexes[field.pos()] = i;
        }
      }

    } else {
      // without a header, map to fields by position
      for (int i = 0; i < fields.length; i += 1) {
        fields[i] = schema.getFields().get(i);
        indexes[i] = i;
      }
    }
  }

  public E makeRecord(String[] fields, E reuse) {
    E record = reuse;
    if (record == null) {
      record = newRecordInstance();
    }

    if (record instanceof IndexedRecord) {
      fillIndexed((IndexedRecord) record, fields);
    } else {
      fillReflect(record, fields);
    }

    return record;
  }

  @SuppressWarnings("unchecked")
  private E newRecordInstance() {
    if (recordClass != GenericData.Record.class && !recordClass.isInterface()) {
      E record = (E) ReflectData.newInstance(recordClass, schema);
      if (record != null) {
        return record;
      }
    }
    return (E) new GenericData.Record(schema);
  }

  private void fillIndexed(IndexedRecord record, String[] data) {
    for (int i = 0; i < indexes.length; i += 1) {
      int index = indexes[i];
      record.put(i,
          makeValue(index < data.length ? data[index] : null, fields[i]));
    }
  }

  private void fillReflect(Object record, String[] data) {
    for (int i = 0; i < indexes.length; i += 1) {
      Schema.Field field = fields[i];
      int index = indexes[i];
      Object value = makeValue(index < data.length ? data[index] : null, field);
      ReflectData.get().setField(record, field.name(), i, value);
    }
  }

  private static Object makeValue(String string, Schema.Field field) {
    try {
      Object value = makeValue(string, field.schema());
      if (value != null || Schemas.nullOk(field.schema())) {
        return value;
      } else {
        // this will fail if there is no default value
        return ReflectData.get().getDefaultValue(field);
      }
    } catch (RecordException e) {
      // add the field name to the error message
      throw new RecordException(String.format(
          "Cannot convert field %s", field.name()), e);
    } catch (NumberFormatException e) {
      throw new RecordException(String.format(
          "Field %s: value not a %s: '%s'",
          field.name(), field.schema(), string), e);
    } catch (AvroRuntimeException e) {
      throw new RecordException(String.format(
          "Field %s: cannot make %s value: '%s'",
          field.name(), field.schema(), string), e);
    }
  }

  /**
   * Returns a the value as the first matching schema type or null.
   *
   * Note that if the value may be null even if the schema does not allow the
   * value to be null.
   *
   * @param string a String representation of the value
   * @param schema a Schema
   * @return the string coerced to the correct type from the schema or null
   */
  private static Object makeValue(String string, Schema schema) {
    if (string == null) {
      return null;
    }

    try {
      switch (schema.getType()) {
        case BOOLEAN:
          return Boolean.valueOf(string);
        case STRING:
          return string;
        case FLOAT:
          return Float.valueOf(string);
        case DOUBLE:
          return Double.valueOf(string);
        case INT:
          return Integer.valueOf(string);
        case LONG:
          return Long.valueOf(string);
        case ENUM:
          // TODO: translate to enum class
          if (schema.hasEnumSymbol(string)) {
            return string;
          } else {
            try {
              return schema.getEnumSymbols().get(Integer.parseInt(string));
            } catch (IndexOutOfBoundsException ex) {
              return null;
            }
          }
        case UNION:
          Object value = null;
          for (Schema possible : schema.getTypes()) {
            value = makeValue(string, possible);
            if (value != null) {
              return value;
            }
          }
          return null;
        case NULL:
          return null;
        default:
          // FIXED, BYTES, MAP, ARRAY, RECORD are not supported
          throw new RecordException(
              "Unsupported field type:" + schema.getType());
      }
    } catch (NumberFormatException e) {
      // empty string is considered null for numeric types
      if (string.isEmpty()) {
        return null;
      } else {
        throw e;
      }
    }
  }
}
