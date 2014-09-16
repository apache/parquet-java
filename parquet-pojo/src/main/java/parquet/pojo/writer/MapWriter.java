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
package parquet.pojo.writer;

import parquet.io.api.RecordConsumer;
import parquet.pojo.Resolver;
import parquet.pojo.field.FieldAccessor;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * A {@link RecordWriter} for types that implement {@link Map}
 * Writes a map as a series of repeated groups of keys and values, handles nulls correctly
 */
public class MapWriter implements RecordWriter {
  private final RecordWriter keyWriter;
  private final RecordWriter valueWriter;

  public MapWriter(Field field, Class... genericArguments) {
    if (genericArguments.length == 2) {
      keyWriter = Resolver.newResolver(genericArguments[0], null).getWriter();
      valueWriter = Resolver.newResolver(genericArguments[1], null).getWriter();
    } else {
      Type type = field.getGenericType();

      if (type instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Class keyClass = (Class) parameterizedType.getActualTypeArguments()[0];
        Class valueClass = (Class) parameterizedType.getActualTypeArguments()[1];

        keyWriter = Resolver.newResolver(keyClass, null).getWriter();
        valueWriter = Resolver.newResolver(valueClass, null).getWriter();
      } else {
        throw new IllegalStateException("Cannot use untyped maps.");
      }
    }
  }

  private void writeEntry(Object o, RecordConsumer recordConsumer) {
    Map.Entry entry = (Map.Entry) o;

    recordConsumer.startGroup();

    if (entry.getKey() != null) {
      recordConsumer.startField(null, 0);
      keyWriter.writeValue(entry.getKey(), recordConsumer);
      recordConsumer.endField(null, 0);
    }

    if (entry.getValue() != null) {
      recordConsumer.startField(null, 1);
      valueWriter.writeValue(entry.getValue(), recordConsumer);
      recordConsumer.endField(null, 1);
    }

    recordConsumer.endGroup();
  }

  @Override
  public void writeValue(Object value, RecordConsumer recordConsumer) {
    Map map = (Map) value;

    if (map.size() > 0) {
      //start values
      recordConsumer.startField(null, 0);

      for (Object o : map.entrySet()) {
        writeEntry(o, recordConsumer);
      }

      recordConsumer.endField(null, 0);
    }
  }

  @Override
  public void writeFromField(
    Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
  ) {
    Map map = (Map) fieldAccessor.get(parent);

    if (map == null) {
      return;
    }

    //start map
    recordConsumer.startField(null, index);
    recordConsumer.startGroup();
    recordConsumer.startField(null, 0);

    if (map.size() > 0) {
      for (Object o : map.entrySet()) {
        writeEntry(o, recordConsumer);
      }
    }

    recordConsumer.endField(null, 0);
    recordConsumer.endGroup();
    recordConsumer.endField(null, index);
  }
}