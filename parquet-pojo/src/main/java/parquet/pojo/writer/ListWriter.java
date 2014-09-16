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
import java.util.List;

/**
 * {@link RecordWriter} for classes that implement {@link java.util.List}
 */
public class ListWriter<T> implements RecordWriter<List<T>> {
  private final RecordWriter<T> valuesWriter;

  public ListWriter(Field field, Class... genericArguments) {
    //check if we've explicitly passed the generic arguments down
    if (genericArguments.length == 1) {
      valuesWriter = Resolver.newResolver(genericArguments[0], null, genericArguments).getWriter();
    } else {
      Type type = field.getGenericType();

      if (type instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        valuesWriter = new Resolver((Class) parameterizedType.getActualTypeArguments()[0], null, null, null).getWriter();
      } else {
        throw new IllegalStateException(String.format("Cannot use untyped lists. Offending field: %s", field));
      }
    }
  }

  @Override
  public void writeValue(List<T> list, RecordConsumer recordConsumer) {
    if (list == null) {
      return;
    }

    recordConsumer.startField(null, 0);

    for (T v : list) {
      recordConsumer.startGroup();

      if (v != null) {
        recordConsumer.startField(null, 0);
        valuesWriter.writeValue(v, recordConsumer);
        recordConsumer.endField(null, 0);
      }

      recordConsumer.endGroup();
    }

    recordConsumer.endField(null, 0);
  }

  @Override
  public void writeFromField(
    Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
  ) {
    List<T> list = (List<T>) fieldAccessor.get(parent);

    if (list == null) {
      return;
    }
    //start the containing field
    recordConsumer.startField(null, index);
    //start list group
    recordConsumer.startGroup();

    recordConsumer.startField(null, 0);
    for (T v : list) {
      recordConsumer.startGroup();
      if(v != null) {
        recordConsumer.startField(null, 0);
        valuesWriter.writeValue(v, recordConsumer);
        recordConsumer.endField(null, 0);
      }
      recordConsumer.endGroup();
    }
    recordConsumer.endField(null, 0);

    recordConsumer.endGroup();
    recordConsumer.endField(null, index);
  }
}