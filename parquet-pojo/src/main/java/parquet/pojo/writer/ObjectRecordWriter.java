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
import parquet.pojo.field.DelegatingFieldAccessor;
import parquet.pojo.field.FieldAccessor;
import parquet.pojo.field.FieldUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * A writer for types that are not primitives or maps / arrays / lists
 * Iterates through its fields and generates {@link RecordWriter} for them, then uses those to decompose the given object.
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
public class ObjectRecordWriter implements RecordWriter {
  private final RecordWriter[] recordWriters;
  private final FieldAccessor[] fieldAccessors;

  public ObjectRecordWriter(Class clazz) {
    List<Field> fields = FieldUtils.getAllFields(clazz);
    recordWriters = new RecordWriter[fields.size()];
    fieldAccessors = new FieldAccessor[fields.size()];

    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      recordWriters[i] = Resolver.newResolver(field.getType(), field).getWriter();
      fieldAccessors[i] = new DelegatingFieldAccessor(field);
    }
  }

  @Override
  public void writeValue(Object value, RecordConsumer recordConsumer) {
    if (value == null) {
      return;
    }

    recordConsumer.startGroup();
    for (int i = 0; i < recordWriters.length; i++) {
      recordWriters[i].writeFromField(value, recordConsumer, i, fieldAccessors[i]);
    }
    recordConsumer.endGroup();
  }

  @Override
  public void writeFromField(
    Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
  ) {
    Object value = fieldAccessor.get(parent);

    if (value == null) {
      return;
    }

    recordConsumer.startField(null, index);
    recordConsumer.startGroup();

    for (int i = 0; i < recordWriters.length; i++) {
      recordWriters[i].writeFromField(value, recordConsumer, i, fieldAccessors[i]);
    }

    recordConsumer.endGroup();
    recordConsumer.endField(null, index);
  }
}