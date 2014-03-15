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
 * Helper class for writing at the root of an object hierarchy
 *
 * @author Jason Ruckman
 */
public class RootWriter {
  private final WriterDelegate writerDelegate;

  private static interface WriterDelegate {
    void write(Object value, RecordConsumer recordConsumer);
  }

  public RootWriter(Class clazz, Class... genericArgumentsIfPresent) {
    if (FieldUtils.isConsideredPrimitive(clazz)) {
      final RecordWriter recordWriter = Resolver.newResolver(clazz, null).getWriter();

      //primitive writers don't write their fields
      writerDelegate = new WriterDelegate() {
        @Override
        public void write(Object value, RecordConsumer recordConsumer) {
          recordConsumer.startField(null, 0);
          recordWriter.writeValue(value, recordConsumer);
          recordConsumer.endField(null, 0);
        }
      };
    } else if (clazz.isArray() || FieldUtils.isMap(clazz) || FieldUtils.isList(clazz)) {
      final RecordWriter recordWriter = Resolver.newResolver(clazz, null, genericArgumentsIfPresent).getWriter();

      //maps / arrays / list writers will write their child fields (key, value)
      writerDelegate = new WriterDelegate() {
        @Override
        public void write(Object value, RecordConsumer recordConsumer) {
          recordWriter.writeValue(value, recordConsumer);
        }
      };
    } else {
      List<Field> fields = FieldUtils.getAllFields(clazz);

      final RecordWriter[] recordWriters = new RecordWriter[fields.size()];
      final FieldAccessor[] fieldAccessors = new FieldAccessor[fields.size()];
      final int size = fields.size();

      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        recordWriters[i] = Resolver.newResolver(field.getType(), field).getWriter();
        fieldAccessors[i] = new DelegatingFieldAccessor(field);
      }

      //pojos need field decomposition
      writerDelegate = new WriterDelegate() {
        @Override
        public void write(Object value, RecordConsumer recordConsumer) {
          for (int i = 0; i < size; i++) {
            recordWriters[i].writeFromField(value, recordConsumer, i, fieldAccessors[i]);
          }
        }
      };
    }
  }

  public void write(Object value, RecordConsumer recordConsumer) {
    writerDelegate.write(value, recordConsumer);
  }
}