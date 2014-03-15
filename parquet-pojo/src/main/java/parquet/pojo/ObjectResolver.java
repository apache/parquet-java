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
package parquet.pojo;

import parquet.hadoop.mapred.Container;
import parquet.io.api.Converter;
import parquet.pojo.converter.ObjectConverter;
import parquet.pojo.field.FieldUtils;
import parquet.pojo.writer.ObjectRecordWriter;
import parquet.pojo.writer.RecordWriter;
import parquet.schema.GroupType;
import parquet.schema.Type;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

class ObjectResolver extends Resolver {
  public ObjectResolver(
    Class clazz, Field fieldIfPresent, Class[] genericArgumentsIfPresent,
    Container parentContainerIfPresent
  ) {
    super(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent);
  }

  @Override
  public Type getType() {
    List<Type> types = new ArrayList<Type>();

    for (Resolver resolver : childResolvers()) {
      types.add(resolver.getType());
    }

    return new GroupType(Type.Repetition.OPTIONAL, name(clazz, fieldIfPresent), types);
  }

  @Override
  public RecordWriter getWriter() {
    return new ObjectRecordWriter(clazz);
  }

  @Override
  public Converter getConverter() {
    if (fieldIfPresent != null) {
      return new ObjectConverter(clazz, parentContainerIfPresent, fieldIfPresent);
    } else {
      return new ObjectConverter(clazz, parentContainerIfPresent, fieldIfPresent) {
        @Override
        public void end() {

        }
      };
    }
  }

  private List<Resolver> childResolvers() {
    List<Resolver> resolvers = new ArrayList<Resolver>();
    List<Field> fields = FieldUtils.getAllFields(clazz);

    Container c = (fieldIfPresent != null) ? parentContainerIfPresent : container;
    for (Field field : fields) {
      Class[] genericArguments = getGenericArgs(field);

      resolvers.add(
        new Resolver(
          field.getType(), field, genericArguments, c
        )
      );
    }

    return resolvers;
  }
}