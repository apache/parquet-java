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
import parquet.pojo.converter.array.ArrayConverter;
import parquet.pojo.writer.ArrayWriters;
import parquet.pojo.writer.RecordWriter;
import parquet.schema.GroupType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

class ArrayResolver extends Resolver {
  private Resolver componentResolver;
  private Class componentType;

  public ArrayResolver(
    Class clazz, Field fieldIfPresent, Class[] genericArgumentsIfPresent,
    Container parentContainerIfPresent
  ) {
    super(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent);
    componentType = clazz.getComponentType();
    componentResolver = new Resolver(componentType, null, null, null);
  }

  @Override
  public Type getType() {
    List<Type> types = new ArrayList<Type>();

    types.add(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "size"));

    Type type = componentResolver.getType();

    if (componentType.isPrimitive()) {
      types.add(type.changeRepetition(Type.Repetition.REPEATED));
    } else {
      types.add(new GroupType(Type.Repetition.REPEATED, "values", type));
    }

    return new GroupType(Type.Repetition.OPTIONAL, name(clazz, fieldIfPresent), types);
  }

  @Override
  public RecordWriter getWriter() {
    Class componentType = clazz.getComponentType();

    //can't use regular tree traversal since writers have the size + values writers combined
    if (componentType == boolean.class) {
      return new ArrayWriters.BoolArrayWriter();
    } else if (componentType == short.class) {
      return new ArrayWriters.ShortArrayWriter();
    } else if (componentType == char.class) {
      return new ArrayWriters.CharArrayWriter();
    } else if (componentType == int.class) {
      return new ArrayWriters.IntArrayWriter();
    } else if (componentType == long.class) {
      return new ArrayWriters.LongArrayWriter();
    } else if (componentType == float.class) {
      return new ArrayWriters.FloatArrayWriter();
    } else if (componentType == double.class) {
      return new ArrayWriters.DoubleArrayWriter();
    } else {
      return new ArrayWriters.ObjectArrayWriter(
        new Resolver(componentType, null, null, null).getWriter()
      );
    }
  }

  @Override
  public Converter getConverter() {
    Class componentType = clazz.getComponentType();

    if (fieldIfPresent != null) {
      return new ArrayConverter(componentType, fieldIfPresent, parentContainerIfPresent);
    } else {
      return new ArrayConverter(componentType, fieldIfPresent, parentContainerIfPresent) {
        @Override
        public void end() {

        }
      };
    }
  }
}