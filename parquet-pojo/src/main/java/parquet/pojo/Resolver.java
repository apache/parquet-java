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
import parquet.pojo.field.FieldUtils;
import parquet.pojo.writer.RecordWriter;
import parquet.schema.Type;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;

import static parquet.pojo.field.FieldUtils.isList;
import static parquet.pojo.field.FieldUtils.isMap;

public class Resolver {
  protected final Class clazz;
  protected final Field fieldIfPresent;
  protected final Class[] genericArgumentsIfPresent;
  protected final Container parentContainerIfPresent;
  protected final Container container = new Container();

  public Resolver(
    Class clazz, Field fieldIfPresent, Class[] genericArgumentsIfPresent, Container parentContainerIfPresent
  ) {
    this.clazz = clazz;
    this.fieldIfPresent = fieldIfPresent;
    this.genericArgumentsIfPresent = genericArgumentsIfPresent;
    this.parentContainerIfPresent = parentContainerIfPresent;
  }

  /**
   * Name a class / field pairing.  If the field is present, use its name, otherwise use the class name. Unless its a map / list / array, then just use simple versions of those names
   *
   * @param clazz
   * @param fieldIfPresent
   * @return the name
   */
  protected String name(Class clazz, Field fieldIfPresent) {
    if (fieldIfPresent != null) {
      return fieldIfPresent.getName();
    }

    if (isMap(clazz)) {
      return "map";
    } else if (isList(clazz)) {
      return "list";
    } else if (clazz.isArray()) {
      return "array";
    } else {
      return clazz.getName();
    }
  }

  public Type getType() {
    return newResolver(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent).getType();
  }

  public RecordWriter getWriter() {
    return newResolver(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent).getWriter();
  }

  public Converter getConverter() {
    return newResolver(
      clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent
    ).getConverter();
  }

  public static Resolver newResolver(
    Class clazz, Field fieldIfPresent
  ) {
    return newResolver(clazz, fieldIfPresent, new Class[0], null);
  }

  public static Resolver newResolver(
    Class clazz, Field fieldIfPresent, Class[] genericArgumentsIfPresent
  ) {
    return newResolver(clazz, fieldIfPresent, genericArgumentsIfPresent, null);
  }

  public static Resolver newResolver(
    Class clazz, Field fieldIfPresent, Class[] genericArgumentsIfPresent, Container parentContainerIfPresent
  ) {
    if (FieldUtils.isConsideredPrimitive(clazz)) {
      return new PrimitiveResolver(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent);
    } else if (clazz.isArray()) {
      return new ArrayResolver(
        clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent
      );
    } else if (FieldUtils.isMap(clazz)) {
      return new MapResolver(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent);
    } else if (FieldUtils.isList(clazz)) {
      return new ListResolver(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent);
    } else {
      return new ObjectResolver(
        clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent
      );
    }
  }

  public Class[] getGenericArgs(Field field) {
    java.lang.reflect.Type type = field.getGenericType();
    Class[] genericArguments = new Class[0];

    if (type instanceof ParameterizedType) {
      java.lang.reflect.Type[] types = ((ParameterizedType) type).getActualTypeArguments();
      genericArguments = new Class[types.length];
      for (int i = 0; i < types.length; i++) {
        genericArguments[i] = (Class) types[i];
      }
    }

    return genericArguments;
  }
}