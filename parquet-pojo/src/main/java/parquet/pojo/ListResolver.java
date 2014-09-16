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
import parquet.pojo.converter.list.ListConverter;
import parquet.pojo.writer.ListWriter;
import parquet.pojo.writer.RecordWriter;
import parquet.schema.GroupType;
import parquet.schema.Type;

import java.lang.reflect.Field;

/**
 * Resolves classes of type {@link java.util.List}, only works on lists with generic declarations, through fields or through configuration
 */
class ListResolver extends Resolver {
  private Resolver valuesResolver;

  public ListResolver(
    Class clazz, Field fieldIfPresent, Class[] genericArgumentsIfPresent,
    Container parentContainerIfPresent
  ) {
    super(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent);

    if (fieldIfPresent != null) {
      genericArgumentsIfPresent = getGenericArgs(fieldIfPresent);
    }

    //check if we've been provided a valid generic type arg
    if(genericArgumentsIfPresent.length != 1) {
      throw new BadGenericsConfigurationException(
        "List type requires 1 generic argument, provided through a field declaration or through configuration."
      );
    }

    valuesResolver = new Resolver(genericArgumentsIfPresent[0], null, null, null);
  }

  @Override
  public Type getType() {
    return new GroupType(
      Type.Repetition.OPTIONAL, name(clazz, fieldIfPresent),
      new GroupType(Type.Repetition.REPEATED, name(clazz, fieldIfPresent) + "_values", valuesResolver.getType())
    );
  }

  @Override
  public RecordWriter getWriter() {
    return new ListWriter(fieldIfPresent, genericArgumentsIfPresent);
  }

  @Override
  public Converter getConverter() {
    if (fieldIfPresent != null) {
      return new ListConverter(clazz, fieldIfPresent, parentContainerIfPresent, genericArgumentsIfPresent);
    } else {
      return new ListConverter(clazz, fieldIfPresent, parentContainerIfPresent, genericArgumentsIfPresent) {
        @Override
        public void end() {

        }
      };
    }
  }
}