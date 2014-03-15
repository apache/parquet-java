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
import parquet.pojo.converter.map.MapConverter;
import parquet.pojo.writer.MapWriter;
import parquet.pojo.writer.RecordWriter;
import parquet.schema.GroupType;
import parquet.schema.Type;

import java.lang.reflect.Field;

class MapResolver extends Resolver {
  private Resolver keyResolver;
  private Resolver valueResolver;

  public MapResolver(
    Class clazz, Field fieldIfPresent, Class[] genericArgumentsIfPresent,
    Container parentContainerIfPresent
  ) {
    super(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent);

    if (fieldIfPresent != null) {
      genericArgumentsIfPresent = getGenericArgs(fieldIfPresent);
    }

    keyResolver = new Resolver(genericArgumentsIfPresent[0], null, null, null);
    valueResolver = new Resolver(genericArgumentsIfPresent[1], null, null, null);
  }

  @Override
  public Type getType() {
    return new GroupType(
      Type.Repetition.OPTIONAL, name(clazz, fieldIfPresent),
      new GroupType(
        Type.Repetition.REPEATED, "values", keyResolver.getType().rename("key"),
        valueResolver.getType().rename("value")
      )
    );
  }

  @Override
  public RecordWriter getWriter() {
    return new MapWriter(fieldIfPresent, genericArgumentsIfPresent);
  }

  @Override
  public Converter getConverter() {
    if (fieldIfPresent != null) {
      return new MapConverter(clazz, fieldIfPresent, parentContainerIfPresent, genericArgumentsIfPresent);
    } else {
      return new MapConverter(clazz, fieldIfPresent, parentContainerIfPresent, genericArgumentsIfPresent) {
        @Override
        public void end() {

        }
      };
    }
  }
}