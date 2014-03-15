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
import parquet.pojo.converter.PrimitiveConverters;
import parquet.pojo.writer.PrimitiveWriters;
import parquet.pojo.writer.RecordWriter;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.lang.reflect.Field;

class PrimitiveResolver extends Resolver {
  public PrimitiveResolver(
    Class clazz, Field fieldIfPresent, Class[] genericArgumentsIfPresent,
    Container parentContainerIfPresent
  ) {
    super(clazz, fieldIfPresent, genericArgumentsIfPresent, parentContainerIfPresent);
  }

  @Override
  public Type getType() {
    String name = name(clazz, fieldIfPresent);
    Type.Repetition repetition = (clazz.isPrimitive()) ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL;

    if (clazz == boolean.class || clazz == Boolean.class) {
      return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BOOLEAN, name);
    } else if (clazz == char.class || clazz == Character.class || clazz == byte.class || clazz == Byte.class ||
      clazz == short.class || clazz == Short.class || clazz == int.class || clazz == Integer.class) {
      return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, name);
    } else if (clazz == long.class || clazz == Long.class) {
      return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, name);
    } else if (clazz == float.class || clazz == Float.class) {
      return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.FLOAT, name);
    } else if (clazz == double.class || clazz == Double.class) {
      return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.DOUBLE, name);
    } else if (clazz.isEnum()) {
      return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, name);
    } else {
      return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, name);
    }
  }

  @Override
  public RecordWriter getWriter() {
    if (clazz == boolean.class) {
      return new PrimitiveWriters.PrimitiveBooleanWriter();
    } else if (clazz == Boolean.class) {
      return new PrimitiveWriters.BooleanWriter();
    } else if (clazz == byte.class) {
      return new PrimitiveWriters.PrimitiveByteWriter();
    } else if (clazz == Byte.class) {
      return new PrimitiveWriters.ByteWriter();
    } else if (clazz == char.class) {
      return new PrimitiveWriters.PrimitiveCharWriter();
    } else if (clazz == Character.class) {
      return new PrimitiveWriters.CharWriter();
    } else if (clazz == short.class) {
      return new PrimitiveWriters.PrimitiveShortWriter();
    } else if (clazz == Short.class) {
      return new PrimitiveWriters.ShortWriter();
    } else if (clazz.isEnum()) {
      return new PrimitiveWriters.EnumWriter();
    } else if (clazz == int.class) {
      return new PrimitiveWriters.PrimitiveIntWriter();
    } else if (clazz == Integer.class) {
      return new PrimitiveWriters.IntWriter();
    } else if (clazz == long.class) {
      return new PrimitiveWriters.PrimitiveLongWriter();
    } else if (clazz == Long.class) {
      return new PrimitiveWriters.LongWriter();
    } else if (clazz == float.class) {
      return new PrimitiveWriters.PrimitiveFloatWriter();
    } else if (clazz == Float.class) {
      return new PrimitiveWriters.FloatWriter();
    } else if (clazz == double.class) {
      return new PrimitiveWriters.PrimitiveDoubleWriter();
    } else if (clazz == Double.class) {
      return new PrimitiveWriters.DoubleWriter();
    } else if (clazz == byte[].class) {
      return new PrimitiveWriters.ByteArrayWriter();
    } else if (clazz == String.class) {
      return new PrimitiveWriters.StringWriter();
    }

    throw new IllegalStateException(String.format("Type %s could not be mapped to a writer.", clazz));
  }

  @Override
  public Converter getConverter() {
    if (clazz == char.class) {
      if (fieldIfPresent != null) {
        return new PrimitiveConverters.CharFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
      }
      return new PrimitiveConverters.CharConverter();
    } else if (clazz == Character.class) {
      if (fieldIfPresent != null) {
        return new PrimitiveConverters.CharRefFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
      }
    } else if (clazz == short.class) {
      if (fieldIfPresent != null) {
        return new PrimitiveConverters.ShortFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
      }
      return new PrimitiveConverters.ShortConverter();
    } else if (clazz == Short.class) {
      if (fieldIfPresent != null) {
        return new PrimitiveConverters.ShortRefFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
      }
    } else if (clazz == String.class) {
      if (fieldIfPresent != null) {
        return new PrimitiveConverters.StringFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
      }
      return new PrimitiveConverters.StringConverter();
    } else if (clazz.isEnum()) {
      if (fieldIfPresent != null) {
        return new PrimitiveConverters.EnumFieldSettingConverter(
          clazz, parentContainerIfPresent, fieldIfPresent
        );
      }
      return new PrimitiveConverters.EnumConverter(clazz);
    } else if (clazz == byte.class) {
      if (fieldIfPresent != null) {
        return new PrimitiveConverters.ByteFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
      }
      return new PrimitiveConverters.ByteConverter();
    } else if (clazz == Byte.class) {
      if (fieldIfPresent != null) {
        return new PrimitiveConverters.ByteRefFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
      }
    }

    if (fieldIfPresent != null) {
      return new PrimitiveConverters.RefFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
    }

    return new PrimitiveConverters.PrimitivePojoConverter();
  }
}
