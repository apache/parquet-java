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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

class PrimitiveResolver extends Resolver {
  private static final Map<Class, RecordWriter> typeToWriter = new HashMap<Class, RecordWriter>();
  private static final Map<Class, PrimitiveType.PrimitiveTypeName> typesToTypeNames = new HashMap<Class, PrimitiveType.PrimitiveTypeName>();
  private static final Map<Class, Converter> typeToConverters = new HashMap<Class, Converter>();
  private static final Map<Class, Constructor> typeToFieldSettingConverters = new HashMap<Class, Constructor>();

  static {
    populateWriters();
    populateTypeNames();
    populateConverters();
    populateFieldSettingConverters();
  }

  private static void populateTypeNames() {
    typesToTypeNames.put(boolean.class, PrimitiveType.PrimitiveTypeName.BOOLEAN);
    typesToTypeNames.put(Boolean.class, PrimitiveType.PrimitiveTypeName.BOOLEAN);
    typesToTypeNames.put(char.class, PrimitiveType.PrimitiveTypeName.INT32);
    typesToTypeNames.put(Character.class, PrimitiveType.PrimitiveTypeName.INT32);
    typesToTypeNames.put(byte.class, PrimitiveType.PrimitiveTypeName.INT32);
    typesToTypeNames.put(Byte.class, PrimitiveType.PrimitiveTypeName.INT32);
    typesToTypeNames.put(short.class, PrimitiveType.PrimitiveTypeName.INT32);
    typesToTypeNames.put(Short.class, PrimitiveType.PrimitiveTypeName.INT32);
    typesToTypeNames.put(int.class, PrimitiveType.PrimitiveTypeName.INT32);
    typesToTypeNames.put(Integer.class, PrimitiveType.PrimitiveTypeName.INT32);
    typesToTypeNames.put(long.class, PrimitiveType.PrimitiveTypeName.INT64);
    typesToTypeNames.put(Long.class, PrimitiveType.PrimitiveTypeName.INT64);
    typesToTypeNames.put(float.class, PrimitiveType.PrimitiveTypeName.FLOAT);
    typesToTypeNames.put(Float.class, PrimitiveType.PrimitiveTypeName.FLOAT);
    typesToTypeNames.put(double.class, PrimitiveType.PrimitiveTypeName.DOUBLE);
    typesToTypeNames.put(Double.class, PrimitiveType.PrimitiveTypeName.DOUBLE);
    typesToTypeNames.put(byte[].class, PrimitiveType.PrimitiveTypeName.BINARY);
    typesToTypeNames.put(String.class, PrimitiveType.PrimitiveTypeName.BINARY);
  }

  private static void populateWriters() {
    typeToWriter.put(boolean.class, new PrimitiveWriters.PrimitiveBooleanWriter());
    typeToWriter.put(Boolean.class, new PrimitiveWriters.BooleanWriter());
    typeToWriter.put(byte.class, new PrimitiveWriters.PrimitiveByteWriter());
    typeToWriter.put(Byte.class, new PrimitiveWriters.ByteWriter());
    typeToWriter.put(char.class, new PrimitiveWriters.PrimitiveCharWriter());
    typeToWriter.put(Character.class, new PrimitiveWriters.CharWriter());
    typeToWriter.put(short.class, new PrimitiveWriters.PrimitiveShortWriter());
    typeToWriter.put(Short.class, new PrimitiveWriters.ShortWriter());
    typeToWriter.put(int.class, new PrimitiveWriters.PrimitiveIntWriter());
    typeToWriter.put(Integer.class, new PrimitiveWriters.IntWriter());
    typeToWriter.put(long.class, new PrimitiveWriters.PrimitiveLongWriter());
    typeToWriter.put(Long.class, new PrimitiveWriters.LongWriter());
    typeToWriter.put(float.class, new PrimitiveWriters.PrimitiveFloatWriter());
    typeToWriter.put(Float.class, new PrimitiveWriters.FloatWriter());
    typeToWriter.put(double.class, new PrimitiveWriters.PrimitiveDoubleWriter());
    typeToWriter.put(Double.class, new PrimitiveWriters.DoubleWriter());
    typeToWriter.put(byte[].class, new PrimitiveWriters.ByteArrayWriter());
    typeToWriter.put(String.class, new PrimitiveWriters.StringWriter());
  }

  private static void populateFieldSettingConverters() {
    typeToFieldSettingConverters.put(char.class, constructorForFieldConverter(PrimitiveConverters.CharFieldSettingConverter.class));
    typeToFieldSettingConverters.put(Character.class, constructorForFieldConverter(PrimitiveConverters.CharRefFieldSettingConverter.class));
    typeToFieldSettingConverters.put(short.class, constructorForFieldConverter(PrimitiveConverters.ShortFieldSettingConverter.class));
    typeToFieldSettingConverters.put(Short.class, constructorForFieldConverter(PrimitiveConverters.ShortRefFieldSettingConverter.class));
    typeToFieldSettingConverters.put(String.class, constructorForFieldConverter(PrimitiveConverters.StringFieldSettingConverter.class));
    typeToFieldSettingConverters.put(byte.class, constructorForFieldConverter(PrimitiveConverters.ByteFieldSettingConverter.class));
    typeToFieldSettingConverters.put(Byte.class, constructorForFieldConverter(PrimitiveConverters.ByteRefFieldSettingConverter.class));
  }

  private static void populateConverters() {
    typeToConverters.put(char.class, new PrimitiveConverters.CharConverter());
    typeToConverters.put(short.class, new PrimitiveConverters.ShortConverter());
    typeToConverters.put(String.class, new PrimitiveConverters.StringConverter());
    typeToConverters.put(byte.class, new PrimitiveConverters.ByteConverter());
  }


  private static Constructor constructorForFieldConverter(Class<? extends Converter> converterClass) {
    try {
      if(converterClass.isEnum()) {
        return converterClass.getConstructor(converterClass, Container.class, Field.class);
      }
      return converterClass.getConstructor(Container.class, Field.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

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
    PrimitiveType.PrimitiveTypeName typeName;

    if(clazz.isEnum()) {
      typeName = PrimitiveType.PrimitiveTypeName.BINARY;
    } else {
      typeName = typesToTypeNames.get(clazz);
    }

    return new PrimitiveType(repetition, typeName, name);
  }

  @Override
  public RecordWriter getWriter() {
    if(clazz.isEnum()) {
      return new PrimitiveWriters.EnumWriter();
    }

    RecordWriter recordWriter = typeToWriter.get(clazz);
    if (recordWriter != null) {
      return recordWriter;
    }

    throw new IllegalStateException(String.format("Type %s could not be mapped to a writer.", clazz));
  }

  @Override
  public Converter getConverter() {
    if(fieldIfPresent != null) {
      if(fieldIfPresent.getType().isEnum()) {
        return new PrimitiveConverters.EnumFieldSettingConverter(fieldIfPresent.getType(), parentContainerIfPresent, fieldIfPresent);
      }

      try {
        Constructor converterConstructor = typeToFieldSettingConverters.get(fieldIfPresent.getType());
        if(converterConstructor == null) {
          return new PrimitiveConverters.RefFieldSettingConverter(parentContainerIfPresent, fieldIfPresent);
        }
        return (Converter) converterConstructor.newInstance(parentContainerIfPresent, fieldIfPresent);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      if(clazz.isEnum()) {
        return new PrimitiveConverters.EnumConverter(clazz);
      }
      Converter converter = typeToConverters.get(clazz);
      if(converter == null) {
        converter = new PrimitiveConverters.PrimitivePojoConverter();
      }
      return converter;
    }
  }
}