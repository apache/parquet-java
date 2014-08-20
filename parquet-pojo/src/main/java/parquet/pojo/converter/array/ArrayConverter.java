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
package parquet.pojo.converter.array;

import parquet.hadoop.mapred.Container;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.pojo.Resolver;
import parquet.pojo.converter.PojoConverter;
import parquet.pojo.field.DelegatingFieldAccessor;
import parquet.pojo.field.FieldAccessor;

import java.lang.reflect.Field;

/**
 * Converter for array types
 *
 */
public class ArrayConverter extends GroupConverter implements PojoConverter {
  private final Container parentContainer;
  private final ArrayInstantiatingConverter arrayInstantiatingConverter;
  private final Converter valuesConverter;
  private FieldAccessor parentFieldAccessorIfPresent;

  public ArrayConverter(Class componentType, Field fieldIfPresent, Container parentContainer) {
    this.parentContainer = parentContainer;
    this.arrayInstantiatingConverter = new ArrayInstantiatingConverter(componentType);

    if (componentType == char.class) {
      valuesConverter = new CharArrayValuesConverter(arrayInstantiatingConverter);
    } else if (componentType == short.class) {
      valuesConverter = new ShortArrayValuesConverter(arrayInstantiatingConverter);
    } else if (componentType.isPrimitive()) {
      valuesConverter = new PrimitiveArrayValuesConverter(arrayInstantiatingConverter);
    } else {
      valuesConverter = new RefArrayValuesConverter(componentType, arrayInstantiatingConverter);
    }

    if (fieldIfPresent != null) {
      parentFieldAccessorIfPresent = new DelegatingFieldAccessor(fieldIfPresent);
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    if (fieldIndex == 0) {
      return arrayInstantiatingConverter;
    }

    return valuesConverter;
  }

  @Override
  public void start() {
    ((Resettable) valuesConverter).reset();
  }

  @Override
  public void end() {
    parentFieldAccessorIfPresent.set(parentContainer.get(), arrayInstantiatingConverter.currentArray());
  }

  @Override
  public Object getValueAndReset() {
    return arrayInstantiatingConverter.currentArray();
  }

  public static class RefArrayValuesConverter extends GroupConverter implements Resettable {
    private final ArrayInstantiatingConverter arrayInstantiatingConverter;
    private final PojoConverter asPojoConverter;
    private final Converter converter;
    private int index = 0;

    public RefArrayValuesConverter(Class componentType, ArrayInstantiatingConverter arrayInstantiatingConverter) {
      this.arrayInstantiatingConverter = arrayInstantiatingConverter;
      this.converter = Resolver.newResolver(componentType, null, null).getConverter();
      this.asPojoConverter = (PojoConverter) this.converter;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converter;
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
      ((Object[]) arrayInstantiatingConverter.currentArray())[index] = asPojoConverter.getValueAndReset();
      index++;
    }

    @Override
    public void reset() {
      index = 0;
    }
  }

  public static class PrimitiveArrayValuesConverter extends PrimitiveConverter implements Resettable {
    protected final ArrayInstantiatingConverter arrayInstantiatingConverter;
    protected int index = 0;

    public PrimitiveArrayValuesConverter(ArrayInstantiatingConverter arrayInstantiatingConverter) {
      this.arrayInstantiatingConverter = arrayInstantiatingConverter;
    }

    @Override
    public void reset() {
      index = 0;
    }

    @Override
    public void addBinary(Binary value) {
      ((String[]) arrayInstantiatingConverter.currentArray())[index] = value.toStringUsingUTF8();
      index++;
    }

    @Override
    public void addBoolean(boolean value) {
      ((boolean[]) arrayInstantiatingConverter.currentArray())[index] = value;
      index++;
    }

    @Override
    public void addInt(int value) {
      ((int[]) arrayInstantiatingConverter.currentArray())[index] = value;
      index++;
    }

    @Override
    public void addLong(long value) {
      ((long[]) arrayInstantiatingConverter.currentArray())[index] = value;
      index++;
    }

    @Override
    public void addFloat(float value) {
      ((float[]) arrayInstantiatingConverter.currentArray())[index] = value;
      index++;
    }

    @Override
    public void addDouble(double value) {
      ((double[]) arrayInstantiatingConverter.currentArray())[index] = value;
      index++;
    }
  }

  //chars and shorts are special, since they are written as ints to parquet, but are casted to shorts / chars
  public static class CharArrayValuesConverter extends PrimitiveArrayValuesConverter {
    public CharArrayValuesConverter(
      ArrayInstantiatingConverter arrayInstantiatingConverter
    ) {
      super(arrayInstantiatingConverter);
    }

    @Override
    public void addInt(int value) {
      ((char[]) arrayInstantiatingConverter.currentArray())[index] = (char) value;
      index++;
    }
  }

  public static class ShortArrayValuesConverter extends PrimitiveArrayValuesConverter {
    public ShortArrayValuesConverter(
      ArrayInstantiatingConverter arrayInstantiatingConverter
    ) {
      super(arrayInstantiatingConverter);
    }

    @Override
    public void addInt(int value) {
      ((short[]) arrayInstantiatingConverter.currentArray())[index] = (short) value;
      index++;
    }
  }
}