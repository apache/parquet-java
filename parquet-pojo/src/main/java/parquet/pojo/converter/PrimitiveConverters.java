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
package parquet.pojo.converter;

import parquet.hadoop.mapred.Container;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.pojo.field.DelegatingFieldAccessor;
import parquet.pojo.field.FieldAccessor;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Converters for all the primitive types as well as {@link String}, byte array and {@link Enum} support
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
public class PrimitiveConverters {

  public static class PrimitivePojoConverter extends PrimitiveConverter implements PojoConverter {
    protected Object boxedValue = null;

    @Override
    public void addBinary(Binary value) {
      boxedValue = value.getBytes();
    }

    @Override
    public void addBoolean(boolean value) {
      boxedValue = value;
    }

    @Override
    public void addDouble(double value) {
      boxedValue = value;
    }

    @Override
    public void addFloat(float value) {
      boxedValue = value;
    }

    @Override
    public void addInt(int value) {
      boxedValue = value;
    }

    @Override
    public void addLong(long value) {
      boxedValue = value;
    }

    @Override
    public Object getRawValue() {
      Object tmp = boxedValue;
      boxedValue = null;
      return tmp;
    }
  }

  /**
   * {@link PrimitiveConverter} that will set its boxed value on a containing parent object. Provides default implementations for primitive types.
   */
  public static class RefFieldSettingConverter extends PrimitiveConverter {
    protected final Container parentContainer;
    protected final FieldAccessor parentFieldAccessor;

    public RefFieldSettingConverter(Container parentContainer, Field field) {
      this.parentContainer = parentContainer;
      this.parentFieldAccessor = new DelegatingFieldAccessor(field);
    }

    @Override
    public void addBinary(Binary value) {
      parentFieldAccessor.set(parentContainer.get(), value.getBytes());
    }

    @Override
    public void addBoolean(boolean value) {
      parentFieldAccessor.set(parentContainer.get(), value);
    }

    @Override
    public void addDouble(double value) {
      parentFieldAccessor.set(parentContainer.get(), value);
    }

    @Override
    public void addFloat(float value) {
      parentFieldAccessor.set(parentContainer.get(), value);
    }

    @Override
    public void addInt(int value) {
      parentFieldAccessor.set(parentContainer.get(), value);
    }

    @Override
    public void addLong(long value) {
      parentFieldAccessor.set(parentContainer.get(), value);
    }
  }

  /**
   * A {@link PrimitiveConverter} that will set values on a parent object without boxing them.  Provides default implementations for primitive types.
   */
  public static class PrimitiveFieldSettingConverter extends PrimitiveConverter {
    protected final Container parentContainer;
    protected final FieldAccessor parentFieldAccessor;

    public PrimitiveFieldSettingConverter(Container parentContainer, Field field) {
      this.parentContainer = parentContainer;
      this.parentFieldAccessor = new DelegatingFieldAccessor(field);
    }

    @Override
    public void addBoolean(boolean value) {
      parentFieldAccessor.setBoolean(parentContainer.get(), value);
    }

    @Override
    public void addDouble(double value) {
      parentFieldAccessor.setDouble(parentContainer.get(), value);
    }

    @Override
    public void addFloat(float value) {
      parentFieldAccessor.setFloat(parentContainer.get(), value);
    }

    @Override
    public void addInt(int value) {
      parentFieldAccessor.setInt(parentContainer.get(), value);
    }

    @Override
    public void addLong(long value) {
      parentFieldAccessor.setLong(parentContainer.get(), value);
    }
  }

  public static class ByteConverter extends PrimitivePojoConverter {
    @Override
    public void addInt(int value) {
      boxedValue = (byte) value;
    }
  }

  public static class ByteFieldSettingConverter extends PrimitiveFieldSettingConverter {
    public ByteFieldSettingConverter(Container parentContainer, Field field) {
      super(parentContainer, field);
    }

    @Override
    public void addInt(int value) {
      parentFieldAccessor.setByte(parentContainer.get(), (byte) value);
    }
  }

  public static class ByteRefFieldSettingConverter extends RefFieldSettingConverter {
    public ByteRefFieldSettingConverter(Container parentContainer, Field field) {
      super(parentContainer, field);
    }

    @Override
    public void addInt(int value) {
      parentFieldAccessor.set(parentContainer.get(), (byte) value);
    }
  }

  public static class CharConverter extends PrimitivePojoConverter {
    @Override
    public void addInt(int value) {
      boxedValue = (char) value;
    }
  }

  public static class CharFieldSettingConverter extends PrimitiveFieldSettingConverter {
    public CharFieldSettingConverter(Container parentContainer, Field field) {
      super(parentContainer, field);
    }

    @Override
    public void addInt(int value) {
      parentFieldAccessor.setChar(parentContainer.get(), (char) value);
    }
  }

  public static class CharRefFieldSettingConverter extends RefFieldSettingConverter {
    public CharRefFieldSettingConverter(Container parentContainer, Field field) {
      super(parentContainer, field);
    }

    @Override
    public void addInt(int value) {
      parentFieldAccessor.set(parentContainer.get(), (char) value);
    }
  }

  public static class ShortConverter extends PrimitivePojoConverter {
    @Override
    public void addInt(int value) {
      boxedValue = (short) value;
    }
  }

  public static class ShortFieldSettingConverter extends PrimitiveFieldSettingConverter {
    public ShortFieldSettingConverter(Container parentContainer, Field field) {
      super(parentContainer, field);
    }

    @Override
    public void addInt(int value) {
      parentFieldAccessor.setShort(parentContainer.get(), (short) value);
    }
  }

  public static class ShortRefFieldSettingConverter extends RefFieldSettingConverter {
    public ShortRefFieldSettingConverter(Container parentContainer, Field field) {
      super(parentContainer, field);
    }

    @Override
    public void addInt(int value) {
      parentFieldAccessor.set(parentContainer.get(), (short) value);
    }
  }

  public static class EnumConverter extends PrimitivePojoConverter {
    protected final Map<String, Enum> enumMap;

    public EnumConverter(Class clazz) {
      Object[] constants = clazz.getEnumConstants();
      enumMap = new HashMap<String, Enum>();

      for (Object o : constants) {
        Enum e = (Enum) o;
        enumMap.put(e.name(), e);
      }

    }

    @Override
    public void addBinary(Binary value) {
      boxedValue = enumMap.get(value.toStringUsingUTF8());
    }
  }

  public static class EnumFieldSettingConverter extends EnumConverter {
    private final Container parentContainerIfPresent;
    private final FieldAccessor fieldAccessor;

    public EnumFieldSettingConverter(Class clazz, Container parentContainerIfPresent, Field parentFieldIfPresent) {
      super(clazz);
      this.parentContainerIfPresent = parentContainerIfPresent;
      this.fieldAccessor = new DelegatingFieldAccessor(parentFieldIfPresent);
    }

    @Override
    public void addBinary(Binary value) {
      fieldAccessor.set(parentContainerIfPresent.get(), enumMap.get(value.toStringUsingUTF8()));
    }
  }

  public static class StringConverter extends PrimitivePojoConverter {
    @Override
    public void addBinary(Binary value) {
      boxedValue = value.toStringUsingUTF8();
    }
  }

  public static class StringFieldSettingConverter extends RefFieldSettingConverter {
    public StringFieldSettingConverter(Container parentContainer, Field field) {
      super(parentContainer, field);
    }

    @Override
    public void addBinary(Binary value) {
      parentFieldAccessor.set(parentContainer.get(), value.toStringUsingUTF8());
    }
  }
}