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
package parquet.pojo.field;

import java.lang.reflect.Field;

/**
 * Uses reflection to access fields, usually used as a fall back to byte-code generation if the fields are private
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
public class ReflectionFieldAccessor implements FieldAccessor {
  private final Field field;

  public ReflectionFieldAccessor(Field field) {
    field.setAccessible(true);

    this.field = field;
  }

  @Override
  public boolean getBoolean(Object target) {
    try {
      return this.field.getBoolean(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte getByte(Object target) {
    try {
      return this.field.getByte(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public char getChar(Object target) {
    try {
      return this.field.getChar(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public short getShort(Object target) {
    try {
      return this.field.getShort(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getInt(Object target) {
    try {
      return this.field.getInt(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getLong(Object target) {
    try {
      return this.field.getLong(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float getFloat(Object target) {
    try {
      return this.field.getFloat(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double getDouble(Object target) {
    try {
      return this.field.getDouble(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public Object get(Object o) {
    try {
      return this.field.get(o);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void set(Object target, Object value) {
    try {
      this.field.set(target, value);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setBoolean(Object target, boolean b) {
    try {
      this.field.setBoolean(target, b);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setByte(Object target, byte b) {
    try {
      this.field.setByte(target, b);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setChar(Object target, char c) {
    try {
      this.field.setChar(target, c);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setShort(Object target, short s) {
    try {
      this.field.setShort(target, s);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setInt(Object target, int i) {
    try {
      this.field.setInt(target, i);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setLong(Object target, long l) {
    try {
      this.field.setLong(target, l);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setFloat(Object target, float f) {
    try {
      this.field.setFloat(target, f);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setDouble(Object target, double d) {
    try {
      this.field.setDouble(target, d);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}