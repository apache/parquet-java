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
import java.lang.reflect.Modifier;

/**
 * Checks if a field is not private, if so it will delegate to {@link AsmFieldAccessor} otherwise it will delegate to {@link ReflectionFieldAccessor}
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
public class DelegatingFieldAccessor implements FieldAccessor {
  private final Field field;
  private FieldAccessor delegate;

  public DelegatingFieldAccessor(Field field) {
    this.field = field;

    int modifiers = this.field.getModifiers();

        /*
            TODO: Investigate using byte code generation for getters / setters, all the libraries I've found force boxing / unboxing of primitives and don't offer any advantages over reflection
        */
    if (Modifier.isPrivate(modifiers)) {
      this.delegate = new ReflectionFieldAccessor(field);
    } else {
      this.delegate = new AsmFieldAccessor(field);
    }
  }

  @Override
  public boolean getBoolean(Object target) {
    return this.delegate.getBoolean(target);
  }

  @Override
  public byte getByte(Object target) {
    return this.delegate.getByte(target);
  }

  @Override
  public char getChar(Object target) {
    return this.delegate.getChar(target);
  }

  @Override
  public short getShort(Object target) {
    return this.delegate.getShort(target);
  }

  @Override
  public int getInt(Object target) {
    return this.delegate.getInt(target);
  }

  @Override
  public long getLong(Object target) {
    return this.delegate.getLong(target);
  }

  @Override
  public float getFloat(Object target) {
    return this.delegate.getFloat(target);
  }

  @Override
  public double getDouble(Object target) {
    return this.delegate.getDouble(target);
  }

  public Object get(Object o) {
    return this.delegate.get(o);
  }

  @Override
  public void set(Object target, Object value) {
    this.delegate.set(target, value);
  }

  @Override
  public void setBoolean(Object target, boolean b) {
    this.delegate.setBoolean(target, b);
  }

  @Override
  public void setByte(Object target, byte b) {
    this.delegate.setByte(target, b);
  }

  @Override
  public void setChar(Object target, char c) {
    this.delegate.setChar(target, c);
  }

  @Override
  public void setShort(Object target, short s) {
    this.delegate.setShort(target, s);
  }

  @Override
  public void setInt(Object target, int i) {
    this.delegate.setInt(target, i);
  }

  @Override
  public void setLong(Object target, long l) {
    this.delegate.setLong(target, l);
  }

  @Override
  public void setFloat(Object target, float f) {
    this.delegate.setFloat(target, f);
  }

  @Override
  public void setDouble(Object target, double d) {
    this.delegate.setDouble(target, d);
  }
}
