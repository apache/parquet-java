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

import com.esotericsoftware.reflectasm.FieldAccess;

import java.lang.reflect.Field;

/**
 * Uses byte-code generation to provide high speed access to non-private fields
 */
public class AsmFieldAccessor implements FieldAccessor {
  private final int index;
  private final FieldAccess fieldAccess;

  public AsmFieldAccessor(Field field) {
    this.fieldAccess = FieldAccess.get(field.getDeclaringClass());
    this.index = fieldAccess.getIndex(field.getName());
  }

  @Override
  public boolean getBoolean(Object target) {
    return this.fieldAccess.getBoolean(target, index);
  }

  @Override
  public byte getByte(Object target) {
    return this.fieldAccess.getByte(target, index);
  }

  @Override
  public char getChar(Object target) {
    return this.fieldAccess.getChar(target, index);
  }

  @Override
  public short getShort(Object target) {
    return this.fieldAccess.getShort(target, index);
  }

  @Override
  public int getInt(Object target) {
    return this.fieldAccess.getInt(target, index);
  }

  @Override
  public long getLong(Object target) {
    return this.fieldAccess.getLong(target, index);
  }

  @Override
  public float getFloat(Object target) {
    return this.fieldAccess.getFloat(target, index);
  }

  @Override
  public double getDouble(Object target) {
    return this.fieldAccess.getDouble(target, index);
  }

  @Override
  public Object get(Object o) {
    return this.fieldAccess.get(o, index);
  }

  @Override
  public void set(Object target, Object value) {
    this.fieldAccess.set(target, index, value);
  }

  @Override
  public void setBoolean(Object target, boolean b) {
    this.fieldAccess.setBoolean(target, index, b);
  }

  @Override
  public void setByte(Object target, byte b) {
    this.fieldAccess.setByte(target, index, b);
  }

  @Override
  public void setChar(Object target, char c) {
    this.fieldAccess.setChar(target, index, c);
  }

  @Override
  public void setShort(Object target, short s) {
    this.fieldAccess.setShort(target, index, s);
  }

  @Override
  public void setInt(Object target, int i) {
    this.fieldAccess.set(target, index, i);
  }

  @Override
  public void setLong(Object target, long l) {
    this.fieldAccess.setLong(target, index, l);
  }

  @Override
  public void setFloat(Object target, float f) {
    this.fieldAccess.setFloat(target, index, f);
  }

  @Override
  public void setDouble(Object target, double d) {
    this.fieldAccess.setDouble(target, index, d);
  }
}
