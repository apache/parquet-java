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

public interface FieldAccessor {
  boolean getBoolean(Object target);

  byte getByte(Object target);

  char getChar(Object target);

  short getShort(Object target);

  int getInt(Object target);

  long getLong(Object target);

  float getFloat(Object target);

  double getDouble(Object target);

  Object get(Object o);

  void set(Object target, Object value);

  void setBoolean(Object target, boolean b);

  void setByte(Object target, byte b);

  void setChar(Object target, char c);

  void setShort(Object target, short s);

  void setInt(Object target, int i);

  void setLong(Object target, long l);

  void setFloat(Object target, float f);

  void setDouble(Object target, double d);
}
