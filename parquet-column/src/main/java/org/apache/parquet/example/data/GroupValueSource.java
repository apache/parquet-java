/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.example.data;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;

public abstract class GroupValueSource {

  public int getFieldRepetitionCount(String field) {
    return getFieldRepetitionCount(getType().getFieldIndex(field));
  }

  public GroupValueSource getGroup(String field, int index) {
    return getGroup(getType().getFieldIndex(field), index);
  }

  public String getString(String field, int index) {
    return getString(getType().getFieldIndex(field), index);
  }

  public int getInteger(String field, int index) {
    return getInteger(getType().getFieldIndex(field), index);
  }

  public long getLong(String field, int index) {
    return getLong(getType().getFieldIndex(field), index);
  }

  public double getDouble(String field, int index) {
    return getDouble(getType().getFieldIndex(field), index);
  }

  public float getFloat(String field, int index) {
    return getFloat(getType().getFieldIndex(field), index);
  }

  public boolean getBoolean(String field, int index) {
    return getBoolean(getType().getFieldIndex(field), index);
  }

  public Binary getBinary(String field, int index) {
    return getBinary(getType().getFieldIndex(field), index);
  }

  public Binary getInt96(String field, int index) {
    return getInt96(getType().getFieldIndex(field), index);
  }

  public abstract int getFieldRepetitionCount(int fieldIndex);

  public abstract GroupValueSource getGroup(int fieldIndex, int index);

  public abstract String getString(int fieldIndex, int index);

  public abstract int getInteger(int fieldIndex, int index);

  public abstract long getLong(int fieldIndex, int index);

  public abstract double getDouble(int fieldIndex, int index);

  public abstract float getFloat(int fieldIndex, int index);

  public abstract boolean getBoolean(int fieldIndex, int index);

  public abstract Binary getBinary(int fieldIndex, int index);

  public abstract Binary getInt96(int fieldIndex, int index);

  public abstract String getValueToString(int fieldIndex, int index);

  public abstract GroupType getType();
}
