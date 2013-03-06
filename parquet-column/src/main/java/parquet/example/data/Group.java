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
package parquet.example.data;

import parquet.Log;
import parquet.io.Binary;
import parquet.io.RecordConsumer;

abstract public class Group extends GroupValueSource {
  private static final Log logger = Log.getLog(Group.class);
  private static final boolean DEBUG = Log.DEBUG;

  public void add(String field, int value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, long value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, String value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, boolean value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, Binary value) {
    add(getType().getFieldIndex(field), value);
  }

  public Group addGroup(String field) {
    if (DEBUG) logger.debug("add group "+field+" to "+getType().getName());
    return addGroup(getType().getFieldIndex(field));
  }

  public Group getGroup(String field, int index) {
    return getGroup(getType().getFieldIndex(field), index);
  }

  abstract public void add(int fieldIndex, int value);

  abstract public void add(int fieldIndex, long value);

  abstract public void add(int fieldIndex, String value);

  abstract public void add(int fieldIndex, boolean value);

  abstract public void add(int fieldIndex, Binary value);

  abstract public void add(int fieldIndex, float value);

  abstract public void add(int fieldIndex, double value);

  abstract public Group addGroup(int fieldIndex);

  abstract public Group getGroup(int fieldIndex, int index);

  public Group asGroup() {
    return this;
  }

  public Group append(String fieldName, int value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, long value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, String value) {
    add(fieldName, Binary.fromString(value));
    return this;
  }

  public Group append(String fieldName, boolean value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, Binary value) {
    add(fieldName, value);
    return this;
  }

  abstract public void writeValue(int field, int index, RecordConsumer recordConsumer);

}
