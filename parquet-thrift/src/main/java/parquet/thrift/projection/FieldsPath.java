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
package parquet.thrift.projection;

import java.util.ArrayList;

import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;

/**
 * Represents a column path as a sequence of fields.
 *
 * @author Tianshuo Deng
 */
public class FieldsPath {
  private final ArrayList<ThriftField> fields = new ArrayList<ThriftField>();

  public void push(ThriftField f) {
    this.fields.add(f);
  }

  public ThriftField pop() {
    return this.fields.remove(fields.size() - 1);
  }

  public ArrayList<ThriftField> getFields() {
    return fields;
  }

  public String toDelimitedString(String delim) {
    StringBuilder delimited = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      ThriftField currentField = fields.get(i);
      if (i > 0) {
        ThriftField previousField = fields.get(i - 1);
        if (FieldsPath.isKeyFieldOfMap(currentField, previousField)) {
          delimited.append("key");
          delimited.append(delim);
          continue;
        } else if (FieldsPath.isValueFieldOfMap(currentField, previousField)) {
          delimited.append("value");
          delimited.append(delim);
          continue;
        }
      }
      delimited.append(currentField.getName()).append(delim);
    }

    if (delimited.length() == 0) {
      return "";
    } else {
      return delimited.substring(0, delimited.length() - 1);
    }
  }

  @Override
  public String toString() {
    return toDelimitedString(".");
  }

  private static boolean isValueFieldOfMap(ThriftField currentField, ThriftField previousField) {
    ThriftType previousType = previousField.getType();
    return previousType instanceof ThriftType.MapType && ((ThriftType.MapType) previousType).getValue() == currentField;
  }

  private static boolean isKeyFieldOfMap(ThriftField currentField, ThriftField previousField) {
    ThriftType previousType = previousField.getType();
    return previousType instanceof ThriftType.MapType && ((ThriftType.MapType) previousType).getKey() == currentField;
  }

}
