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
 * represent field path for thrift field
 *
 * @author Tianshuo Deng
 */
public class FieldsPath {
  ArrayList<ThriftField> fields = new ArrayList<ThriftField>();

  public void push(ThriftField field) {
    this.fields.add(field);
  }

  public ThriftField pop() {
    return this.fields.remove(fields.size() - 1);
  }

  @Override
  public String toString() {
    StringBuffer pathStrBuffer = new StringBuffer();
    for (int i = 0; i < fields.size(); i++) {
      ThriftField currentField = fields.get(i);
      if (i > 0) {
        ThriftField previousField = fields.get(i - 1);
        if (isKeyFieldOfMap(currentField, previousField)) {
          pathStrBuffer.append("key/");
          continue;
        } else if (isValueFieldOfMap(currentField, previousField)) {
          pathStrBuffer.append("value/");
          continue;
        }
      }

      pathStrBuffer.append(currentField.getName()).append("/");
    }

    if (pathStrBuffer.length() == 0) {
      return "";
    } else {
      String pathStr = pathStrBuffer.substring(0, pathStrBuffer.length() - 1);
      return pathStr;
    }
  }

  private boolean isValueFieldOfMap(ThriftField currentField, ThriftField previousField) {
    ThriftType previousType = previousField.getType();
    if(!(previousType instanceof ThriftType.MapType)) {
      return false;
    }
    return ((ThriftType.MapType)previousType).getValue()==currentField;
  }

  private boolean isKeyFieldOfMap(ThriftField currentField, ThriftField previousField) {
    ThriftType previousType = previousField.getType();
    if(!(previousType instanceof ThriftType.MapType)) {
      return false;
    }
    return ((ThriftType.MapType)previousType).getKey()==currentField;
  }
}
