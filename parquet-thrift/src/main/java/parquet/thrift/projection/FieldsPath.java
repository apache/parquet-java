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
package parquet.thrift.projection;

import com.twitter.elephantbird.thrift.TStructDescriptor;

import java.util.ArrayList;

/**
 * represent field path for thrift field
 *
 * @author Tianshuo Deng
 */
public class FieldsPath {
  ArrayList<TStructDescriptor.Field> fields = new ArrayList<TStructDescriptor.Field>();

  public void push(TStructDescriptor.Field field) {
    this.fields.add(field);
  }

  public TStructDescriptor.Field pop() {
    return this.fields.remove(fields.size() - 1);
  }

  @Override
  public String toString() {
    StringBuffer pathStrBuffer = new StringBuffer();
    for (int i = 0; i < fields.size(); i++) {
      TStructDescriptor.Field currentField = fields.get(i);

      if (i > 0) {
        TStructDescriptor.Field previousField = fields.get(i - 1);
        if (isKeyFieldOfMap(currentField, previousField)) {
          pathStrBuffer.append("key/");
          continue;
        } else if (isValueFieldOfMap(currentField, previousField)) {
          pathStrBuffer.append("value/");
          continue;
        }
      }

      pathStrBuffer.append(currentField.getFieldMetaData().fieldName).append("/");
    }

    if (pathStrBuffer.length() == 0) {
      return "";
    } else {
      String pathStr = pathStrBuffer.substring(0, pathStrBuffer.length() - 1);
      return pathStr;
    }
  }

  private boolean isValueFieldOfMap(TStructDescriptor.Field currentField, TStructDescriptor.Field previousField) {
    return previousField.getMapValueField()==currentField;
  }

  private boolean isKeyFieldOfMap(TStructDescriptor.Field currentField, TStructDescriptor.Field previousField) {
    return previousField.getMapKeyField()==currentField;
  }
}
