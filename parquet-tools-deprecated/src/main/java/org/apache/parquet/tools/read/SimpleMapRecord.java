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
package org.apache.parquet.tools.read;

import com.fasterxml.jackson.databind.node.BinaryNode;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;

public class SimpleMapRecord extends SimpleRecord {
  @Override
  protected Object toJsonObject() {
    Map<String, Object> result = Maps.newLinkedHashMap();
    for (NameValue value : values) {
      String key = null;
      Object val = null;
      for (NameValue kv : ((SimpleRecord) value.getValue()).values) {
        String kvName = kv.getName();
        Object kvValue = kv.getValue();
        if (kvName.equals("key")) {
          key = keyToString(kvValue);
        } else if (kvName.equals("value")) {
          val = toJsonValue(kvValue);
        }
      }
      result.put(key, val);
    }
    return result;
  }

  String keyToString(Object kvValue) {
    if (kvValue == null) {
      return "null";
    }

    Class<?> type = kvValue.getClass();
    if (type.isArray()) {
      if (type.getComponentType() == boolean.class) {
        return Arrays.toString((boolean[]) kvValue);
      }
      else if (type.getComponentType() == byte.class) {
        return new BinaryNode((byte[]) kvValue).asText();
      }
      else if (type.getComponentType() == char.class) {
        return Arrays.toString((char[]) kvValue);
      }
      else if (type.getComponentType() == double.class) {
        return Arrays.toString((double[]) kvValue);
      }
      else if (type.getComponentType() == float.class) {
        return Arrays.toString((float[]) kvValue);
      }
      else if (type.getComponentType() == int.class) {
        return Arrays.toString((int[]) kvValue);
      }
      else if (type.getComponentType() == long.class) {
        return Arrays.toString((long[]) kvValue);
      }
      else if (type.getComponentType() == short.class) {
        return Arrays.toString((short[]) kvValue);
      }
      else {
        return Arrays.toString((Object[]) kvValue);
      }
    } else {
      return String.valueOf(kvValue);
    }
  }
}
