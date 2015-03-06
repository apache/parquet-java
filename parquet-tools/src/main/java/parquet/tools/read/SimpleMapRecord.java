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
package parquet.tools.read;

import com.google.common.collect.Maps;

import java.util.Map;

public class SimpleMapRecord extends SimpleRecord {
  @Override
  protected Object toJsonObject() {
    Map<String, Object> result = Maps.newLinkedHashMap();
    for (NameValue value : values) {
      String key = null;
      Object val = null;
      for (NameValue kv : ((SimpleRecord) value.getValue()).values) {
        if (kv.getName().equals("key")) {
          key = (String) kv.getValue();
        } else if (kv.getName().equals("value")) {
          val = toJsonValue(kv.getValue());
        }
      }
      result.put(key, val);
    }
    return result;
  }
}
