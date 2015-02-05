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
package parquet.thrift.struct;

import java.io.IOException;
import java.io.StringWriter;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

class JSON {

  private static ObjectMapper om = new ObjectMapper();
  static {
    om.configure(Feature.INDENT_OUTPUT, true);
  }

  static <T> T fromJSON(String json, Class<T> clzz) {
    try {
      return om.readValue(json, clzz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String toJSON(Object o) {
    final StringWriter sw = new StringWriter();
    try {
      om.writeValue(sw, o);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sw.toString();
  }
}
