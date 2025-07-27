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
package org.apache.parquet.variant;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.Preconditions;


public class JsonUtil {

  private JsonUtil() {}

  private static final JsonFactory FACTORY =
      new JsonFactoryBuilder()
          .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
          .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false)
          .build();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  public static int getInt(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing int: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isIntegralNumber() && pNode.canConvertToInt(),
        "Cannot parse to an integer value: %s: %s",
        property,
        pNode);
    return pNode.asInt();
  }

  public static String getString(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing string: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isTextual(),
        "Cannot parse to a string value: %s: %s",
        property,
        pNode);
    return pNode.asText();
  }

  public static String getStringOrNull(String property, JsonNode node) {
    if (!node.has(property)) {
      return null;
    }
    JsonNode pNode = node.get(property);
    if (pNode != null && pNode.isNull()) {
      return null;
    }
    return getString(property, node);
  }

}
