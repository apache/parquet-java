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

package org.apache.parquet.hadoop;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;

/**
 * Parses per-column values from a {@link Configuration}.
 *
 * <p>Legacy keys use {@code root.key#column.path}. Structured keys use {@code
 * root.key.column-path#encoded.path}, where each UTF-8 path component is encoded independently so
 * literal dots in field names remain distinct from path separators.
 */
class ColumnConfigParser {

  private static final String COLUMN_PATH_SUFFIX = ".column-path";
  private static final String EMPTY_PATH_COMPONENT = "~";

  private static class ConfigHelper<T> {
    private final String prefix;
    private final Function<String, T> function;
    private final BiConsumer<String, T> consumer;

    public ConfigHelper(String prefix, Function<String, T> function, BiConsumer<String, T> consumer) {
      this.prefix = prefix;
      this.function = function;
      this.consumer = consumer;
    }

    public void processKey(String key) {
      if (key.startsWith(prefix)) {
        String columnPath = key.substring(prefix.length());
        T value = function.apply(key);
        consumer.accept(columnPath, value);
      }
    }
  }

  private static class ColumnPathConfigHelper<T> {
    private final String prefix;
    private final Function<String, T> function;
    private final BiConsumer<String[], T> consumer;

    private ColumnPathConfigHelper(String prefix, Function<String, T> function, BiConsumer<String[], T> consumer) {
      this.prefix = prefix;
      this.function = function;
      this.consumer = consumer;
    }

    private void processKey(String key) {
      if (key.startsWith(prefix)) {
        String columnPath = key.substring(prefix.length());
        T value = function.apply(key);
        consumer.accept(decodeColumnPath(columnPath), value);
      }
    }
  }

  private final List<ConfigHelper<?>> helpers = new ArrayList<>();
  private final List<ColumnPathConfigHelper<?>> columnPathHelpers = new ArrayList<>();

  public <T> ColumnConfigParser withColumnConfig(
      String rootKey, Function<String, T> function, BiConsumer<String, T> consumer) {
    helpers.add(new ConfigHelper<T>(rootKey + '#', function, consumer));
    return this;
  }

  public <T> ColumnConfigParser withColumnPathConfig(
      String rootKey, Function<String, T> function, BiConsumer<String[], T> consumer) {
    columnPathHelpers.add(new ColumnPathConfigHelper<T>(rootKey + COLUMN_PATH_SUFFIX + '#', function, consumer));
    return this;
  }

  static String columnPathKey(String rootKey, String[] columnPath) {
    return rootKey + COLUMN_PATH_SUFFIX + '#' + encodeColumnPath(columnPath);
  }

  private static String encodeColumnPath(String[] columnPath) {
    return Stream.of(columnPath)
        .map(ColumnConfigParser::encodePathComponent)
        .collect(Collectors.joining("."));
  }

  private static String encodePathComponent(String component) {
    if (component.isEmpty()) {
      return EMPTY_PATH_COMPONENT;
    }
    return Base64.getUrlEncoder().withoutPadding().encodeToString(component.getBytes(StandardCharsets.UTF_8));
  }

  private static String[] decodeColumnPath(String columnPath) {
    return Stream.of(columnPath.split("\\.", -1))
        .map(ColumnConfigParser::decodePathComponent)
        .toArray(String[]::new);
  }

  private static String decodePathComponent(String component) {
    if (EMPTY_PATH_COMPONENT.equals(component)) {
      return "";
    }
    return new String(Base64.getUrlDecoder().decode(component), StandardCharsets.UTF_8);
  }

  public void parseConfig(Configuration conf) {
    for (Map.Entry<String, String> entry : conf) {
      for (ConfigHelper<?> helper : helpers) {
        // We retrieve the value from function instead of parsing from the string here to use the exact
        // implementations
        // in Configuration
        helper.processKey(entry.getKey());
      }
    }
    // Structured-path overrides are applied last so an unambiguous setting wins over a legacy
    // dot-string setting for the same property.
    for (Map.Entry<String, String> entry : conf) {
      for (ColumnPathConfigHelper<?> helper : columnPathHelpers) {
        helper.processKey(entry.getKey());
      }
    }
  }
}
