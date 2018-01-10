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
package org.apache.parquet;

import java.util.regex.Pattern;

/**
 * Utility for handling quoted identifiers.
 *
 * Created by stylesm on 20/02/17.
 */
public class QuotedIdentifiers {

  /**
   * Pattern for matching column names that need to wrapped in backquotes.
   */
  private static final Pattern pattern = Pattern.compile("[ ,;{}()`\\.\\n\\t=]");

  /**
   * @return the name of the type, possibly wrapped in backquotes.
   */
  public static String getName(String name) {
    return pattern.matcher(name).find() ? String.format("`%s`", name.replace("`", "``")) : name;
  }

  public static String[] getParts(String path) {
    String[] parts = path.split("\\.(?=([^`]*`[^`]*`)*[^`]*$)");
    for (int i = 0; i < parts.length; ++i) {
      String part = parts[i];
      if (part.startsWith("`") && part.endsWith("`")) {
        parts[i] = part.substring(1, part.length() - 1).replaceAll("``", "`");
      }
    }
    return parts;
  }
}
