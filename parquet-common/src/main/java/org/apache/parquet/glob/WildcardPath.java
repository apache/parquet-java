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
package org.apache.parquet.glob;

import java.util.regex.Pattern;

import org.apache.parquet.Preconditions;

/**
 * Holds a String with wildcards ('*'), and can answer whether a given string
 * matches this WildcardPath. For example: "foo.*.baz" or "foo*baz.bar*"
 *
 * The '*' in "foo*bar" is treated the same way that java regex treats "(.*)",
 * and all WildcardPath's are considered to match child paths. For example,
 * "foo.bar" will match "foo.bar.baz". It will not match "foo.barbaz" however.
 * To match "foo.barbaz" the pattern "foo.bar*" could be used, which would also
 * match "foo.barbaz.x"
 *
 * Only '*' is considered a special character. All other characters are not
 * treated as special characters, including '{', '}', '.', and '/' with one
 * exception -- the delimiter character is used for matching against child paths
 * as explained above.
 *
 * It is assumed that {} globs have already been expanded before constructing
 * this object.
 */
public class WildcardPath {
  private static final String STAR_REGEX = "(.*)";
  private static final String MORE_NESTED_FIELDS_TEMPLATE = "((%s).*)?";
  private final String parentGlobPath;
  private final String originalPattern;
  private final Pattern pattern;

  public WildcardPath(String parentGlobPath, String wildcardPath, char delim) {
    this.parentGlobPath = Preconditions.checkNotNull(parentGlobPath, "parentGlobPath");
    this.originalPattern = Preconditions.checkNotNull(wildcardPath, "wildcardPath");
    this.pattern = Pattern.compile(buildRegex(wildcardPath, delim));
  }

  public static String buildRegex(String wildcardPath, char delim) {
    if (wildcardPath.isEmpty()) {
      return wildcardPath;
    }

    String delimStr = Pattern.quote(Character.toString(delim));

    String[] splits = wildcardPath.split("\\*", -1); // -1 means keep trailing empty strings
    StringBuilder regex = new StringBuilder();

    for (int i = 0; i < splits.length; i++) {
      if ((i == 0 || i == splits.length - 1) && splits[i].isEmpty()) {
        // there was a * at the beginning or end of the string, so add a regex wildcard
        regex.append(STAR_REGEX);
        continue;
      }

      if (splits[i].isEmpty()) {
        // means there was a double asterisk, we've already
        // handled this just keep going.
        continue;
      }

      // don't treat this part of the string as a regex, escape
      // the entire thing
      regex.append(Pattern.quote(splits[i]));

      if (i < splits.length - 1) {
        // this isn't the last split, so add a *
        regex.append(STAR_REGEX);
      }
    }
    // x.y.z should match "x.y.z" and also "x.y.z.foo.bar"
    regex.append(String.format(MORE_NESTED_FIELDS_TEMPLATE, delimStr));
    return regex.toString();
  }

  public boolean matches(String path) {
    return pattern.matcher(path).matches();
  }

  public String getParentGlobPath() {
    return parentGlobPath;
  }

  public String getOriginalPattern() {
    return originalPattern;
  }

  @Override
  public String toString() {
    return String.format("WildcardPath(parentGlobPath: '%s', pattern: '%s')", parentGlobPath, originalPattern);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    WildcardPath wildcardPath = (WildcardPath) o;
    return originalPattern.equals(wildcardPath.originalPattern);
  }

  @Override
  public int hashCode() {
    return originalPattern.hashCode();
  }
}
