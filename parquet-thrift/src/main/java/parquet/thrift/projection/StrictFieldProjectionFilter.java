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
import java.util.List;
import java.util.regex.Pattern;

import parquet.Log;
import parquet.Preconditions;
import parquet.Strings;

public class StrictFieldProjectionFilter implements FieldProjectionFilter {
  private static final Log LOG = Log.getLog(FieldProjectionFilter.class);
  private static final String GLOB_SEPARATOR = ";";

  // use a list instead of a Set, so we can detect overlapping patterns and
  // warn about it.
  private final List<WildcardPath> columnsToKeep;

  public static StrictFieldProjectionFilter fromSemicolonDelimitedString(String columnsToKeepGlobs) {
    String[] splits = columnsToKeepGlobs.split(GLOB_SEPARATOR);
    List<String> globs = new ArrayList<String>();
    for (String s : splits) {
      if (!s.isEmpty()) {
        globs.add(s);
      }
    }
    return new StrictFieldProjectionFilter(globs);
  }

  public StrictFieldProjectionFilter(List<String> columnsToKeepGlobs) {
    this.columnsToKeep = new ArrayList<WildcardPath>();
    for (String glob : columnsToKeepGlobs) {
      for (String expandedGlob : Strings.expandGlob(glob)) {
        this.columnsToKeep.add(new WildcardPath(glob, expandedGlob));
      }
    }
  }

  @Override
  public boolean matches(FieldsPath path) {
    WildcardPath match = null;

    // since we have a rule of every path must match at least one column,
    // we visit every single wildcard path, instead of short circuiting,
    // for the case where more than one pattern matches a column. Otherwise
    // we'd get a misleading exception saying a path didn't match a column,
    // even though it looks like it should have (but didn't because of short circuiting).
    // This also allows us log a warning when more than one glob path matches.
    for (WildcardPath wp : columnsToKeep) {
      if (wp.matches(path.toDelimitedString("."))) {
        if (match != null && !match.getParentGlobPath().equals(wp.getParentGlobPath())) {
          String message = "Field path: '%s' matched more than one glob path pattern. First match: " +
              "'%s' (when expanded to '%s') second match:'%s' (when expanded to '%s')";
          LOG.warn(String.format(message,
              path, match.getParentGlobPath(), match.getOriginalPattern(),
              wp.getParentGlobPath(), wp.getOriginalPattern()));
        } else {
          match = wp;
        }
      }
    }

    return match != null;
  }

  // visible for testing
  List<WildcardPath> getUnmatchedPatterns() {
    List<WildcardPath> unmatched = new ArrayList<WildcardPath>();
    for (WildcardPath wp : columnsToKeep) {
      if (!wp.hasMatched()) {
        unmatched.add(wp);
      }
    }
    return unmatched;
  }

  @Override
  public void assertNoUnmatchedPatterns() throws ThriftProjectionException{
    List<WildcardPath> unmatched = getUnmatchedPatterns();
    if (!unmatched.isEmpty()) {
      StringBuilder message =
          new StringBuilder("The following projection patterns did not match any columns in this schema:\n");
      for (WildcardPath wp : unmatched) {
        message.append(String.format("Pattern: '%s' (when expanded to '%s')",
            wp.getParentGlobPath(), wp.getOriginalPattern()));
        message.append('\n');
      }
      throw new ThriftProjectionException(message.toString());
    }
  }

  /**
   * Holds a String with wildcards '*', for example:
   * "foo.*.baz" or "foo*baz.bar*"
   *
   * All other characters are not treated as special characters, including '{', '}', and '.'
   * It is assumed that {} globs have already been expanded before constructing
   * this object.
   */
  public static class WildcardPath {
    private static final String STAR_REGEX = "(.*)";
    private static final String MORE_NESTED_FIELDS_REGEX = "(\\..*)?";
    private final String parentGlobPath;
    private final String originalPattern;
    private final Pattern pattern;
    private boolean hasMatched = false;

    public WildcardPath(String parentGlobPath, String wildcardPath) {
      this.parentGlobPath = Preconditions.checkNotNull(parentGlobPath, "parentGlobPath");
      this.originalPattern = Preconditions.checkNotNull(wildcardPath, "wildcardPath");
      this.pattern = Pattern.compile(buildRegex(wildcardPath));
    }

    public static String buildRegex(String wildcardPath) {
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
      regex.append(MORE_NESTED_FIELDS_REGEX);
      return regex.toString();
    }

    public boolean matches(String path) {
      boolean matches = pattern.matcher(path).matches();
      if (matches) {
        hasMatched = true;
      }
      return matches;
    }

    public boolean hasMatched() {
      return hasMatched;
    }

    public String getParentGlobPath() {
      return parentGlobPath;
    }

    public String getOriginalPattern() {
      return originalPattern;
    }

    @Override
    public String toString() {
      return String.format("WildcardPath(parentGlobPath: '%s', pattern: '%s', hasMatched: '%s')",
          parentGlobPath, originalPattern, hasMatched);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WildcardPath wildcardPath = (WildcardPath) o;
      return originalPattern.equals(wildcardPath.originalPattern);
    }

    @Override
    public int hashCode() {
      return originalPattern.hashCode();
    }
  }
}
