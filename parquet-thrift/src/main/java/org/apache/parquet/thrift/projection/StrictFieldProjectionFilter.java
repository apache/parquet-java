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
package org.apache.parquet.thrift.projection;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.Strings;
import org.apache.parquet.glob.WildcardPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stricter Implementation of {@link FieldProjectionFilter}.
 *
 * This supercedes {@code org.apache.parquet.thrift.projection.deprecated.DeprecatedFieldProjectionFilter}
 * whichx that allowed for more powerful glob patterns, but had less error reporting and less strict requirements.
 *
 * This filter requires that every *possible* expansion of glob expressions (like '{x,y,z}') must match at least one
 * column. Each expansion may match more than one if it contains wildcards ('*').
 *
 * Note that this class is stateful -- it keeps track of which expanded glob paths have matched a column, so that it can
 * throw when {@link #assertNoUnmatchedPatterns()} is called.
 */
public class StrictFieldProjectionFilter implements FieldProjectionFilter {
  private static final Logger LOG = LoggerFactory.getLogger(FieldProjectionFilter.class);
  private static final String GLOB_SEPARATOR = ";";

  // use a list instead of a Set, so we can detect overlapping patterns and
  // warn about it.
  private final List<WildcardPathStatus> columnsToKeep;

  // visible for testing
  static List<String> parseSemicolonDelimitedString(String columnsToKeepGlobs) {
    String[] splits = columnsToKeepGlobs.split(GLOB_SEPARATOR);
    List<String> globs = new ArrayList<String>();
    for (String s : splits) {
      if (!s.isEmpty()) {
        globs.add(s);
      }
    }

    if (globs.isEmpty()) {
      throw new ThriftProjectionException(String.format("Semicolon delimited string '%s' contains 0 glob strings",
          columnsToKeepGlobs));
    }

    return globs;
  }

  /**
   * Construct a StrictFieldProjectionFilter from a single string.
   *
   * columnsToKeepGlobs should be a list of Strings in the format expected by
   * {@link Strings#expandGlobToWildCardPaths(String, char)}, separated by ';'
   * Should only be used for parsing values out of the hadoop config -- for APIs
   * and programmatic access, use {@link #StrictFieldProjectionFilter(List)}.
   *
   * @param columnsToKeepGlobs glob pattern for columns to keep
   * @return a field projection filter
   */
  public static StrictFieldProjectionFilter fromSemicolonDelimitedString(String columnsToKeepGlobs) {
    return new StrictFieldProjectionFilter(parseSemicolonDelimitedString(columnsToKeepGlobs));
  }

  /**
   * Construct a StrictFieldProjectionFilter from a list of Strings in the format expected by
   * {@link Strings#expandGlobToWildCardPaths(String, char)}
   * @param columnsToKeepGlobs glob patterns for columns to keep
   */
  public StrictFieldProjectionFilter(List<String> columnsToKeepGlobs) {
    this.columnsToKeep = new ArrayList<WildcardPathStatus>();
    for (String glob : columnsToKeepGlobs) {
      for (WildcardPath wp : Strings.expandGlobToWildCardPaths(glob, '.')) {
        columnsToKeep.add(new WildcardPathStatus(wp));
      }
    }
  }

  @Override
  public boolean keep(FieldsPath path) {
    return keep(path.toDelimitedString("."));
  }

  // visible for testing
  boolean keep(String path) {
    WildcardPath match = null;

    // since we have a rule of every path must match at least one column,
    // we visit every single wildcard path, instead of short circuiting,
    // for the case where more than one pattern matches a column. Otherwise
    // we'd get a misleading exception saying a path didn't match a column,
    // even though it looks like it should have (but didn't because of short circuiting).
    // This also allows us log a warning when more than one glob path matches.
    for (WildcardPathStatus wp : columnsToKeep) {
      if (wp.matches(path)) {
        if (match != null && !match.getParentGlobPath().equals(wp.getWildcardPath().getParentGlobPath())) {
          String message = "Field path: '%s' matched more than one glob path pattern. First match: " +
              "'%s' (when expanded to '%s') second match:'%s' (when expanded to '%s')";

          warn(String.format(message,
              path, match.getParentGlobPath(), match.getOriginalPattern(),
              wp.getWildcardPath().getParentGlobPath(), wp.getWildcardPath().getOriginalPattern()));
        } else {
          match = wp.getWildcardPath();
        }
      }
    }

    return match != null;
  }

  // visible for testing
  protected void warn(String warning) {
    LOG.warn(warning);
  }

  private List<WildcardPath> getUnmatchedPatterns() {
    List<WildcardPath> unmatched = new ArrayList<WildcardPath>();
    for (WildcardPathStatus wp : columnsToKeep) {
      if (!wp.hasMatched()) {
        unmatched.add(wp.getWildcardPath());
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
   * Holds a WildcardPath and a boolean, used to track whether
   * this path has ever matched anything.
   */
  public static final class WildcardPathStatus {
    private final WildcardPath wildcardPath;
    private boolean hasMatched;

    public WildcardPathStatus(WildcardPath wildcardPath) {
      this.wildcardPath = wildcardPath;
      this.hasMatched = false;
    }

    public boolean matches(String path) {
      boolean matches = wildcardPath.matches(path);
      this.hasMatched = hasMatched || matches;
      return matches;
    }

    public WildcardPath getWildcardPath() {
      return wildcardPath;
    }

    public boolean hasMatched() {
      return hasMatched;
    }
  }

}
