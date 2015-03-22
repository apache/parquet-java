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

import parquet.Log;
import parquet.Strings;
import parquet.glob.WildcardPath;

public class StrictFieldProjectionFilter implements FieldProjectionFilter {
  private static final Log LOG = Log.getLog(FieldProjectionFilter.class);
  private static final String GLOB_SEPARATOR = ";";

  // use a list instead of a Set, so we can detect overlapping patterns and
  // warn about it.
  private final List<WildcardPathStatus> columnsToKeep;

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
    this.columnsToKeep = new ArrayList<WildcardPathStatus>();
    for (String glob : columnsToKeepGlobs) {
      for (WildcardPath wp : Strings.expandGlobToWildCardPaths(glob, '.')) {
        columnsToKeep.add(new WildcardPathStatus(wp));
      }
    }
  }

  @Override
  public boolean keep(FieldsPath path) {
    WildcardPath match = null;

    // since we have a rule of every path must match at least one column,
    // we visit every single wildcard path, instead of short circuiting,
    // for the case where more than one pattern matches a column. Otherwise
    // we'd get a misleading exception saying a path didn't match a column,
    // even though it looks like it should have (but didn't because of short circuiting).
    // This also allows us log a warning when more than one glob path matches.
    for (WildcardPathStatus wp : columnsToKeep) {
      if (wp.matches(path.toDelimitedString("."))) {
        if (match != null && !match.getParentGlobPath().equals(wp.getWildcardPath().getParentGlobPath())) {
          String message = "Field path: '%s' matched more than one glob path pattern. First match: " +
              "'%s' (when expanded to '%s') second match:'%s' (when expanded to '%s')";
          LOG.warn(String.format(message,
              path.toDelimitedString("."), match.getParentGlobPath(), match.getOriginalPattern(),
              wp.getWildcardPath().getParentGlobPath(), wp.getWildcardPath().getOriginalPattern()));
        } else {
          match = wp.getWildcardPath();
        }
      }
    }

    return match != null;
  }

  // visible for testing
  List<WildcardPath> getUnmatchedPatterns() {
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
