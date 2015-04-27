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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Filter thrift attributes using glob syntax.
 *
 * @author Tianshuo Deng
 */
public class FieldProjectionFilter implements ProjectionFilter {
  public static final String PATTERN_SEPARATOR = ";";
  List<PathGlobPatternStatus> filterPatterns;
    private final Set<String> matchedPaths = new HashSet<String>();
    private final Set<String> unmatchedPaths = new HashSet<String>();

  /**
   * Class for remembering if a glob pattern has matched anything.
   * If there is an invalid glob pattern that matches nothing, it should throw.
   */
  private static class PathGlobPatternStatus {
    PathGlobPattern pattern;
    boolean hasMatchingPath = false;

    PathGlobPatternStatus(String pattern) {
      this.pattern = new PathGlobPattern(pattern);
    }

    public boolean matches(FieldsPath path) {
      if (this.pattern.matches(path.toString())) {
        this.hasMatchingPath = true;
        return true;
      } else {
        return false;
      }
    }
  }

  public FieldProjectionFilter() {
    filterPatterns = new LinkedList<PathGlobPatternStatus>();
  }

  public FieldProjectionFilter(String filterDescStr) {
    filterPatterns = new LinkedList<PathGlobPatternStatus>();

    if (filterDescStr == null || filterDescStr.isEmpty())
      return;

    String[] rawPatterns = filterDescStr.split(PATTERN_SEPARATOR);
    for (String rawPattern : rawPatterns) {
      filterPatterns.add(new PathGlobPatternStatus(rawPattern));
    }
  }

    @Override
  public boolean isMatched(FieldsPath path) {
        boolean result = false;

        if (filterPatterns.size() == 0) {
            matchedPaths.add(path.toString());
            result = true;
        }

        for (PathGlobPatternStatus filterPattern : filterPatterns) {

            if (filterPattern.matches(path)) {
                matchedPaths.add(path.toString());
                result = true;
            }
        }
        if (!result) {
            unmatchedPaths.add(path.toString());
        }
        return result;
  }

  public List<PathGlobPattern> getUnMatchedPatterns() {
    List<PathGlobPattern> unmatched = new LinkedList<PathGlobPattern>();
    for (PathGlobPatternStatus p : filterPatterns) {
      if (!p.hasMatchingPath) {
        unmatched.add(p.pattern);
      }
    }
    return unmatched;
  }

    public Set<String> getMatchedPaths() {
        return matchedPaths;
    }

    public Set<String> getUnmatchedPaths() {
        return unmatchedPaths;
    }
}
