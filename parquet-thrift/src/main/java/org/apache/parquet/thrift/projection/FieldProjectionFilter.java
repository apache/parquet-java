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

import java.util.LinkedList;
import java.util.List;

/**
 * Filter thrift attributes using glob syntax.
 *
 * @author Tianshuo Deng
 */
public class FieldProjectionFilter {
  public static final String PATTERN_SEPARATOR = ";";
  List<PathGlobPatternStatus> filterPatterns;

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

  public boolean isMatched(FieldsPath path) {
    if (filterPatterns.size() == 0)
      return true;

    for (int i = 0; i < filterPatterns.size(); i++) {

      if (filterPatterns.get(i).matches(path))
        return true;
    }
    return false;
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

}
