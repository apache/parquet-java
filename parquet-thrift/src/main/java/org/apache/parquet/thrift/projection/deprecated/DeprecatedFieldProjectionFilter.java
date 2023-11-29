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
package org.apache.parquet.thrift.projection.deprecated;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.parquet.thrift.projection.FieldsPath;
import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.projection.ThriftProjectionException;

/**
 * Filter thrift attributes using glob syntax.
 * This is used for parsing values assigned to ThriftReadSupport.THRIFT_COLUMN_FILTER_KEY
 */
@Deprecated
public class DeprecatedFieldProjectionFilter implements FieldProjectionFilter {
  public static final String PATTERN_SEPARATOR = ";";
  private final List<PathGlobPatternStatus> filterPatterns;

  /**
   * Class for remembering if a glob pattern has matched anything.
   * If there is an invalid glob pattern that matches nothing, it should throw.
   */
  @Deprecated
  private static class PathGlobPatternStatus {
    PathGlobPattern pattern;
    boolean hasMatchingPath = false;

    PathGlobPatternStatus(String pattern) {
      this.pattern = new PathGlobPattern(pattern);
    }

    public boolean matches(String path) {
      if (this.pattern.matches(path)) {
        this.hasMatchingPath = true;
        return true;
      } else {
        return false;
      }
    }
  }

  public DeprecatedFieldProjectionFilter(String filterDescStr) {
    Objects.requireNonNull(filterDescStr, "filterDescStr cannot be null");

    filterPatterns = new LinkedList<PathGlobPatternStatus>();

    if (filterDescStr == null || filterDescStr.isEmpty())
      return;

    String[] rawPatterns = filterDescStr.split(PATTERN_SEPARATOR);
    for (String rawPattern : rawPatterns) {
      filterPatterns.add(new PathGlobPatternStatus(rawPattern));
    }
  }

  @Override
  public boolean keep(FieldsPath path) {
    if (filterPatterns.isEmpty()) {
      return true;
    }

    for (PathGlobPatternStatus pattern : filterPatterns) {
      if (pattern.matches(path.toDelimitedString("/")))
        return true;
    }
    return false;
  }

  @Override
  public void assertNoUnmatchedPatterns() throws ThriftProjectionException {
    List<PathGlobPattern> unmatched = new LinkedList<PathGlobPattern>();
    for (PathGlobPatternStatus p : filterPatterns) {
      if (!p.hasMatchingPath) {
        unmatched.add(p.pattern);
      }
    }

    if (!unmatched.isEmpty()) {
      StringBuilder message =
          new StringBuilder("The following projection patterns did not match any columns in this schema:\n");
      for (PathGlobPattern p : unmatched) {
        message.append(p);
        message.append('\n');
      }
      throw new ThriftProjectionException(message.toString());
    }
  }
}
