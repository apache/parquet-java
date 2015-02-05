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

import java.util.LinkedList;
import java.util.List;

/**
 * Filter thrift attributes using glob syntax.
 *
 * @author Tianshuo Deng
 */
public class FieldProjectionFilter {
  public static final String PATTERN_SEPARATOR = ";";
  List<PathGlobPattern> filterPatterns;

  public FieldProjectionFilter() {
    filterPatterns = new LinkedList<PathGlobPattern>();
  }

  public FieldProjectionFilter(String filterDescStr) {
    filterPatterns = new LinkedList<PathGlobPattern>();

    if (filterDescStr == null || filterDescStr.isEmpty())
      return;

    String[] rawPatterns = filterDescStr.split(PATTERN_SEPARATOR);
    for (String rawPattern : rawPatterns) {
      filterPatterns.add(new PathGlobPattern(rawPattern));
    }
  }

  public boolean isMatched(FieldsPath path) {
    if (filterPatterns.size() == 0)
      return true;

    for (int i = 0; i < filterPatterns.size(); i++) {
      if (matchPattern(path, filterPatterns.get(i)))
        return true;
    }
    return false;
  }

  private boolean matchPattern(FieldsPath path, PathGlobPattern filterPattern) {
    if (filterPattern.matches(path.toString())) {
      return true;
    } else {
      return false;
    }
  }
}
