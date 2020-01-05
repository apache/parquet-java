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

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.glob.GlobExpander;
import org.apache.parquet.glob.WildcardPath;

public final class Strings {
  private Strings() { }

  /**
   * Returns true if s.isEmpty() or s == null
   *
   * @param s a string that may be null or empty
   * @return true if the string s is null or is empty
   */
  public static boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }

  /**
   * Expands a string with braces ("{}") into all of its possible permutations.
   * We call anything inside of {} braces a "one-of" group.
   *
   * The only special characters in this glob syntax are '}', '{' and ','
   *
   * The top-level pattern must not contain any commas, but a "one-of" group separates
   * its elements with commas, and a one-of group may contain sub one-of groups.
   *
   * For example:
   * start{a,b,c}end -&gt; startaend, startbend, startcend
   * start{a,{b,c},d} -&gt; startaend, startbend, startcend, startdend
   * {a,b,c} -&gt; a, b, c
   * start{a, b{x,y}} -&gt; starta, startbx, startby
   *
   * @param globPattern a string in the format described above
   * @return a list of all the strings that would satisfy globPattern, including duplicates
   */
  public static List<String> expandGlob(String globPattern) {
    return GlobExpander.expand(globPattern);
  }

  /**
   * Expands a string according to {@link #expandGlob(String)}, and then constructs a {@link WildcardPath}
   * for each expanded result which can be used to match strings as described in {@link WildcardPath}.
   *
   * @param globPattern a String to be passed to {@link #expandGlob(String)}
   * @param delim the delimeter used by {@link WildcardPath}
   * @return a list of wildcard paths, one for each expanded result
   */
  public static List<WildcardPath> expandGlobToWildCardPaths(String globPattern, char delim) {
    List<WildcardPath> ret = new ArrayList<WildcardPath>();
    for (String expandedGlob : Strings.expandGlob(globPattern)) {
      ret.add(new WildcardPath(globPattern, expandedGlob, delim));
    }
    return ret;
  }
}
