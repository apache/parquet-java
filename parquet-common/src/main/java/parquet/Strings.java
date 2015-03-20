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

package parquet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import parquet.glob.GlobExpander;

public final class Strings {
  private Strings() { }

  /**
   * Join an Iterable of Strings into a single string with a delimiter.
   * For example, join(Arrays.asList("foo","","bar","x"), "|") would return
   * "foo||bar|x"
   *
   * @param s an iterable of strings
   * @param on the delimiter
   * @return a single joined string
   */
  public static String join(Iterable<String> s, String on) {
    Iterator<String> iter = s.iterator();
    StringBuilder sb = new StringBuilder();
    while (iter.hasNext()) {
      sb.append(iter.next());
      if (iter.hasNext()) {
        sb.append(on);
      }
    }
    return sb.toString();
  }

  /**
   * Join an Array of Strings into a single string with a delimiter.
   * For example, join(new String[] {"foo","","bar","x"}, "|") would return
   * "foo||bar|x"
   *
   * @param s an iterable of strings
   * @param on the delimiter
   * @return a single joined string
   */
  public static String join(String[] s, String on) {
    return join(Arrays.asList(s), on);
  }

  /**
   * Returns true if s.isEmpty() or s == null
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
   * start{a,b,c}end -> startaend, startbend, startcend
   * start{a,{b,c},d} -> startaend, startbend, startcend, startdend
   * {a,b,c} -> a, b, c
   * start{a, b{x,y}} -> starta, startbx, startby
   *
   * @param globPattern a string in the format described above
   * @return a list of all the strings that would satisfy globPattern, including duplicates
   */
  public static List<String> expandGlob(String globPattern) {
    return GlobExpander.expand(globPattern);
  }

}
