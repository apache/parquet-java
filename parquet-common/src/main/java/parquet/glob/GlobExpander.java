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
package parquet.glob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import parquet.glob.GlobNode.Atom;
import parquet.glob.GlobNode.GlobNodeSequence;
import parquet.glob.GlobNode.OneOf;

/**
 * Implementation of {@link parquet.Strings#expandGlob(String)}
 */
public final class GlobExpander {
  private GlobExpander()  { }

  /**
   * See {@link parquet.Strings#expandGlob(String)} for docs.
   */
  public static List<String> expand(String globPattern) {
    return GlobExpanderImpl.expand(GlobParser.parse(globPattern));
  }

  /**
   * Transforms a tree of {@link GlobNode} into a list of all the strings that satisfy
   * this tree.
   */
  private final static class GlobExpanderImpl implements GlobNode.Visitor<List<String>> {
    private static final GlobExpanderImpl INSTANCE = new GlobExpanderImpl();

    private GlobExpanderImpl() {}

    public static List<String> expand(GlobNode node) {
      return node.accept(INSTANCE);
    }

    @Override
    public List<String> visit(Atom atom) {
      // atoms are the base case, just return a singleton list
      return Arrays.asList(atom.get());
    }

    @Override
    public List<String> visit(OneOf oneOf) {
      // in the case of OneOf, we just need to take all of
      // the possible values the OneOf represents and
      // union them together
      List<String> results = new ArrayList<String>();
      for (GlobNode n : oneOf.getChildren()) {
        results.addAll(n.accept(this));
      }
      return results;
    }

    @Override
    public List<String> visit(GlobNodeSequence seq) {
      // in the case of a sequence, for each child
      // we need to expand the child into all of its
      // possibilities, then do a cross product of
      // all the children, in order.

      List<String> results = new ArrayList<String>();
      for (GlobNode n : seq.getChildren()) {
        results = crossOrTakeNonEmpty(results, n.accept(this));
      }
      return results;
    }

    /**
     * Computes the cross product of two lists by adding each string in list1 to each string in list2.
     * If one of the lists is empty, a copy of the other list is returned.
     * If both are empty, an empty list is returned.
     */
    public static List<String> crossOrTakeNonEmpty(List<String> list1, List<String> list2) {
      if (list1.isEmpty()) {
        ArrayList<String> result = new ArrayList<String>(list2.size());
        result.addAll(list2);
        return result;
      }

      if (list2.isEmpty()) {
        ArrayList<String> result = new ArrayList<String>(list1.size());
        result.addAll(list1);
        return result;
      }

      List<String> result = new ArrayList<String>(list1.size() * list2.size());
      for (String s1 : list1) {
        for (String s2 : list2) {
          result.add(s1 + s2);
        }
      }
      return result;
    }
  }
}