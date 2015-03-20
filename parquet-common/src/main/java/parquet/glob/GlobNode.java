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

import java.util.List;

interface GlobNode {
  <R> R accept(Visitor<R> visitor);

  static interface Visitor<T> {
    T visit(Atom atom);
    T visit(OneOf oneOf);
    T visit(GlobNodeSequence seq);
  }

  // just wraps a string, the base case in a glob pattern
  static class Atom implements GlobNode {
    private final String s;

    public Atom(String s) {
      this.s = s;
    }

    public String get() {
      return s;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      return getClass() == o.getClass() && s.equals(((Atom) o).s);
    }

    @Override
    public int hashCode() {
      return s.hashCode();
    }

    @Override
    public String toString() {
      return "Atom(" + s + ")";
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  // represents a {} group in a glob pattern, which
  // essentially means "one of the nodes in this group"
  static class OneOf implements GlobNode {
    private final List<GlobNode> children;

    public OneOf(List<GlobNode> children) {
      this.children = children;
    }

    public List<GlobNode> getChildren() {
      return children;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      return getClass() == o.getClass() && children.equals(((OneOf) o).children);
    }

    @Override
    public int hashCode() {
      return children.hashCode();
    }

    @Override
    public String toString() {
      return "OneOf" + children;
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  // A glob pattern is parsed into a GlobNodeSequence, an
  // ordered collection of GlobNodes. While this class looks
  // a lot like OneOf, it is logically interpreted differently.
  // A GlobNodeSequence doesn't represent one of many choices,
  // it represents many choices that come one after the other.
  static class GlobNodeSequence implements GlobNode {
    private final List<GlobNode> children;

    public GlobNodeSequence(List<GlobNode> children) {
      this.children = children;
    }

    public List<GlobNode> getChildren() {
      return children;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      return getClass() == o.getClass() && children.equals(((OneOf) o).children);
    }

    @Override
    public int hashCode() {
      return children.hashCode();
    }

    @Override
    public String toString() {
      return "GlobNodeSequence" + children;
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }
}
