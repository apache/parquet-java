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
package org.apache.parquet.glob;

import java.util.List;

/**
 * A GlobNode represents a tree structure for describing a parsed glob pattern.
 * <p>
 * GlobNode uses the visitor pattern for tree traversal.
 * <p>
 * See {@link org.apache.parquet.Strings#expandGlob(String)}
 */
interface GlobNode {
  <R> R accept(Visitor<R> visitor);

  static interface Visitor<T> {
    T visit(Atom atom);

    T visit(OneOf oneOf);

    T visit(GlobNodeSequence seq);
  }

  /**
   * An Atom is just a String, it's a concrete String that is either part
   * of the top-level pattern, or one of the choices in a OneOf clause, or an
   * element in a GlobNodeSequence. In this sense it's the base case or leaf node
   * of a GlobNode tree.
   * <p>
   * For example, in pre{x,y{a,b}}post pre, x, y, z, b, and post are all Atoms.
   */
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
      return o != null && getClass() == o.getClass() && s.equals(((Atom) o).s);
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

  /**
   * A OneOf represents a {} clause in a glob pattern, which means
   * "one of the elements of this set must be satisfied", for example:
   * in pre{x,y} {x,y} is a OneOf, and in  or pre{x, {a,b}}post both {x, {a,b}}
   * and {a,b} are OneOfs.
   */
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
      return o != null && getClass() == o.getClass() && children.equals(((OneOf) o).children);
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

  /**
   * A GlobNodeSequence is an ordered collection of GlobNodes that must be satisfied in order,
   * and represents structures like pre{x,y}post or {x,y}{a,b}. In {test, pre{x,y}post}, pre{x,y}post is a
   * GlobNodeSequence. Unlike a OneOf, GlobNodeSequence's children have an ordering that is meaningful and
   * the requirements of its children must each be satisfied.
   */
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
      return o != null && getClass() == o.getClass() && children.equals(((OneOf) o).children);
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
