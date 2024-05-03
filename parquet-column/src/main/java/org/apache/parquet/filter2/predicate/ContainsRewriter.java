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
package org.apache.parquet.filter2.predicate;

import java.util.Objects;
import org.apache.parquet.filter2.predicate.FilterPredicate.Visitor;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Contains;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.In;
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.LtEq;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.NotEq;
import org.apache.parquet.filter2.predicate.Operators.NotIn;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;

/**
 * Recursively rewrites Contains predicates composed using And or Or into a single Contains predicate
 * containing all predicate assertions.
 *
 * This is a performance optimization, as all composed Contains sub-predicates must share the same column, and
 * can therefore can be applied efficiently as a single predicate pass.
 */
public final class ContainsRewriter implements Visitor<FilterPredicate> {
  private static final ContainsRewriter INSTANCE = new ContainsRewriter();

  public static FilterPredicate rewrite(FilterPredicate pred) {
    Objects.requireNonNull(pred, "pred cannot be null");
    return pred.accept(INSTANCE);
  }

  private ContainsRewriter() {}

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Eq<T> eq) {
    return eq;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(NotEq<T> notEq) {
    return notEq;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Lt<T> lt) {
    return lt;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(LtEq<T> ltEq) {
    return ltEq;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Gt<T> gt) {
    return gt;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(GtEq<T> gtEq) {
    return gtEq;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(In<T> in) {
    return in;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(NotIn<T> notIn) {
    return notIn;
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Contains<T> contains) {
    return contains;
  }

  @Override
  public FilterPredicate visit(And and) {
    final FilterPredicate left;
    if (and.getLeft() instanceof And) {
      left = visit((And) and.getLeft());
    } else if (and.getLeft() instanceof Or) {
      left = visit((Or) and.getLeft());
    } else if (and.getLeft() instanceof Contains) {
      left = and.getLeft();
    } else {
      return and;
    }

    final FilterPredicate right;
    if (and.getRight() instanceof And) {
      right = visit((And) and.getRight());
    } else if (and.getRight() instanceof Or) {
      right = visit((Or) and.getRight());
    } else if (and.getRight() instanceof Contains) {
      right = and.getRight();
    } else {
      return and;
    }

    if (left instanceof Contains) {
      if (!(right instanceof Contains)) {
        throw new UnsupportedOperationException(
            "Contains predicates cannot be composed with non-Contains predicates");
      }
      return ((Contains) left).and(right);
    } else {
      return and;
    }
  }

  @Override
  public FilterPredicate visit(Or or) {
    final FilterPredicate left;
    if (or.getLeft() instanceof And) {
      left = visit((And) or.getLeft());
    } else if (or.getLeft() instanceof Or) {
      left = visit((Or) or.getLeft());
    } else if (or.getLeft() instanceof Contains) {
      left = or.getLeft();
    } else {
      return or;
    }

    final FilterPredicate right;
    if (or.getRight() instanceof And) {
      right = visit((And) or.getRight());
    } else if (or.getRight() instanceof Or) {
      right = visit((Or) or.getRight());
    } else if (or.getRight() instanceof Contains) {
      right = or.getRight();
    } else {
      return or;
    }

    if (left instanceof Contains) {
      if (!(right instanceof Contains)) {
        throw new UnsupportedOperationException(
            "Contains predicates cannot be composed with non-Contains predicates");
      }
      return ((Contains) left).or(right);
    } else {
      return or;
    }
  }

  @Override
  public FilterPredicate visit(Not not) {
    return not;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> FilterPredicate visit(UserDefined<T, U> udp) {
    return udp;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> FilterPredicate visit(
      LogicalNotUserDefined<T, U> udp) {
    return udp;
  }
}
