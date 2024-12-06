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

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

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
import org.apache.parquet.filter2.predicate.Operators.Size;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;

/**
 * Recursively removes all use of the not() operator in a predicate
 * by replacing all instances of not(x) with the inverse(x),
 * eg: not(and(eq(), not(eq(y))) -&gt; or(notEq(), eq(y))
 * <p>
 * The returned predicate should have the same meaning as the original, but
 * without the use of the not() operator.
 * <p>
 * See also {@link LogicalInverter}, which is used
 * to do the inversion.
 */
public final class LogicalInverseRewriter implements Visitor<FilterPredicate> {
  private static final LogicalInverseRewriter INSTANCE = new LogicalInverseRewriter();

  public static FilterPredicate rewrite(FilterPredicate pred) {
    Objects.requireNonNull(pred, "pred cannot be null");
    return pred.accept(INSTANCE);
  }

  private LogicalInverseRewriter() {}

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
  public FilterPredicate visit(Size size) {
    return size;
  }

  @Override
  public FilterPredicate visit(And and) {
    return and(and.getLeft().accept(this), and.getRight().accept(this));
  }

  @Override
  public FilterPredicate visit(Or or) {
    return or(or.getLeft().accept(this), or.getRight().accept(this));
  }

  @Override
  public FilterPredicate visit(Not not) {
    return LogicalInverter.invert(not.getPredicate().accept(this));
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
