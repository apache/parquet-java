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
import org.apache.parquet.filter2.predicate.Operators.ContainsAnd;
import org.apache.parquet.filter2.predicate.Operators.ContainsEq;
import org.apache.parquet.filter2.predicate.Operators.ContainsNotEq;
import org.apache.parquet.filter2.predicate.Operators.ContainsOr;
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
 * Converts a {@link FilterPredicate} to its logical inverse.
 * The returned predicate should be equivalent to not(p), but without
 * the use of a not() operator.
 * <p>
 * See also {@link LogicalInverseRewriter}, which can remove the use
 * of all not() operators without inverting the overall predicate.
 */
public final class LogicalInverter implements Visitor<FilterPredicate> {
  private static final LogicalInverter INSTANCE = new LogicalInverter();

  public static FilterPredicate invert(FilterPredicate pred) {
    Objects.requireNonNull(pred, "pred cannot be null");
    return pred.accept(INSTANCE);
  }

  private LogicalInverter() {}

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Eq<T> eq) {
    return new NotEq<>(eq.getColumn(), eq.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(NotEq<T> notEq) {
    return new Eq<>(notEq.getColumn(), notEq.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Lt<T> lt) {
    return new GtEq<>(lt.getColumn(), lt.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(LtEq<T> ltEq) {
    return new Gt<>(ltEq.getColumn(), ltEq.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Gt<T> gt) {
    return new LtEq<>(gt.getColumn(), gt.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(GtEq<T> gtEq) {
    return new Lt<>(gtEq.getColumn(), gtEq.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(In<T> in) {
    return new NotIn<>(in.getColumn(), in.getValues());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(NotIn<T> notIn) {
    return new In<>(notIn.getColumn(), notIn.getValues());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(ContainsEq<T> contains) {
    throw new UnsupportedOperationException("DoesNotContain not supported yet");
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(ContainsNotEq<T> contains) {
    throw new UnsupportedOperationException("DoesNotContainNotEq not supported yet!");
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(ContainsAnd<T> containsAnd) {
    return new ContainsOr<>(containsAnd.getLeft(), containsAnd.getRight());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(ContainsOr<T> containsOr) {
    return new ContainsAnd<>(containsOr.getLeft(), containsOr.getRight());
  }

  @Override
  public FilterPredicate visit(And and) {
    return new Or(and.getLeft().accept(this), and.getRight().accept(this));
  }

  @Override
  public FilterPredicate visit(Or or) {
    return new And(or.getLeft().accept(this), or.getRight().accept(this));
  }

  @Override
  public FilterPredicate visit(Not not) {
    return not.getPredicate();
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> FilterPredicate visit(UserDefined<T, U> udp) {
    return new LogicalNotUserDefined<>(udp);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> FilterPredicate visit(
      LogicalNotUserDefined<T, U> udp) {
    return udp.getUserDefined();
  }
}
