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
package org.apache.parquet.filter2.compat;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;

import java.util.HashSet;
import java.util.Set;


public class FilterCompatColumnCollector implements FilterCompat.Visitor<Set<Operators.Column>> {

  public static final FilterCompatColumnCollector INSTANCE = new FilterCompatColumnCollector();

  private FilterCompatColumnCollector() {
  }

  @Override
  public Set<Operators.Column> visit(FilterCompat.FilterPredicateCompat filterPredicateCompat) {
    FilterPredicateColumnCollector collector = new FilterPredicateColumnCollector();
    filterPredicateCompat.getFilterPredicate().accept(collector);
    return collector.getColumnSet();
  }

  @Override
  public Set<Operators.Column> visit(FilterCompat.UnboundRecordFilterCompat unboundRecordFilterCompat) {
    /* we return null to implicitly ignore the   */
    return null;
  }

  @Override
  public Set<Operators.Column> visit(FilterCompat.NoOpFilter noOpFilter) {
    return null;
  }

  /**
   * This class is stateful, and not thread-safe
   */
  private static class FilterPredicateColumnCollector implements FilterPredicate.Visitor<Boolean> {

    private final HashSet<Operators.Column> columnSet = new HashSet<Operators.Column>();

    @Override
    public <T extends Comparable<T>> Boolean visit(Operators.Eq<T> eq) {
      return columnSet.add(eq.getColumn());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Operators.NotEq<T> notEq) {
      return columnSet.add(notEq.getColumn());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Operators.Lt<T> lt) {
      return columnSet.add(lt.getColumn());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Operators.LtEq<T> ltEq) {
      return columnSet.add(ltEq.getColumn());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Operators.Gt<T> gt) {
      return columnSet.add(gt.getColumn());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Operators.GtEq<T> gtEq) {
      return columnSet.add(gtEq.getColumn());
    }

    @Override
    public Boolean visit(Operators.And and) {
      and.getLeft().accept(this);
      return and.getRight().accept(this);
    }

    @Override
    public Boolean visit(Operators.Or or) {
      or.getLeft().accept(this);
      return or.getRight().accept(this);
    }

    @Override
    public Boolean visit(Operators.Not not) {
      return not.getPredicate().accept(this);
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
        Operators.UserDefined<T, U> udp) {
      return columnSet.add(udp.getColumn());
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
        Operators.LogicalNotUserDefined<T, U> udp) {

      return udp.getUserDefined().accept(this);
    }

    public HashSet<Operators.Column> getColumnSet() {
      return columnSet;
    }
  }
}