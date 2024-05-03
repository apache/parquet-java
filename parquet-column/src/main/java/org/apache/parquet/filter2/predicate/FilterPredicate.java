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
 * A FilterPredicate is an expression tree describing the criteria for which records to keep when loading data from
 * a parquet file. These predicates are applied in multiple places. Currently, they are applied to all row groups at
 * job submission time to see if we can potentially drop entire row groups, and then they are applied during column
 * assembly to drop individual records that are not wanted.
 * <p>
 * FilterPredicates do not contain closures or instances of anonymous classes, rather they are expressed as
 * an expression tree of operators.
 * <p>
 * FilterPredicates are implemented in terms of the visitor pattern.
 * <p>
 * See {@link Operators} for the implementation of the operator tokens,
 * and {@link FilterApi} for the dsl functions for constructing an expression tree.
 */
public interface FilterPredicate {

  /**
   * A FilterPredicate must accept a Visitor, per the visitor pattern.
   *
   * @param visitor a visitor
   * @param <R>     return type of the visitor
   * @return the return value of Visitor#visit(this)
   */
  <R> R accept(Visitor<R> visitor);

  /**
   * A FilterPredicate Visitor must visit all the operators in a FilterPredicate expression tree,
   * and must handle recursion itself, per the visitor pattern.
   *
   * @param <R> return type of the visitor
   */
  public static interface Visitor<R> {
    <T extends Comparable<T>> R visit(Eq<T> eq);

    <T extends Comparable<T>> R visit(NotEq<T> notEq);

    <T extends Comparable<T>> R visit(Lt<T> lt);

    <T extends Comparable<T>> R visit(LtEq<T> ltEq);

    <T extends Comparable<T>> R visit(Gt<T> gt);

    <T extends Comparable<T>> R visit(GtEq<T> gtEq);

    default <T extends Comparable<T>> R visit(In<T> in) {
      throw new UnsupportedOperationException("visit in is not supported.");
    }

    default <T extends Comparable<T>> R visit(NotIn<T> notIn) {
      throw new UnsupportedOperationException("visit NotIn is not supported.");
    }

    default <T extends Comparable<T>> R visit(Contains<T> contains) {
      throw new UnsupportedOperationException("visit Contains is not supported.");
    }

    R visit(And and);

    R visit(Or or);

    R visit(Not not);

    <T extends Comparable<T>, U extends UserDefinedPredicate<T>> R visit(UserDefined<T, U> udp);

    <T extends Comparable<T>, U extends UserDefinedPredicate<T>> R visit(LogicalNotUserDefined<T, U> udp);
  }
}
