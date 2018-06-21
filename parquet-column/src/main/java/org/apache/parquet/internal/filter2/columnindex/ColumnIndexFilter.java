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
package org.apache.parquet.internal.filter2.columnindex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.UnboundRecordFilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate.Visitor;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.LtEq;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.NotEq;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * Filter implementation based on column indexes.
 * No filtering will be applied for columns where no column index is available.
 * No filtering will be applied at all if no offset index is available for any of the columns.
 */
public class ColumnIndexFilter implements Visitor<RowRanges> {
  private final ColumnIndexStore columnIndexStore;
  private final List<ColumnPath> columns;
  private final long rowCount;

  public static RowRanges calculateRowRanges(FilterCompat.Filter filter, ColumnIndexStore columnIndexStore,
      Collection<ColumnPath> paths, long rowCount) {
    return filter.accept(new FilterCompat.Visitor<RowRanges>() {
      @Override
      public RowRanges visit(FilterPredicateCompat filterPredicateCompat) {
        return filterPredicateCompat.getFilterPredicate()
            .accept(new ColumnIndexFilter(columnIndexStore, paths, rowCount));
      }

      @Override
      public RowRanges visit(UnboundRecordFilterCompat unboundRecordFilterCompat) {
        return RowRanges.single(rowCount);
      }

      @Override
      public RowRanges visit(NoOpFilter noOpFilter) {
        return RowRanges.single(rowCount);
      }
    });
  }

  private ColumnIndexFilter(ColumnIndexStore columnIndexStore, Collection<ColumnPath> paths, long rowCount) {
    // TODO[GS]: Handle the case of no offsetIndex or columnIndex is available: return all rows
    this.columnIndexStore = columnIndexStore;
    this.columns = new ArrayList<>(paths);
    this.rowCount = rowCount;
  }

  private RowRanges initRanges() {
    return RowRanges.single(rowCount);
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Eq<T> eq) {
    return initRanges();
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(NotEq<T> notEq) {
    return initRanges();
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Lt<T> lt) {
    return initRanges();
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(LtEq<T> ltEq) {
    return initRanges();
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Gt<T> gt) {
    return initRanges();
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(GtEq<T> gtEq) {
    return initRanges();
  }

  @Override
  public RowRanges visit(And and) {
    return initRanges();
  }

  @Override
  public RowRanges visit(Or or) {
    return initRanges();
  }

  @Override
  public RowRanges visit(Not not) {
    return initRanges();
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RowRanges visit(UserDefined<T, U> udp) {
    return initRanges();
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RowRanges visit(
      LogicalNotUserDefined<T, U> udp) {
    return initRanges();
  }

  // TODO[GS]: implement the methods by using set operations on RowRanges
}
