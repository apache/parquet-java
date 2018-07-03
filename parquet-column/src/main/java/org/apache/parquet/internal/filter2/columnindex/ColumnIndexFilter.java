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

import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.UnboundRecordFilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate.Visitor;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Column;
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
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter implementation based on column indexes.
 * No filtering will be applied for columns where no column index is available.
 * No filtering will be applied at all if no offset index is available for any of the columns in the file.
 */
public class ColumnIndexFilter implements Visitor<RowRanges> {
  private static final Logger logger = LoggerFactory.getLogger(ColumnIndexFilter.class);
  private final ColumnIndexStore columnIndexStore;
  private final Set<ColumnPath> columns;
  private final long rowCount;
  private boolean missingOffsetIndex;
  private RowRanges allRows;

  public static RowRanges calculateRowRanges(FilterCompat.Filter filter, ColumnIndexStore columnIndexStore,
      Set<ColumnPath> paths, long rowCount) {
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

  private ColumnIndexFilter(ColumnIndexStore columnIndexStore, Set<ColumnPath> paths, long rowCount) {
    this.columnIndexStore = columnIndexStore;
    this.columns = paths;
    this.rowCount = rowCount;
  }

  private RowRanges allRows() {
    if (allRows == null) {
      allRows = RowRanges.single(rowCount);
    }
    return allRows;
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Eq<T> eq) {
    return applyPredicate(eq.getColumn(), ci -> ci.visit(eq));
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(NotEq<T> notEq) {
    return applyPredicate(notEq.getColumn(), ci -> ci.visit(notEq));
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Lt<T> lt) {
    return applyPredicate(lt.getColumn(), ci -> ci.visit(lt));
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(LtEq<T> ltEq) {
    return applyPredicate(ltEq.getColumn(), ci -> ci.visit(ltEq));
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Gt<T> gt) {
    return applyPredicate(gt.getColumn(), ci -> ci.visit(gt));
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(GtEq<T> gtEq) {
    return applyPredicate(gtEq.getColumn(), ci -> ci.visit(gtEq));
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RowRanges visit(UserDefined<T, U> udp) {
    return applyPredicate(udp.getColumn(), ci -> ci.visit(udp));
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RowRanges visit(
      LogicalNotUserDefined<T, U> udp) {
    return applyPredicate(udp.getUserDefined().getColumn(), ci -> ci.visit(udp));
  }

  private RowRanges applyPredicate(Column<?> column, Function<ColumnIndex, PrimitiveIterator.OfInt> func) {
    ColumnPath columnPath = column.getColumnPath();

    OffsetIndex oi = columnIndexStore.getOffsetIndex(columnPath);
    if (oi == null) {
      if (columns.contains(columnPath)) {
        missingOffsetIndex = true;
        logger.warn("No offset index for column {} is available; Unable to do filtering", columnPath);
      }
      return allRows();
    }

    ColumnIndex ci = columnIndexStore.getColumnIndex(columnPath);
    if (ci == null) {
      if (columns.contains(columnPath)) {
        logger.warn("No column index for column {} is available; Unable to filter on this column", columnPath);
      }
      return allRows();
    }

    return RowRanges.build(rowCount, func.apply(ci), oi);
  }

  @Override
  public RowRanges visit(And and) {
    return applyBinaryPredicate(() -> and.getLeft().accept(this), () -> and.getRight().accept(this),
        RowRanges::intersection);
  }

  @Override
  public RowRanges visit(Or or) {
    return applyBinaryPredicate(() -> or.getLeft().accept(this), () -> or.getRight().accept(this),
        RowRanges::union);
  }

  private RowRanges applyBinaryPredicate(Supplier<RowRanges> left, Supplier<RowRanges> right,
      BiFunction<RowRanges, RowRanges, RowRanges> combiner) {
    if (missingOffsetIndex) {
      return allRows();
    }
    RowRanges leftResult = left.get();
    if (missingOffsetIndex) {
      return allRows();
    }
    RowRanges rightResult = right.get();
    if (missingOffsetIndex) {
      return allRows();
    }
    return combiner.apply(leftResult, rightResult);
  }

  @Override
  public RowRanges visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }
}
