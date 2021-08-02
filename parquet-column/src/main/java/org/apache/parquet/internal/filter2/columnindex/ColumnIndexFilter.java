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
import java.util.function.Function;

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
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter implementation based on column indexes.
 * No filtering will be applied for columns where no column index is available.
 * Offset index is required for all the columns in the projection, therefore a {@link MissingOffsetIndexException} will
 * be thrown from any {@code visit} methods if any of the required offset indexes is missing.
 */
public class ColumnIndexFilter implements Visitor<RowRanges> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnIndexFilter.class);
  private final ColumnIndexStore columnIndexStore;
  private final Set<ColumnPath> columns;
  private final long rowCount;
  private RowRanges allRows;

  /**
   * Calculates the row ranges containing the indexes of the rows might match the specified filter.
   *
   * @param filter
   *          to be used for filtering the rows
   * @param columnIndexStore
   *          the store for providing column/offset indexes
   * @param paths
   *          the paths of the columns used in the actual projection; a column not being part of the projection will be
   *          handled as containing {@code null} values only even if the column has values written in the file
   * @param rowCount
   *          the total number of rows in the row-group
   * @return the ranges of the possible matching row indexes; the returned ranges will contain all the rows if any of
   *         the required offset index is missing
   */
  public static RowRanges calculateRowRanges(FilterCompat.Filter filter, ColumnIndexStore columnIndexStore,
      Set<ColumnPath> paths, long rowCount) {
    return filter.accept(new FilterCompat.Visitor<RowRanges>() {
      @Override
      public RowRanges visit(FilterPredicateCompat filterPredicateCompat) {
        try {
          return filterPredicateCompat.getFilterPredicate()
              .accept(new ColumnIndexFilter(columnIndexStore, paths, rowCount));
        } catch (MissingOffsetIndexException e) {
          LOGGER.info(e.getMessage());
          return RowRanges.createSingle(rowCount);
        }
      }

      @Override
      public RowRanges visit(UnboundRecordFilterCompat unboundRecordFilterCompat) {
        return RowRanges.createSingle(rowCount);
      }

      @Override
      public RowRanges visit(NoOpFilter noOpFilter) {
        return RowRanges.createSingle(rowCount);
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
      allRows = RowRanges.createSingle(rowCount);
    }
    return allRows;
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Eq<T> eq) {
    return applyPredicate(eq.getColumn(), ci -> ci.visit(eq), eq.getValue() == null ? allRows() : RowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(NotEq<T> notEq) {
    return applyPredicate(notEq.getColumn(), ci -> ci.visit(notEq),
        notEq.getValue() == null ? RowRanges.EMPTY : allRows());
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Lt<T> lt) {
    return applyPredicate(lt.getColumn(), ci -> ci.visit(lt), RowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(LtEq<T> ltEq) {
    return applyPredicate(ltEq.getColumn(), ci -> ci.visit(ltEq), RowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(Gt<T> gt) {
    return applyPredicate(gt.getColumn(), ci -> ci.visit(gt), RowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>> RowRanges visit(GtEq<T> gtEq) {
    return applyPredicate(gtEq.getColumn(), ci -> ci.visit(gtEq), RowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RowRanges visit(UserDefined<T, U> udp) {
    return applyPredicate(udp.getColumn(), ci -> ci.visit(udp),
        udp.getUserDefinedPredicate().acceptsNullValue() ? allRows() : RowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RowRanges visit(
      LogicalNotUserDefined<T, U> udp) {
    return applyPredicate(udp.getUserDefined().getColumn(), ci -> ci.visit(udp),
        udp.getUserDefined().getUserDefinedPredicate().acceptsNullValue() ? RowRanges.EMPTY : allRows());
  }

  private RowRanges applyPredicate(Column<?> column, Function<ColumnIndex, PrimitiveIterator.OfInt> func,
      RowRanges rangesForMissingColumns) {
    ColumnPath columnPath = column.getColumnPath();
    if (!columns.contains(columnPath)) {
      return rangesForMissingColumns;
    }

    OffsetIndex oi = columnIndexStore.getOffsetIndex(columnPath);
    ColumnIndex ci = columnIndexStore.getColumnIndex(columnPath);
    if (ci == null) {
      LOGGER.info("No column index for column {} is available; Unable to filter on this column", columnPath);
      return allRows();
    }

    return RowRanges.create(rowCount, func.apply(ci), oi);
  }

  @Override
  public RowRanges visit(And and) {
    return RowRanges.intersection(and.getLeft().accept(this), and.getRight().accept(this));
  }

  @Override
  public RowRanges visit(Or or) {
    return RowRanges.union(or.getLeft().accept(this), or.getRight().accept(this));
  }

  @Override
  public RowRanges visit(Not not) {
    throw new IllegalArgumentException(
        "Predicates containing a NOT must be run through LogicalInverseRewriter. " + not);
  }
}
