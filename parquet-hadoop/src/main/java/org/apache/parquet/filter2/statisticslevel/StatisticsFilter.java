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
package org.apache.parquet.filter2.statisticslevel;

import static org.apache.parquet.filter2.compat.PredicateEvaluation.BLOCK_CANNOT_MATCH;
import static org.apache.parquet.filter2.compat.PredicateEvaluation.BLOCK_MIGHT_MATCH;
import static org.apache.parquet.filter2.compat.PredicateEvaluation.BLOCK_MUST_MATCH;
import static org.apache.parquet.filter2.compat.PredicateEvaluation.checkPredicate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.parquet.column.MinMax;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.compat.PredicateEvaluation;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Column;
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
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * Applies a {@link org.apache.parquet.filter2.predicate.FilterPredicate} to statistics about a group of
 * records.
 *
 * Note: the supplied predicate must not contain any instances of the not() operator as this is not
 * supported by this filter.
 *
 * the supplied predicate should first be run through {@link org.apache.parquet.filter2.predicate.LogicalInverseRewriter} to rewrite it
 * in a form that doesn't make use of the not() operator.
 *
 * the supplied predicate should also have already been run through
 * {@link org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator}
 * to make sure it is compatible with the schema of this file.
 *
 * Returns true if all the records represented by the statistics in the provided column metadata can be dropped.
 *         false otherwise (including when it is not known, which is often the case).
 */
// TODO: this belongs in the parquet-column project, but some of the classes here need to be moved too
// TODO: (https://issues.apache.org/jira/browse/PARQUET-38)
public class StatisticsFilter implements FilterPredicate.Visitor<Boolean> {

  public static Boolean evaluate(FilterPredicate pred, List<ColumnChunkMetaData> columns) {
    Objects.requireNonNull(pred, "pred cannot be null");
    Objects.requireNonNull(columns, "columns cannot be null");
    StatisticsFilter statisticsFilter = new StatisticsFilter(columns);
    Boolean predicate = pred.accept(statisticsFilter);
    checkPredicate(predicate);
    return predicate;
  }

  public static boolean canDrop(FilterPredicate pred, List<ColumnChunkMetaData> columns) {
    return evaluate(pred, columns) == BLOCK_CANNOT_MATCH;
  }

  private final Map<ColumnPath, ColumnChunkMetaData> columns = new HashMap<ColumnPath, ColumnChunkMetaData>();

  private StatisticsFilter(List<ColumnChunkMetaData> columnsList) {
    for (ColumnChunkMetaData chunk : columnsList) {
      columns.put(chunk.getPath(), chunk);
    }
  }

  private ColumnChunkMetaData getColumnChunk(ColumnPath columnPath) {
    return columns.get(columnPath);
  }

  // is this column chunk composed entirely of nulls?
  // assumes the column chunk's statistics is not empty
  private boolean isAllNulls(ColumnChunkMetaData column) {
    return column.getStatistics().getNumNulls() == column.getValueCount();
  }

  // are there any nulls in this column chunk?
  // assumes the column chunk's statistics is not empty
  private boolean hasNulls(ColumnChunkMetaData column) {
    return column.getStatistics().getNumNulls() > 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
    Column<T> filterColumn = eq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    T value = eq.getValue();

    if (meta == null) {
      // the column isn't in this file so all values are null.
      if (value != null) {
        // non-null is never null
        return BLOCK_CANNOT_MATCH;
      }
      return BLOCK_MIGHT_MATCH;
    }

    Statistics<T> stats = meta.getStatistics();

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (value == null) {
      // We don't know anything about the nulls in this chunk
      if (!stats.isNumNullsSet()) {
        return BLOCK_MIGHT_MATCH;
      }
      // we are looking for records where v eq(null)
      // so if there are no nulls in this chunk, we can drop it,
      // if there has nulls in this chunk, we must take it
      return !hasNulls(meta) ? BLOCK_CANNOT_MATCH : BLOCK_MUST_MATCH;
    }

    if (isAllNulls(meta)) {
      // we are looking for records where v eq(someNonNull)
      // and this is a column of all nulls, so drop it
      return BLOCK_CANNOT_MATCH;
    }

    if (!stats.hasNonNullValue()) {
      // stats does not contain min/max values, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    // we are looking for records where v eq(someNonNull)
    if (stats.compareMinToValue(value) > 0 || stats.compareMaxToValue(value) < 0) {
      // drop if value < min || value > max
      return BLOCK_CANNOT_MATCH;
    } else if (stats.compareMaxToValue(value) == 0
      && stats.comparator().compare(stats.genericGetMin(), stats.genericGetMax()) == 0) {
      // if max=min=value, we must take it
      return BLOCK_MUST_MATCH;
    } else {
      return BLOCK_MIGHT_MATCH;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(In<T> in) {
    Column<T> filterColumn = in.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    Set<T> values = in.getValues();

    if (meta == null) {
      // the column isn't in this file so all values are null.
      if (!values.contains(null)) {
        // non-null is never null
        return BLOCK_CANNOT_MATCH;
      }
      return BLOCK_MIGHT_MATCH;
    }

    Statistics<T> stats = meta.getStatistics();

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(meta)) {
      if (values.contains(null)) {
        // we are looking for records where v in(someNull)
        // so, if this is a column of all nulls, we must take it
        return BLOCK_MUST_MATCH;
      } else {
        // we are looking for records where v in(someNonNull)
        // so, if this is a column of all nulls, we must drop it
        return BLOCK_CANNOT_MATCH;
      }
    }

    if (!stats.hasNonNullValue()) {
      // stats does not contain min/max values, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (stats.isNumNullsSet()) {
      if (stats.getNumNulls() == 0) {
        if (values.contains(null) && values.size() == 1) return BLOCK_CANNOT_MATCH;
      } else {
        // we are looking for records where v in(someNull)
        // so, if this is a column which has nulls, we must take it
        if (values.contains(null)) return BLOCK_MUST_MATCH;
      }
    }

    MinMax<T> minMax = new MinMax(meta.getPrimitiveType().comparator(), values);
    T min = minMax.getMin();
    T max = minMax.getMax();

    // drop if all the element in value < min || all the element in value > max
    if (stats.compareMinToValue(max) <= 0 &&
      stats.compareMaxToValue(min) >= 0) {
      return BLOCK_MIGHT_MATCH;
    } else {
      return BLOCK_CANNOT_MATCH;
    }
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(NotIn<T> notIn) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
    Column<T> filterColumn = notEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    T value = notEq.getValue();

    if (meta == null) {
      if (value == null) {
        // null is always equal to null
        return BLOCK_CANNOT_MATCH;
      }
      return BLOCK_MIGHT_MATCH;
    }

    Statistics<T> stats = meta.getStatistics();

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (value == null) {
      // we are looking for records where v notEq(null)
      // so, if this is a column of all nulls, we can drop it
      // if this is a column where not all nulls , we must take it
      return isAllNulls(meta) ? BLOCK_CANNOT_MATCH : BLOCK_MUST_MATCH;
    }

    if (stats.isNumNullsSet() && hasNulls(meta)) {
      // we are looking for records where v notEq(someNonNull)
      // but this chunk contains nulls, so we must take it
      return BLOCK_MUST_MATCH;
    }

    if (!stats.hasNonNullValue()) {
      // stats does not contain min/max values, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    // we are looking for records where v notEq(someNonNull)
    if (stats.compareMinToValue(value) == 0 && stats.compareMaxToValue(value) == 0) {
      // drop if this is a column where min = max = value
      return BLOCK_CANNOT_MATCH;
    } else {
      // value != min or max, we must take it.
      return BLOCK_MUST_MATCH;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(Lt<T> lt) {
    Column<T> filterColumn = lt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never less than a
      // value. for all x, null is never < x.
      return BLOCK_CANNOT_MATCH;
    }

    Statistics<T> stats = meta.getStatistics();

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(meta)) {
      // we are looking for records where v < someValue
      // this chunk is all nulls, so we can drop it
      return BLOCK_CANNOT_MATCH;
    }

    if (!stats.hasNonNullValue()) {
      // stats does not contain min/max values, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    T value = lt.getValue();

    // we are looking for records where v < someValue
    if (stats.compareMinToValue(value) >= 0) {
      // drop if value <= min
      return BLOCK_CANNOT_MATCH;
    } else {
      // if value > min, we must take it
      return BLOCK_MUST_MATCH;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
    Column<T> filterColumn = ltEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never less than or
      // equal to a value. for all x, null is never <= x.
      return BLOCK_CANNOT_MATCH;
    }

    Statistics<T> stats = meta.getStatistics();

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(meta)) {
      // we are looking for records where v <= someValue
      // this chunk is all nulls, so we can drop it
      return BLOCK_CANNOT_MATCH;
    }

    if (!stats.hasNonNullValue()) {
      // stats does not contain min/max values, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    T value = ltEq.getValue();

    // we are looking for records where v <= someValue
    if (stats.compareMinToValue(value) > 0) {
      // drop if value < min
      return BLOCK_CANNOT_MATCH;
    } else {
      // if value >= min, we must take it
      return BLOCK_MUST_MATCH;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(Gt<T> gt) {
    Column<T> filterColumn = gt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never greater than a
      // value. for all x, null is never > x.
      return BLOCK_CANNOT_MATCH;
    }

    Statistics<T> stats = meta.getStatistics();

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(meta)) {
      // we are looking for records where v > someValue
      // this chunk is all nulls, so we can drop it
      return BLOCK_CANNOT_MATCH;
    }

    if (!stats.hasNonNullValue()) {
      // stats does not contain min/max values, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    T value = gt.getValue();

    // we are looking for records where v > someValue
    if (stats.compareMaxToValue(value) <= 0) {
      // drop if value >= max
      return BLOCK_CANNOT_MATCH;
    } else {
      // if value < max, we must take it
      return BLOCK_MUST_MATCH;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
    Column<T> filterColumn = gtEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never greater than or
      // equal to a value. for all x, null is never >= x.
      return BLOCK_CANNOT_MATCH;
    }

    Statistics<T> stats = meta.getStatistics();

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(meta)) {
      // we are looking for records where v >= someValue
      // this chunk is all nulls, so we can drop it
      return BLOCK_CANNOT_MATCH;
    }

    if (!stats.hasNonNullValue()) {
      // stats does not contain min/max values, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    T value = gtEq.getValue();

    // we are looking for records where v >= someValue
    if (stats.compareMaxToValue(value) < 0) {
      // drop if value > max
      return BLOCK_CANNOT_MATCH;
    } else {
      // if value <= max, we must take it
      return BLOCK_MUST_MATCH;
    }
  }

  @Override
  public Boolean visit(And and) {
    // seems unintuitive to put an || not an && here but we can
    // drop a chunk of records if we know that either the left or
    // the right predicate agrees that no matter what we don't
    // need this chunk.
    return PredicateEvaluation.evaluateAnd(and, this);
  }

  @Override
  public Boolean visit(Or or) {
    // seems unintuitive to put an && not an || here
    // but we can only drop a chunk of records if we know that
    // both the left and right predicates agree that no matter what
    // we don't need this chunk.
    return PredicateEvaluation.evaluateOr(or, this);
  }

  @Override
  public Boolean visit(Not not) {
    throw new IllegalArgumentException(
      "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }

  private <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> ud, boolean inverted) {
    Column<T> filterColumn = ud.getColumn();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    U udp = ud.getUserDefinedPredicate();

    if (columnChunk == null) {
      // the column isn't in this file so all values are null.
      // lets run the udp with null value to see if it keeps null or not.
      if (inverted) {
        return udp.acceptsNullValue() ? BLOCK_CANNOT_MATCH : BLOCK_MIGHT_MATCH;
      } else {
        return !udp.acceptsNullValue() ? BLOCK_CANNOT_MATCH : BLOCK_MIGHT_MATCH;
      }
    }

    Statistics<T> stats = columnChunk.getStatistics();

    if (stats.isEmpty()) {
      // we have no statistics available, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    if (isAllNulls(columnChunk)) {
      // lets run the udp with null value to see if it keeps null or not.
      if (inverted) {
        return udp.acceptsNullValue() ? BLOCK_CANNOT_MATCH : BLOCK_MIGHT_MATCH;
      } else {
        return !udp.acceptsNullValue() ? BLOCK_CANNOT_MATCH : BLOCK_MIGHT_MATCH;
      }
    }

    if (!stats.hasNonNullValue()) {
      // stats does not contain min/max values, we cannot drop any chunks
      return BLOCK_MIGHT_MATCH;
    }

    org.apache.parquet.filter2.predicate.Statistics<T> udpStats =
      new org.apache.parquet.filter2.predicate.Statistics<T>(stats.genericGetMin(), stats.genericGetMax(),
        stats.comparator());

    if (inverted) {
      return udp.inverseCanDrop(udpStats) ? BLOCK_CANNOT_MATCH : BLOCK_MIGHT_MATCH;
    } else {
      return udp.canDrop(udpStats) ? BLOCK_CANNOT_MATCH : BLOCK_MIGHT_MATCH;
    }
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> ud) {
    return visit(ud, false);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(LogicalNotUserDefined<T, U> lnud) {
    return visit(lnud.getUserDefined(), true);
  }

}
