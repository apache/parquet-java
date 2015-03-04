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
package parquet.filter2.compat;

import parquet.Log;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.LogicalInverseRewriter;

import static parquet.Preconditions.checkArgument;
import static parquet.Preconditions.checkNotNull;

/**
 * Parquet currently has two ways to specify a filter for dropping records at read time.
 * The first way, that only supports filtering records during record assembly, is found
 * in {@link parquet.filter}. The new API (found in {@link parquet.filter2}) supports
 * also filtering entire rowgroups of records without reading them at all.
 *
 * This class defines a common interface that both of these filters share,
 * {@link Filter}. A Filter can be either an {@link UnboundRecordFilter} from the old API, or
 * a {@link FilterPredicate} from the new API, or a sentinel no-op filter.
 *
 * Having this common interface simplifies passing a filter through the read path of parquet's
 * codebase.
 */
public class FilterCompat {
  private static final Log LOG = Log.getLog(FilterCompat.class);

  /**
   * Anyone wanting to use a {@link Filter} need only implement this interface,
   * per the visitor pattern.
   */
  public static interface Visitor<T> {
    T visit(FilterPredicateCompat filterPredicateCompat);
    T visit(UnboundRecordFilterCompat unboundRecordFilterCompat);
    T visit(NoOpFilter noOpFilter);
  }

  public static interface Filter {
    <R> R accept(Visitor<R> visitor);
  }

  // sentinel no op filter that signals "do no filtering"
  public static final Filter NOOP = new NoOpFilter();

  /**
   * Given a FilterPredicate, return a Filter that wraps it.
   * This method also logs the filter being used and rewrites
   * the predicate to not include the not() operator.
   */
  public static Filter get(FilterPredicate filterPredicate) {
    checkNotNull(filterPredicate, "filterPredicate");

    LOG.info("Filtering using predicate: " + filterPredicate);

    // rewrite the predicate to not include the not() operator
    FilterPredicate collapsedPredicate = LogicalInverseRewriter.rewrite(filterPredicate);

    if (!filterPredicate.equals(collapsedPredicate)) {
      LOG.info("Predicate has been collapsed to: " + collapsedPredicate);
    }

    return new FilterPredicateCompat(collapsedPredicate);
  }

  /**
   * Given an UnboundRecordFilter, return a Filter that wraps it.
   */
  public static Filter get(UnboundRecordFilter unboundRecordFilter) {
    return new UnboundRecordFilterCompat(unboundRecordFilter);
  }

  /**
   * Given either a FilterPredicate or the class of an UnboundRecordFilter, or neither (but not both)
   * return a Filter that wraps whichever was provided.
   *
   * Either filterPredicate or unboundRecordFilterClass must be null, or an exception is thrown.
   *
   * If both are null, the no op filter will be returned.
   */
  public static Filter get(FilterPredicate filterPredicate, UnboundRecordFilter unboundRecordFilter) {
    checkArgument(filterPredicate == null || unboundRecordFilter == null,
        "Cannot provide both a FilterPredicate and an UnboundRecordFilter");

    if (filterPredicate != null) {
      return get(filterPredicate);
    }

    if (unboundRecordFilter != null) {
      return get(unboundRecordFilter);
    }

    return NOOP;
  }

  // wraps a FilterPredicate
  public static final class FilterPredicateCompat implements Filter {
    private final FilterPredicate filterPredicate;

    private FilterPredicateCompat(FilterPredicate filterPredicate) {
      this.filterPredicate = checkNotNull(filterPredicate, "filterPredicate");
    }

    public FilterPredicate getFilterPredicate() {
      return filterPredicate;
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  // wraps an UnboundRecordFilter
  public static final class UnboundRecordFilterCompat implements Filter {
    private final UnboundRecordFilter unboundRecordFilter;

    private UnboundRecordFilterCompat(UnboundRecordFilter unboundRecordFilter) {
      this.unboundRecordFilter = checkNotNull(unboundRecordFilter, "unboundRecordFilter");
    }

    public UnboundRecordFilter getUnboundRecordFilter() {
      return unboundRecordFilter;
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  // sentinel no op filter
  public static final class NoOpFilter implements Filter {
    private NoOpFilter() {}

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

}
