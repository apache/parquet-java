package parquet.filter2.compat;

import parquet.Log;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.LogicalInverseRewriter;

import static parquet.Preconditions.checkArgument;
import static parquet.Preconditions.checkNotNull;

public class FilterCompat {
  private static final Log LOG = Log.getLog(FilterCompat.class);

  public static interface Visitor<T> {
    T visit(FilterPredicateCompat filterPredicateCompat);
    T visit(UnboundRecordFilterCompat unboundRecordFilterCompat);
    T visit(NoOpFilter noOpFilter);
  }

  public static interface Filter {
    <R> R accept(Visitor<R> visitor);
  }

  public static final Filter NOOP = new NoOpFilter();

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

  public static Filter get(UnboundRecordFilter unboundRecordFilter) {
    return new UnboundRecordFilterCompat(unboundRecordFilter);
  }

  public static Filter get(Class<?> unboundRecordFilterClass) {
    checkNotNull(unboundRecordFilterClass, "unboundRecordFilterClass");
    try {
      UnboundRecordFilter unboundRecordFilter = (UnboundRecordFilter) unboundRecordFilterClass.newInstance();
      return get(unboundRecordFilter);
    } catch (InstantiationException e) {
      throw new RuntimeException("could not instantiate unbound record filter class", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("could not instantiate unbound record filter class", e);
    }
  }

  public static Filter get(FilterPredicate filterPredicate, Class<?> unboundRecordFilterClass) {
    checkArgument(filterPredicate == null || unboundRecordFilterClass == null,
        "Cannot provide both a FilterPredicate and an UnboundRecordFilter");

    if (filterPredicate != null) {
      return get(filterPredicate);
    }

    if (unboundRecordFilterClass != null) {
      return get(unboundRecordFilterClass);
    }

    return NOOP;
  }

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

  public static final class NoOpFilter implements Filter {
    private NoOpFilter() {}

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

}
