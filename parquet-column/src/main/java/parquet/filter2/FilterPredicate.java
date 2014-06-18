package parquet.filter2;

import parquet.filter2.FilterPredicates.And;
import parquet.filter2.FilterPredicates.Eq;
import parquet.filter2.FilterPredicates.Gt;
import parquet.filter2.FilterPredicates.GtEq;
import parquet.filter2.FilterPredicates.Lt;
import parquet.filter2.FilterPredicates.LtEq;
import parquet.filter2.FilterPredicates.Not;
import parquet.filter2.FilterPredicates.NotEq;
import parquet.filter2.FilterPredicates.Or;

/**
 * FilterPredicates are implemented in terms of the visitor pattern.
 */
public interface FilterPredicate {
  public static interface Visitor {
    boolean visit(Eq<?> eq);
    boolean visit(NotEq<?> notEq);
    boolean visit(Lt<?> lt);
    boolean visit(LtEq<?> ltEq);
    boolean visit(Gt<?> gt);
    boolean visit(GtEq<?> gtEq);
    boolean visit(And and);
    boolean visit(Or or);
    boolean visit(Not not);
  }

  boolean accept(Visitor visitor);
}
