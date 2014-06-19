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
import parquet.filter2.FilterPredicates.UserDefined;

/**
 * FilterPredicates are implemented in terms of the visitor pattern.
 */
public interface FilterPredicate {
  public static interface Visitor {
    <T> boolean visit(Eq<T> eq);
    <T> boolean visit(NotEq<T> notEq);
    <T> boolean visit(Lt<T> lt);
    <T> boolean visit(LtEq<T> ltEq);
    <T> boolean visit(Gt<T> gt);
    <T> boolean visit(GtEq<T> gtEq);
    boolean visit(And and);
    boolean visit(Or or);
    boolean visit(Not not);
    <T, U> boolean visit(UserDefined<T, U> udp);
  }

  boolean accept(Visitor visitor);
}
