package parquet.filter2;

import parquet.filter2.FilterPredicates.And;
import parquet.filter2.FilterPredicates.Eq;
import parquet.filter2.FilterPredicates.Gt;
import parquet.filter2.FilterPredicates.GtEq;
import parquet.filter2.FilterPredicates.LogicalNotUserDefined;
import parquet.filter2.FilterPredicates.Lt;
import parquet.filter2.FilterPredicates.LtEq;
import parquet.filter2.FilterPredicates.Not;
import parquet.filter2.FilterPredicates.NotEq;
import parquet.filter2.FilterPredicates.Or;
import parquet.filter2.FilterPredicates.UserDefined;
import parquet.filter2.UserDefinedPredicates.UserDefinedPredicate;

/**
 * FilterPredicates are implemented in terms of the visitor pattern.
 */
public interface FilterPredicate {

  public static interface Visitor<R> {
    <T> R visit(Eq<T> eq);
    <T> R visit(NotEq<T> notEq);
    <T> R visit(Lt<T> lt);
    <T> R visit(LtEq<T> ltEq);
    <T> R visit(Gt<T> gt);
    <T> R visit(GtEq<T> gtEq);
    R visit(And and);
    R visit(Or or);
    R visit(Not not);
    <T, U extends UserDefinedPredicate<T>> R visit(UserDefined<T, U> udp);
    <T, U extends UserDefinedPredicate<T>> R visit(LogicalNotUserDefined<T, U> udp);
  }

  <R> R accept(Visitor<R> visitor);

}
