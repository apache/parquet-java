package parquet.filter2;

import parquet.filter2.FilterPredicateOperators.And;
import parquet.filter2.FilterPredicateOperators.Eq;
import parquet.filter2.FilterPredicateOperators.Gt;
import parquet.filter2.FilterPredicateOperators.GtEq;
import parquet.filter2.FilterPredicateOperators.LogicalNotUserDefined;
import parquet.filter2.FilterPredicateOperators.Lt;
import parquet.filter2.FilterPredicateOperators.LtEq;
import parquet.filter2.FilterPredicateOperators.Not;
import parquet.filter2.FilterPredicateOperators.NotEq;
import parquet.filter2.FilterPredicateOperators.Or;
import parquet.filter2.FilterPredicateOperators.UserDefined;
import parquet.filter2.UserDefinedPredicates.UserDefinedPredicate;

/**
 * A FilterPredicate is an expression tree describing the criteria for which records to keep when loading data from
 * a parquet file. These predicates are applied in multiple places. Currently, they are applied to all row groups at
 * job submission time to see if we can potentially drop entire row groups, and then they are applied during column
 * assembly to skip assembly of records that are not wanted.
 *
 * FilterPredicates do not contain closures or instances of anonymous classes, rather they are expressed as
 * an expression tree of operators.
 *
 * FilterPredicates are implemented in terms of the visitor pattern.
 *
 * See {@link FilterPredicateOperators} for the implementation of the operator tokens,
 * and {@link parquet.filter2.Filter} for the dsl functions for constructing an expression tree.
 */
public interface FilterPredicate {

  /**
   * A FilterPredicate must accept a Visitor, per the visitor pattern.
   */
  <R> R accept(Visitor<R> visitor);

  /**
   * A FilterPredicate Visitor must visit all the operators in a FilterPredicate expression tree,
   * and must handle recursion itself, per the visitor pattern.
   */
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

}
