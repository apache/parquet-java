package parquet.filter2.predicate;

import java.io.Serializable;

import parquet.filter2.predicate.Operators.And;
import parquet.filter2.predicate.Operators.ConfiguredUserDefined;
import parquet.filter2.predicate.Operators.Eq;
import parquet.filter2.predicate.Operators.Gt;
import parquet.filter2.predicate.Operators.GtEq;
import parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import parquet.filter2.predicate.Operators.LogicalNotConfiguredUserDefined;
import parquet.filter2.predicate.Operators.Lt;
import parquet.filter2.predicate.Operators.LtEq;
import parquet.filter2.predicate.Operators.Not;
import parquet.filter2.predicate.Operators.NotEq;
import parquet.filter2.predicate.Operators.Or;
import parquet.filter2.predicate.Operators.UserDefined;
/**
 * A FilterPredicate is an expression tree describing the criteria for which records to keep when loading data from
 * a parquet file. These predicates are applied in multiple places. Currently, they are applied to all row groups at
 * job submission time to see if we can potentially drop entire row groups, and then they are applied during column
 * assembly to drop individual records that are not wanted.
 *
 * FilterPredicates do not contain closures or instances of anonymous classes, rather they are expressed as
 * an expression tree of operators.
 *
 * FilterPredicates are implemented in terms of the visitor pattern.
 *
 * See {@link Operators} for the implementation of the operator tokens,
 * and {@link FilterApi} for the dsl functions for constructing an expression tree.
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
    <T extends Comparable<T>> R visit(Eq<T> eq);
    <T extends Comparable<T>> R visit(NotEq<T> notEq);
    <T extends Comparable<T>> R visit(Lt<T> lt);
    <T extends Comparable<T>> R visit(LtEq<T> ltEq);
    <T extends Comparable<T>> R visit(Gt<T> gt);
    <T extends Comparable<T>> R visit(GtEq<T> gtEq);
    R visit(And and);
    R visit(Or or);
    R visit(Not not);
    <T extends Comparable<T>, U extends UserDefinedPredicate<T> > R visit(UserDefined<T, U> udp);
    <T extends Comparable<T>, U extends UserDefinedPredicate<T> & Serializable > R visit(ConfiguredUserDefined<T, U> udp);
    <T extends Comparable<T>, U extends UserDefinedPredicate<T> > R visit(LogicalNotUserDefined<T, U> udp);
    <T extends Comparable<T>, U extends UserDefinedPredicate<T> & Serializable > R visit(LogicalNotConfiguredUserDefined<T, U> udp);
  }

}
