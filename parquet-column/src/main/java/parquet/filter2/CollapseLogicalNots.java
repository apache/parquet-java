package parquet.filter2;

import parquet.filter2.FilterPredicate.Visitor;
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
 * Recursively removes all use of the not() operator in a predicate
 * by replacing all instances of not(x) with the inverse(x),
 * eg: not(and(eq(), not(eq(y))) -> or(notEq(), eq(y))
 *
 * The returned predicate should have the same meaning as the original, but
 * without the use of the not() operator.
 */
// TODO: we could actually just remove the not operator entirely instead
//       we could instead apply the logic in FilterPredicateInverter when the not() method is called
//       the only downside is that that would make the toString of a predicate sort of surprising,
//       eg not(or(eq(foo, 10), eq(foo, 11))).toString() would be: "and(notEq(foo, 10), notEq(foo, 11))"
public class CollapseLogicalNots implements Visitor<FilterPredicate> {

  public static FilterPredicate collapse(FilterPredicate pred) {
    return pred.accept(new CollapseLogicalNots());
  }

  @Override
  public <T> FilterPredicate visit(Eq<T> eq) {
    return eq;
  }

  @Override
  public <T> FilterPredicate visit(NotEq<T> notEq) {
    return notEq;
  }

  @Override
  public <T> FilterPredicate visit(Lt<T> lt) {
    return lt;
  }

  @Override
  public <T> FilterPredicate visit(LtEq<T> ltEq) {
    return ltEq;
  }

  @Override
  public <T> FilterPredicate visit(Gt<T> gt) {
    return gt;
  }

  @Override
  public <T> FilterPredicate visit(GtEq<T> gtEq) {
    return gtEq;
  }

  @Override
  public FilterPredicate visit(And and) {
    return new And(and.getLeft().accept(this), and.getRight().accept(this));
  }

  @Override
  public FilterPredicate visit(Or or) {
    return new Or(or.getLeft().accept(this), or.getRight().accept(this));
  }

  @Override
  public FilterPredicate visit(Not not) {
    return FilterPredicateInverter.invert(not.getPredicate().accept(this));
  }

  @Override
  public <T, U extends UserDefinedPredicate<T>> FilterPredicate visit(UserDefined<T, U> udp) {
    return udp;
  }

  @Override
  public <T, U extends UserDefinedPredicate<T>> FilterPredicate visit(LogicalNotUserDefined<T, U> udp) {
    return udp;
  }
}
