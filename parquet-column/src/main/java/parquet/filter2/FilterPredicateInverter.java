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

/**
 * Converts a FilterPredicate to its logical inverse.
 * The returned predicate should be equivalent to not(p), but without
 * the use of a not() operator.
 */
public class FilterPredicateInverter implements Visitor<FilterPredicate> {

  public static FilterPredicate invert(FilterPredicate p) {
    return p.accept(new FilterPredicateInverter());
  }

  @Override
  public <T> FilterPredicate visit(Eq<T> eq) {
    return new NotEq<T>(eq.getColumn(), eq.getValue());
  }

  @Override
  public <T> FilterPredicate visit(NotEq<T> notEq) {
    return new Eq<T>(notEq.getColumn(), notEq.getValue());
  }

  @Override
  public <T> FilterPredicate visit(Lt<T> lt) {
    return new GtEq<T>(lt.getColumn(), lt.getValue());
  }

  @Override
  public <T> FilterPredicate visit(LtEq<T> ltEq) {
    return new Gt<T>(ltEq.getColumn(), ltEq.getValue());
  }

  @Override
  public <T> FilterPredicate visit(Gt<T> gt) {
    return new LtEq<T>(gt.getColumn(), gt.getValue());
  }

  @Override
  public <T> FilterPredicate visit(GtEq<T> gtEq) {
    return new Lt<T>(gtEq.getColumn(), gtEq.getValue());
  }

  @Override
  public FilterPredicate visit(And and) {
    return new Or(and.getLeft().accept(this), and.getRight().accept(this));
  }

  @Override
  public FilterPredicate visit(Or or) {
    return new And(or.getLeft().accept(this), or.getRight().accept(this));
  }

  @Override
  public FilterPredicate visit(Not not) {
    return not.getPredicate();
  }

  @Override
  public <T, U> FilterPredicate visit(UserDefined<T, U> udp) {
    return new LogicalNotUserDefined<T, U>(udp);
  }

  @Override
  public <T, U> FilterPredicate visit(LogicalNotUserDefined<T, U> udp) {
    return udp.getUserDefined();
  }
}
