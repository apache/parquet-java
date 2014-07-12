package parquet.filter2.predicate;

import parquet.filter2.predicate.FilterPredicate.Visitor;
import parquet.filter2.predicate.Operators.And;
import parquet.filter2.predicate.Operators.Eq;
import parquet.filter2.predicate.Operators.Gt;
import parquet.filter2.predicate.Operators.GtEq;
import parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import parquet.filter2.predicate.Operators.Lt;
import parquet.filter2.predicate.Operators.LtEq;
import parquet.filter2.predicate.Operators.Not;
import parquet.filter2.predicate.Operators.NotEq;
import parquet.filter2.predicate.Operators.Or;
import parquet.filter2.predicate.Operators.UserDefined;

/**
 * Converts a {@link FilterPredicate} to its logical inverse.
 * The returned predicate should be equivalent to not(p), but without
 * the use of a not() operator.
 *
 * See also {@link LogicalInverseRewriter}, which can remove the use
 * of all not() operators without inverting the overall predicate.
 *
 * This class can be reused, it is stateless and thread safe.
 */
public class LogicalInverter implements Visitor<FilterPredicate> {

  public static FilterPredicate invert(FilterPredicate p) {
    return p.accept(new LogicalInverter());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Eq<T> eq) {
    return new NotEq<T>(eq.getColumn(), eq.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(NotEq<T> notEq) {
    return new Eq<T>(notEq.getColumn(), notEq.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Lt<T> lt) {
    return new GtEq<T>(lt.getColumn(), lt.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(LtEq<T> ltEq) {
    return new Gt<T>(ltEq.getColumn(), ltEq.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(Gt<T> gt) {
    return new LtEq<T>(gt.getColumn(), gt.getValue());
  }

  @Override
  public <T extends Comparable<T>> FilterPredicate visit(GtEq<T> gtEq) {
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
  public <T extends Comparable<T>,  U extends UserDefinedPredicate<T>> FilterPredicate visit(UserDefined<T, U> udp) {
    return new LogicalNotUserDefined<T, U>(udp);
  }

  @Override
  public <T extends Comparable<T>,  U extends UserDefinedPredicate<T>> FilterPredicate visit(LogicalNotUserDefined<T, U> udp) {
    return udp.getUserDefined();
  }
}
