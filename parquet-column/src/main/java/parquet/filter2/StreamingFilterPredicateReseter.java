package parquet.filter2;

import parquet.filter2.StreamingFilterPredicate.And;
import parquet.filter2.StreamingFilterPredicate.Atom;
import parquet.filter2.StreamingFilterPredicate.Or;
import parquet.filter2.StreamingFilterPredicate.Visitor;

public class StreamingFilterPredicateReseter implements Visitor {
  private static final StreamingFilterPredicateReseter INSTANCE = new StreamingFilterPredicateReseter();

  public static void reset(StreamingFilterPredicate pred) {
    pred.accept(INSTANCE);
  }

  @Override
  public boolean visit(Atom p) {
    p.reset();
    return false;
  }

  @Override
  public boolean visit(And and) {
    and.getLeft().accept(this);
    and.getRight().accept(this);
    return false;
  }

  @Override
  public boolean visit(Or or) {
    or.getLeft().accept(this);
    or.getRight().accept(this);
    return false;
  }
}
