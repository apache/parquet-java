package parquet.filter2.recordlevel;

import parquet.filter2.recordlevel.StreamingFilterPredicate.And;
import parquet.filter2.recordlevel.StreamingFilterPredicate.Atom;
import parquet.filter2.recordlevel.StreamingFilterPredicate.Or;
import parquet.filter2.recordlevel.StreamingFilterPredicate.Visitor;

public class StreamingFilterPredicateEvaluator implements Visitor {
  private static final StreamingFilterPredicateEvaluator INSTANCE = new StreamingFilterPredicateEvaluator();

  public static boolean evaluate(StreamingFilterPredicate pred) {
    return pred.accept(INSTANCE);
  }

  @Override
  public boolean visit(Atom p) {
    if (!p.isKnown()) {
      p.updateNull();
    }
    return p.getResult();
  }

  @Override
  public boolean visit(And and) {
    return and.getLeft().accept(this) && and.getRight().accept(this);
  }

  @Override
  public boolean visit(Or or) {
    return or.getLeft().accept(this) || or.getRight().accept(this);
  }
}
