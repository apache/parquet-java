package parquet.filter2.recordlevel;

import parquet.Preconditions;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Visitor;

/**
 * Determines whether an {@link IncrementallyUpdatedFilterPredicate} is satisfied or not.
 * This implementation makes the assumption that all {@link ValueInspector}s in an unknown state
 * represent columns with a null value, and updates them accordingly.
 *
 * TODO(alexlevenson): We could also build an evaluator that detects if enough values are known to determine the outcome
 *                     of the predicate and quit the record assembly early.
 */
public class IncrementallyUpdatedFilterPredicateEvaluator implements Visitor {
  private static final IncrementallyUpdatedFilterPredicateEvaluator INSTANCE = new IncrementallyUpdatedFilterPredicateEvaluator();

  public static boolean evaluate(IncrementallyUpdatedFilterPredicate pred) {
    Preconditions.checkNotNull(pred, "pred");
    return pred.accept(INSTANCE);
  }

  private IncrementallyUpdatedFilterPredicateEvaluator() {}

  @Override
  public boolean visit(ValueInspector p) {
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
