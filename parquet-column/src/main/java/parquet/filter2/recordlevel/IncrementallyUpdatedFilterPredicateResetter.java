package parquet.filter2.recordlevel;

import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Visitor;

/**
 * Resets all the {@link ValueInspector}s in a {@link IncrementallyUpdatedFilterPredicate}.
 */
public final class IncrementallyUpdatedFilterPredicateResetter implements Visitor {
  private static final IncrementallyUpdatedFilterPredicateResetter INSTANCE = new IncrementallyUpdatedFilterPredicateResetter();

  public static void reset(IncrementallyUpdatedFilterPredicate pred) {
    pred.accept(INSTANCE);
  }

  private IncrementallyUpdatedFilterPredicateResetter() { }

  @Override
  public boolean visit(ValueInspector p) {
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
