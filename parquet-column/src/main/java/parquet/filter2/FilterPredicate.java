package parquet.filter2;

import parquet.filter2.FilterPredicates.*;

/**
 * FilterPredicates are implemented in terms of the visitor pattern.
 */
public interface FilterPredicate {
  public static interface Visitor {
    boolean visit(Eq<?> eq);
    boolean visit(Lt<?> lt);
    boolean visit(Gt<?> gt);
    boolean visit(And and);
    boolean visit(Or or);
    boolean visit(Not not);
  }

  boolean accept(Visitor visitor);
}
