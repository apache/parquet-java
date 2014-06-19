package parquet.filter2;

import org.junit.Test;

import parquet.filter2.FilterPredicates.And;
import parquet.filter2.FilterPredicates.Column;
import parquet.filter2.FilterPredicates.Eq;
import parquet.filter2.FilterPredicates.Gt;
import parquet.filter2.FilterPredicates.Not;
import parquet.filter2.FilterPredicates.Or;
import parquet.filter2.FilterPredicates.UserDefined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.filter2.Filter.and;
import static parquet.filter2.Filter.column;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.gt;
import static parquet.filter2.Filter.intPredicate;
import static parquet.filter2.Filter.not;
import static parquet.filter2.Filter.notEq;
import static parquet.filter2.Filter.or;
import static parquet.filter2.FilterPredicates.NotEq;

public class FilterTest {

  private static final Column<Integer> intColumn = column("a.b.c");
  private static final Column<Double> doubleColumn = column("x.y.z");

  private static final FilterPredicate predicate =
      and(not(or(eq(intColumn, 7), notEq(intColumn, 17))), gt(doubleColumn, 100.0));

  @Test
  public void testFilterPredicateCreation() {
    FilterPredicate outerAnd = predicate;

    assertTrue(outerAnd instanceof And);

    FilterPredicate not = ((And) outerAnd).getLeft();
    FilterPredicate gt = ((And) outerAnd).getRight();
    assertTrue(not instanceof Not);

    FilterPredicate or = ((Not) not).getPredicate();
    assertTrue(or instanceof Or);

    FilterPredicate leftEq = ((Or) or).getLeft();
    FilterPredicate rightNotEq = ((Or) or).getRight();
    assertTrue(leftEq instanceof Eq);
    assertTrue(rightNotEq instanceof NotEq);
    assertEquals(7, ((Eq) leftEq).getValue());
    assertEquals(17, ((NotEq) rightNotEq).getValue());
    assertEquals("a.b.c", ((Eq) leftEq).getColumn().getColumnPath());
    assertEquals("a.b.c", ((NotEq) rightNotEq).getColumn().getColumnPath());

    assertTrue(gt instanceof Gt);
    assertEquals(100.0, ((Gt) gt).getValue());
    assertEquals("x.y.z", ((Gt) gt).getColumn().getColumnPath());

  }

  @Test
  public void testToString() {
    assertEquals("and(not(or(eq(a.b.c, 7), noteq(a.b.c, 17))), gt(x.y.z, 100.0))",
        predicate.toString());
  }

  public static class DummyUdp extends UserDefinedPredicates.IntUserDefinedPredicate {
    @Override
    public boolean filterByValue(int value) {
      return false;
    }
  }

  @Test
  public void testNamedUdp() {
    FilterPredicate predicate = or(eq(doubleColumn, 12.0), intPredicate(intColumn, DummyUdp.class));
    assertTrue(predicate instanceof Or);
    FilterPredicate ud = ((Or) predicate).getRight();
    assertTrue(ud instanceof UserDefined);
    assertEquals(DummyUdp.class, ((UserDefined) ud).getUserDefinedPredicateClass());
    assertTrue(((UserDefined) ud).getUserDefinedPredicate() instanceof DummyUdp);
  }
}
