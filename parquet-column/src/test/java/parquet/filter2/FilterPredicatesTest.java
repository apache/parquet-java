package parquet.filter2;

import org.junit.Test;

import parquet.filter2.FilterPredicates.And;
import parquet.filter2.FilterPredicates.Column;
import parquet.filter2.FilterPredicates.Eq;
import parquet.filter2.FilterPredicates.Gt;
import parquet.filter2.FilterPredicates.Not;
import parquet.filter2.FilterPredicates.Or;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.filter2.Filter.and;
import static parquet.filter2.Filter.column;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.gt;
import static parquet.filter2.Filter.gtEq;
import static parquet.filter2.Filter.lt;
import static parquet.filter2.Filter.ltEq;
import static parquet.filter2.Filter.not;
import static parquet.filter2.Filter.notEq;
import static parquet.filter2.Filter.or;

public class FilterPredicatesTest {

  private static final Column<Integer> intColumn = column("a.b.c");
  private static final Column<Double> doubleColumn = column("x.y.z");

  private static final FilterPredicate predicate =
      and(not(or(eq(intColumn, 7), eq(intColumn, 17))), gt(doubleColumn, 100.0));

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
    FilterPredicate rightEq = ((Or) or).getRight();
    assertTrue(leftEq instanceof Eq);
    assertTrue(rightEq instanceof Eq);
    assertEquals(7, ((Eq) leftEq).getValue());
    assertEquals(17, ((Eq) rightEq).getValue());
    assertEquals("a.b.c", ((Eq) leftEq).getColumn().getColumnPath());
    assertEquals("a.b.c", ((Eq) rightEq).getColumn().getColumnPath());

    assertTrue(gt instanceof Gt);
    assertEquals(100.0, ((Gt) gt).getValue());
    assertEquals("x.y.z", ((Gt) gt).getColumn().getColumnPath());
  }

  @Test
  public void testSugarPredicates() {
    assertEquals(not(eq(intColumn, 10)), notEq(intColumn, 10));
    assertEquals(or(lt(intColumn, 10), eq(intColumn, 10)), ltEq(intColumn, 10));
    assertEquals(or(gt(intColumn, 10), eq(intColumn, 10)), gtEq(intColumn, 10));
  }

  @Test
  public void testCollapseDoubleNegation() {
    assertEquals(eq(intColumn, 10), not(not(eq(intColumn, 10))));
    assertEquals(eq(intColumn, 10), not(not(not(not(eq(intColumn, 10))))));
    assertEquals(not(eq(intColumn, 10)), not(not(not(eq(intColumn, 10)))));
  }

  @Test
  public void testToString() {
    assertEquals("and(not(or(eq(a.b.c, 7), eq(a.b.c, 17))), gt(x.y.z, 100.0))",
        predicate.toString());
  }
}
