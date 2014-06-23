package parquet.filter2;

import org.junit.Test;

import parquet.filter2.FilterPredicates.Column;
import parquet.filter2.FilterPredicates.IntUserDefined;
import parquet.filter2.FilterPredicates.LogicalNotUserDefined;

import static org.junit.Assert.assertEquals;
import static parquet.filter2.CollapseLogicalNots.collapse;
import static parquet.filter2.Filter.and;
import static parquet.filter2.Filter.doubleColumn;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.gt;
import static parquet.filter2.Filter.gtEq;
import static parquet.filter2.Filter.intColumn;
import static parquet.filter2.Filter.intPredicate;
import static parquet.filter2.Filter.lt;
import static parquet.filter2.Filter.ltEq;
import static parquet.filter2.Filter.not;
import static parquet.filter2.Filter.notEq;
import static parquet.filter2.Filter.or;

public class TestCollapseLogicalNots {
  private static final Column<Integer> intColumn = intColumn("a.b.c");
  private static final Column<Double> doubleColumn = doubleColumn("a.b.c");

  private static final FilterPredicate complex =
      and(
          not(
              or(ltEq(doubleColumn, 12.0),
                  and(
                      not(or(eq(intColumn, 7), notEq(intColumn, 17))),
                      intPredicate(intColumn, DummyUdp.class)))),
          or(gt(doubleColumn, 100.0), not(gtEq(intColumn, 77))));

  private static final FilterPredicate complexCollapsed =
      and(
          and(gt(doubleColumn, 12.0),
              or(
                  or(eq(intColumn, 7), notEq(intColumn, 17)),
                  new LogicalNotUserDefined<Integer, DummyUdp>(intPredicate(intColumn, DummyUdp.class)))),
          or(gt(doubleColumn, 100.0), lt(intColumn, 77)));

  private static void assertNoOp(FilterPredicate p) {
    assertEquals(p, collapse(p));
  }

  @Test
  public void testBaseCases() {
    IntUserDefined<DummyUdp> ud = intPredicate(intColumn, DummyUdp.class);

    assertNoOp(eq(intColumn, 17));
    assertNoOp(notEq(intColumn, 17));
    assertNoOp(lt(intColumn, 17));
    assertNoOp(ltEq(intColumn, 17));
    assertNoOp(gt(intColumn, 17));
    assertNoOp(gtEq(intColumn, 17));
    assertNoOp(and(eq(intColumn, 17), eq(doubleColumn, 12.0)));
    assertNoOp(or(eq(intColumn, 17), eq(doubleColumn, 12.0)));
    assertNoOp(ud);

    assertEquals(notEq(intColumn, 17), collapse(not(eq(intColumn, 17))));
    assertEquals(eq(intColumn, 17), collapse(not(notEq(intColumn, 17))));
    assertEquals(gtEq(intColumn, 17), collapse(not(lt(intColumn, 17))));
    assertEquals(gt(intColumn, 17), collapse(not(ltEq(intColumn, 17))));
    assertEquals(ltEq(intColumn, 17), collapse(not(gt(intColumn, 17))));
    assertEquals(lt(intColumn, 17), collapse(not(gtEq(intColumn, 17))));
    assertEquals(new LogicalNotUserDefined<Integer, DummyUdp>(ud), collapse(not(ud)));

    FilterPredicate notedAnd = not(and(eq(intColumn, 17), eq(doubleColumn, 12.0)));
    FilterPredicate distributedAnd = or(notEq(intColumn, 17), notEq(doubleColumn, 12.0));
    assertEquals(distributedAnd, collapse(notedAnd));

    FilterPredicate andWithNots = and(not(gtEq(intColumn, 17)), lt(intColumn, 7));
    FilterPredicate andWithoutNots = and(lt(intColumn, 17), lt(intColumn, 7));
    assertEquals(andWithoutNots, collapse(andWithNots));
  }

  @Test
  public void testComplex() {
    assertEquals(complexCollapsed, collapse(complex));
  }
}
