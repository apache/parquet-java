package parquet.filter2;

import org.junit.Test;

import parquet.filter2.FilterPredicateOperators.Column;
import parquet.filter2.FilterPredicateOperators.DoubleColumn;
import parquet.filter2.FilterPredicateOperators.IntColumn;
import parquet.filter2.FilterPredicateOperators.LogicalNotUserDefined;
import parquet.filter2.FilterPredicateOperators.UserDefined;

import static org.junit.Assert.assertEquals;
import static parquet.filter2.Filter.and;
import static parquet.filter2.Filter.doubleColumn;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.gt;
import static parquet.filter2.Filter.gtEq;
import static parquet.filter2.Filter.intColumn;
import static parquet.filter2.Filter.lt;
import static parquet.filter2.Filter.ltEq;
import static parquet.filter2.Filter.not;
import static parquet.filter2.Filter.notEq;
import static parquet.filter2.Filter.or;
import static parquet.filter2.Filter.userDefined;
import static parquet.filter2.FilterPredicateInverter.invert;

public class TestFilterPredicateInverter {
  private static final IntColumn intColumn = intColumn("a.b.c");
  private static final DoubleColumn doubleColumn = doubleColumn("a.b.c");

  private  static  final UserDefined<Integer, DummyUdp> ud = userDefined(intColumn, DummyUdp.class);

  private static final FilterPredicate complex =
      and(
          or(ltEq(doubleColumn, 12.0),
              and(
                  not(or(eq(intColumn, 7), notEq(intColumn, 17))),
                  userDefined(intColumn, DummyUdp.class))),
          or(gt(doubleColumn, 100.0), notEq(intColumn, 77)));

  private static final FilterPredicate complexInverse =
      or(
          and(gt(doubleColumn, 12.0),
              or(
                  or(eq(intColumn, 7), notEq(intColumn, 17)),
                  new LogicalNotUserDefined<Integer, DummyUdp>(userDefined(intColumn, DummyUdp.class)))),
          and(ltEq(doubleColumn, 100.0), eq(intColumn, 77)));

  @Test
  public void testBaseCases() {
    assertEquals(notEq(intColumn, 17), invert(eq(intColumn, 17)));
    assertEquals(eq(intColumn, 17), invert(notEq(intColumn, 17)));
    assertEquals(gtEq(intColumn, 17), invert(lt(intColumn, 17)));
    assertEquals(gt(intColumn, 17), invert(ltEq(intColumn, 17)));
    assertEquals(ltEq(intColumn, 17), invert(gt(intColumn, 17)));
    assertEquals(lt(intColumn, 17), invert(gtEq(intColumn, 17)));

    FilterPredicate andPos = and(eq(intColumn, 17), eq(doubleColumn, 12.0));
    FilterPredicate andInv = or(notEq(intColumn, 17), notEq(doubleColumn, 12.0));
    assertEquals(andInv, invert(andPos));

    FilterPredicate orPos = or(eq(intColumn, 17), eq(doubleColumn, 12.0));
    FilterPredicate orInv = and(notEq(intColumn, 17), notEq(doubleColumn, 12.0));
    assertEquals(orPos, invert(orInv));

    assertEquals(eq(intColumn, 17), invert(not(eq(intColumn, 17))));

    UserDefined<Integer, DummyUdp> ud = userDefined(intColumn, DummyUdp.class);
    assertEquals(new LogicalNotUserDefined<Integer, DummyUdp>(ud), invert(ud));
    assertEquals(ud, invert(not(ud)));
    assertEquals(ud, invert(new LogicalNotUserDefined<Integer, DummyUdp>(ud)));
  }

  @Test
  public void testComplex() {
    assertEquals(complexInverse, invert(complex));
  }
}
